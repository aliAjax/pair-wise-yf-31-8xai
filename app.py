#!/usr/bin/env python3
"""Airline disruption recovery engine using standard-library SQLite and HTTP."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, time, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

PORT = 8202
ROLES = {"viewer", "scheduler", "ops_manager", "auditor"}
DEFAULT_SLOT_CAPACITY = 8
SLOT_BLOCK_MINUTES = 60
RUNWAYS = ("departure", "arrival")


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message)
        self.status, self.code, self.message, self.details = status, code, message, details


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None = None) -> str:
    return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_time(value: str | None) -> datetime:
    if not value:
        raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def parse_clock(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ApiError(400, "invalid_clock", f"时刻格式应为 HH:MM: {value}") from exc


def overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start < b_end and b_start < a_end


class Repository:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self._local = threading.local()
        init = self.conn
        init.execute("PRAGMA journal_mode=WAL")
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None, timeout=15.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        connection = getattr(self._local, "conn", None)
        if connection is None:
            connection = self._connect()
            self._local.conn = connection
        return connection

    @contextmanager
    def tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def _init(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS airports(code TEXT PRIMARY KEY, country TEXT NOT NULL, curfew_start TEXT NOT NULL, curfew_end TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS aircraft(id TEXT PRIMARY KEY, model TEXT NOT NULL, maintenance_due TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
            CREATE TABLE IF NOT EXISTS crew(id TEXT PRIMARY KEY, name TEXT NOT NULL, base TEXT NOT NULL, duty_start TEXT NOT NULL, max_duty_minutes INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'active');
            CREATE TABLE IF NOT EXISTS permits(id INTEGER PRIMARY KEY AUTOINCREMENT, origin TEXT NOT NULL, destination TEXT NOT NULL, valid_from TEXT NOT NULL, valid_to TEXT NOT NULL, curfew_exempt INTEGER NOT NULL DEFAULT 0, UNIQUE(origin,destination,valid_from,valid_to));
            CREATE TABLE IF NOT EXISTS flights(
                id INTEGER PRIMARY KEY AUTOINCREMENT, flight_no TEXT NOT NULL UNIQUE, origin TEXT NOT NULL, destination TEXT NOT NULL,
                std TEXT NOT NULL, sta TEXT NOT NULL, aircraft_id TEXT NOT NULL REFERENCES aircraft(id), crew_id TEXT NOT NULL REFERENCES crew(id),
                passenger_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'scheduled', delay_minutes INTEGER NOT NULL DEFAULT 0,
                revision INTEGER NOT NULL DEFAULT 1, cancel_reason TEXT, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS disruptions(id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, resource_id TEXT NOT NULL, starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS recovery_plans(
                id INTEGER PRIMARY KEY AUTOINCREMENT, disruption_id INTEGER NOT NULL REFERENCES disruptions(id), name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft', revision INTEGER NOT NULL DEFAULT 1, score_json TEXT, metrics_json TEXT,
                created_by TEXT NOT NULL, created_at TEXT NOT NULL, locked_at TEXT, locked_by TEXT
            );
            CREATE TABLE IF NOT EXISTS assignments(
                id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL REFERENCES recovery_plans(id) ON DELETE CASCADE,
                flight_id INTEGER NOT NULL REFERENCES flights(id), aircraft_id TEXT NOT NULL REFERENCES aircraft(id), crew_id TEXT NOT NULL REFERENCES crew(id),
                new_std TEXT NOT NULL, new_sta TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'planned', delay_minutes INTEGER NOT NULL DEFAULT 0,
                missed_connections INTEGER NOT NULL DEFAULT 0, UNIQUE(plan_id,flight_id)
            );
            CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS ferry_requests(
                id INTEGER PRIMARY KEY AUTOINCREMENT, ferry_no TEXT NOT NULL UNIQUE,
                aircraft_id TEXT NOT NULL REFERENCES aircraft(id), origin TEXT NOT NULL, destination TEXT NOT NULL,
                earliest_start TEXT NOT NULL, turnaround_minutes INTEGER NOT NULL DEFAULT 45,
                status TEXT NOT NULL DEFAULT 'draft', plan_id INTEGER REFERENCES recovery_plans(id),
                created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ferry_segments(
                id INTEGER PRIMARY KEY AUTOINCREMENT, ferry_id INTEGER NOT NULL REFERENCES ferry_requests(id) ON DELETE CASCADE,
                seq INTEGER NOT NULL, origin TEXT NOT NULL, destination TEXT NOT NULL,
                std TEXT NOT NULL, sta TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued', queued_at TEXT NOT NULL,
                UNIQUE(ferry_id, seq)
            );
            CREATE TABLE IF NOT EXISTS slot_occupancy(
                id INTEGER PRIMARY KEY AUTOINCREMENT, airport TEXT NOT NULL, slot_start TEXT NOT NULL, runway TEXT NOT NULL DEFAULT 'departure',
                ferry_segment_id INTEGER NOT NULL REFERENCES ferry_segments(id) ON DELETE CASCADE, created_at TEXT NOT NULL,
                UNIQUE(ferry_segment_id, runway)
            );
            CREATE TABLE IF NOT EXISTS slot_capacity(
                airport TEXT NOT NULL, slot_start TEXT NOT NULL, runway TEXT NOT NULL, capacity INTEGER NOT NULL,
                PRIMARY KEY(airport, slot_start, runway)
            );
            """
        )

    @staticmethod
    def audit(conn: sqlite3.Connection, plan_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(plan_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?)",
                     (plan_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class AirlineRecoveryService:
    def __init__(self, db_path: str | Path):
        self.repo = Repository(db_path)

    @staticmethod
    def identity(headers: Any) -> tuple[str, str]:
        actor, role = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip()
        if not actor or role not in ROLES:
            raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        return actor, role

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row else None

    def seed_airport(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager":
            raise ApiError(403, "seed_forbidden", "只有运行经理可以维护机场数据")
        code = str(body.get("code", "")).upper().strip()
        country = str(body.get("country", "")).upper().strip()
        if not code or not country:
            raise ApiError(400, "missing_fields", "code 和 country 必填")
        start = str(body.get("curfew_start", "23:00")); end = str(body.get("curfew_end", "06:00"))
        parse_clock(start); parse_clock(end)
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO airports(code,country,curfew_start,curfew_end) VALUES(?,?,?,?)", (code, country, start, end))
            return dict(conn.execute("SELECT * FROM airports WHERE code=?", (code,)).fetchone())

    def seed_aircraft(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "seed_forbidden", "只有运行经理可以维护飞机")
        ident, model = str(body.get("id", "")).strip(), str(body.get("model", "")).strip()
        if not ident or not model: raise ApiError(400, "missing_fields", "id 和 model 必填")
        due = iso(parse_time(body.get("maintenance_due")))
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO aircraft(id,model,maintenance_due,status) VALUES(?,?,?,?)", (ident, model, due, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM aircraft WHERE id=?", (ident,)).fetchone())

    def seed_crew(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "seed_forbidden", "只有运行经理可以维护机组")
        ident, name, base = str(body.get("id", "")).strip(), str(body.get("name", "")).strip(), str(body.get("base", "")).upper().strip()
        duty = parse_time(body.get("duty_start")); maximum = body.get("max_duty_minutes")
        if not ident or not name or not base or not isinstance(maximum, int) or maximum <= 0:
            raise ApiError(400, "invalid_crew", "id、name、base 和正整数 max_duty_minutes 必填")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO crew(id,name,base,duty_start,max_duty_minutes,status) VALUES(?,?,?,?,?,?)", (ident, name, base, iso(duty), maximum, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM crew WHERE id=?", (ident,)).fetchone())

    def create_permit(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "permit_forbidden", "只有运行经理可以维护航线许可")
        origin, destination = str(body.get("origin", "")).upper(), str(body.get("destination", "")).upper()
        if not origin or not destination: raise ApiError(400, "missing_fields", "origin 和 destination 必填")
        valid_from, valid_to = parse_time(body.get("valid_from")), parse_time(body.get("valid_to"))
        if valid_to <= valid_from: raise ApiError(400, "invalid_permit", "许可结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            try:
                cur = conn.execute("INSERT INTO permits(origin,destination,valid_from,valid_to,curfew_exempt) VALUES(?,?,?,?,?)",
                                   (origin, destination, iso(valid_from), iso(valid_to), int(bool(body.get("curfew_exempt")))))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "permit_exists", "相同航线与有效期的许可已存在") from exc
            return dict(conn.execute("SELECT * FROM permits WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_flight(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "flight_forbidden", "当前角色不能创建航班")
        required = ("flight_no", "origin", "destination", "std", "sta", "aircraft_id", "crew_id")
        if any(not body.get(k) for k in required): raise ApiError(400, "missing_fields", f"缺少字段: {', '.join(k for k in required if not body.get(k))}")
        std, sta = parse_time(body["std"]), parse_time(body["sta"])
        if sta <= std: raise ApiError(400, "invalid_times", "到达时间必须晚于起飞时间")
        passengers = body.get("passenger_count", 0)
        if not isinstance(passengers, int) or passengers < 0: raise ApiError(400, "invalid_passengers", "passenger_count 必须是非负整数")
        with self.repo.tx() as conn:
            for table, ident in (("aircraft", body["aircraft_id"]), ("crew", body["crew_id"])):
                row = conn.execute(f"SELECT status FROM {table} WHERE id=?", (ident,)).fetchone()
                if not row or row["status"] != "active": raise ApiError(409, "resource_unavailable", f"{table} {ident} 不可用")
            try:
                cur = conn.execute("""INSERT INTO flights(flight_no,origin,destination,std,sta,aircraft_id,crew_id,passenger_count,updated_at)
                                      VALUES(?,?,?,?,?,?,?,?,?)""",
                                   (body["flight_no"].upper(), body["origin"].upper(), body["destination"].upper(), iso(std), iso(sta),
                                    body["aircraft_id"], body["crew_id"], passengers, iso()))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "flight_exists", "航班号已存在") from exc
            return dict(conn.execute("SELECT * FROM flights WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_disruption(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "disruption_forbidden", "当前角色不能登记中断")
        kind, resource = str(body.get("kind", "")).strip(), str(body.get("resource_id", "")).strip()
        if kind not in {"airport_closure", "aircraft_fault", "crew_timeout"} or not resource:
            raise ApiError(400, "invalid_disruption", "kind 或 resource_id 无效")
        start, end = parse_time(body.get("starts_at")), parse_time(body.get("ends_at"))
        if end <= start: raise ApiError(400, "invalid_times", "中断结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            cur = conn.execute("INSERT INTO disruptions(kind,resource_id,starts_at,ends_at,created_at) VALUES(?,?,?,?,?)",
                               (kind, resource.upper() if kind == "airport_closure" else resource, iso(start), iso(end), iso()))
            return dict(conn.execute("SELECT * FROM disruptions WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_plan(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "plan_forbidden", "当前角色不能创建恢复方案")
        disruption_id, name = body.get("disruption_id"), str(body.get("name", "")).strip()
        assignments = body.get("assignments", [])
        if not isinstance(disruption_id, int) or not name or not isinstance(assignments, list):
            raise ApiError(400, "invalid_plan", "disruption_id、name 和 assignments 必填")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM disruptions WHERE id=?", (disruption_id,)).fetchone():
                raise ApiError(404, "disruption_not_found", "中断事件不存在")
            cur = conn.execute("INSERT INTO recovery_plans(disruption_id,name,created_by,created_at) VALUES(?,?,?,?)", (disruption_id, name, actor, iso()))
            plan_id = cur.lastrowid
            for item in assignments:
                self._insert_assignment(conn, plan_id, item, replace=False)
            Repository.audit(conn, plan_id, actor, role, "plan_created", {"disruption_id": disruption_id, "assignment_count": len(assignments)})
            return self.get_plan(plan_id)

    def _insert_assignment(self, conn: sqlite3.Connection, plan_id: int, item: dict[str, Any], replace: bool) -> None:
        required = ("flight_id", "aircraft_id", "crew_id", "new_std", "new_sta")
        if any(item.get(k) in (None, "") for k in required): raise ApiError(400, "invalid_assignment", f"飞行调整缺少字段: {', '.join(k for k in required if item.get(k) in (None, ''))}")
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能修改")
        std, sta = parse_time(item["new_std"]), parse_time(item["new_sta"])
        if sta <= std: raise ApiError(400, "invalid_times", "新到达时间必须晚于新起飞时间")
        flight = conn.execute("SELECT * FROM flights WHERE id=?", (item["flight_id"],)).fetchone()
        if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
        if flight["status"] == "canceled" and item.get("status", "planned") != "canceled":
            raise ApiError(409, "canceled_flight", "已取消航班不能安排执行")
        delay = int((std - parse_time(flight["std"])).total_seconds() // 60)
        missed = int(item.get("missed_connections", 0))
        if missed < 0: raise ApiError(400, "invalid_connections", "missed_connections 不能为负")
        try:
            if replace:
                conn.execute("""UPDATE assignments SET aircraft_id=?,crew_id=?,new_std=?,new_sta=?,status=?,delay_minutes=?,missed_connections=?
                                WHERE plan_id=? AND flight_id=?""",
                             (item["aircraft_id"], item["crew_id"], iso(std), iso(sta), item.get("status", "planned"), delay, missed, plan_id, item["flight_id"]))
                if conn.execute("SELECT changes()").fetchone()[0] == 0:
                    raise KeyError
            else:
                conn.execute("""INSERT INTO assignments(plan_id,flight_id,aircraft_id,crew_id,new_std,new_sta,status,delay_minutes,missed_connections)
                                VALUES(?,?,?,?,?,?,?,?,?)""",
                             (plan_id, item["flight_id"], item["aircraft_id"], item["crew_id"], iso(std), iso(sta), item.get("status", "planned"), delay, missed))
        except sqlite3.IntegrityError as exc:
            raise ApiError(409, "assignment_conflict", "方案中该航班已存在或资源无效") from exc
        except KeyError as exc:
            raise ApiError(404, "assignment_not_found", "待替换的航班调整不存在") from exc

    def add_assignment(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "assignment_forbidden", "当前角色不能修改方案")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能修改")
            if plan["revision"] != expected: raise ApiError(409, "revision_conflict", "方案已被其他人更新")
            self._insert_assignment(conn, plan_id, body, replace=True)
            conn.execute("UPDATE recovery_plans SET revision=revision+1 WHERE id=?", (plan_id,))
            Repository.audit(conn, plan_id, actor, role, "assignment_reassigned", {"flight_id": body.get("flight_id")})
            return self.get_plan(plan_id)

    def _validate_plan(self, conn: sqlite3.Connection, plan_id: int) -> list[dict[str, Any]]:
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        rows = [dict(r) for r in conn.execute("""SELECT a.*, f.flight_no, f.origin, f.destination, f.passenger_count, f.status flight_status
                                                  FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? ORDER BY a.new_std""", (plan_id,))]
        if not rows: raise ApiError(409, "empty_plan", "方案没有飞行调整")
        problems: list[dict[str, Any]] = []
        by_aircraft: dict[str, list[dict[str, Any]]] = {}
        by_crew: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            if row["status"] == "canceled": continue
            std, sta = parse_time(row["new_std"]), parse_time(row["new_sta"])
            aircraft = conn.execute("SELECT * FROM aircraft WHERE id=?", (row["aircraft_id"],)).fetchone()
            crew = conn.execute("SELECT * FROM crew WHERE id=?", (row["crew_id"],)).fetchone()
            origin = conn.execute("SELECT * FROM airports WHERE code=?", (row["origin"],)).fetchone()
            destination = conn.execute("SELECT * FROM airports WHERE code=?", (row["destination"],)).fetchone()
            if not aircraft or aircraft["status"] != "active": problems.append({"assignment_id": row["id"], "code": "aircraft_unavailable"})
            elif parse_time(aircraft["maintenance_due"]) < sta: problems.append({"assignment_id": row["id"], "code": "maintenance_due", "resource": aircraft["id"]})
            if not crew or crew["status"] != "active": problems.append({"assignment_id": row["id"], "code": "crew_unavailable"})
            if not origin or not destination: problems.append({"assignment_id": row["id"], "code": "airport_unknown"})
            if crew:
                duty_start, max_duty = parse_time(crew["duty_start"]), crew["max_duty_minutes"]
                if (sta - duty_start).total_seconds() / 60 > max_duty: problems.append({"assignment_id": row["id"], "code": "duty_limit", "resource": crew["id"]})
            if destination:
                curfew_start, curfew_end = parse_clock(destination["curfew_start"]), parse_clock(destination["curfew_end"])
                permit = conn.execute("""SELECT * FROM permits WHERE origin=? AND destination=? AND valid_from<=? AND valid_to>=?""",
                                      (row["origin"], row["destination"], row["new_sta"], row["new_sta"])).fetchone()
                arrival_clock = sta.timetz().replace(tzinfo=None)
                inside = arrival_clock >= curfew_start or arrival_clock < curfew_end if curfew_start > curfew_end else curfew_start <= arrival_clock < curfew_end
                if inside and not (permit and permit["curfew_exempt"]): problems.append({"assignment_id": row["id"], "code": "airport_curfew"})
                if row["origin"] != row["destination"] and not permit: problems.append({"assignment_id": row["id"], "code": "route_permit_missing"})
                elif row["origin"] != row["destination"] and not (parse_time(permit["valid_from"]) <= std <= parse_time(permit["valid_to"])):
                    problems.append({"assignment_id": row["id"], "code": "route_permit_window"})
            by_aircraft.setdefault(row["aircraft_id"], []).append(row)
            by_crew.setdefault(row["crew_id"], []).append(row)
        for bucket_name, buckets in (("aircraft", by_aircraft), ("crew", by_crew)):
            for resource, items in buckets.items():
                for i, left in enumerate(items):
                    for right in items[i + 1:]:
                        if overlaps(parse_time(left["new_std"]), parse_time(left["new_sta"]), parse_time(right["new_std"]), parse_time(right["new_sta"])):
                            problems.append({"code": f"{bucket_name}_overlap", "resource": resource, "assignments": [left["id"], right["id"]]})
        return problems

    def validate_plan(self, plan_id: int, actor: str, role: str) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager", "auditor"}: raise ApiError(403, "validate_forbidden", "当前角色不能校验方案")
        with self.repo.tx() as conn:
            problems = self._validate_plan(conn, plan_id)
            if not problems:
                metrics = self._metrics(conn, plan_id)
                conn.execute("UPDATE recovery_plans SET metrics_json=?,score_json=? WHERE id=?", (json.dumps(metrics, ensure_ascii=False), json.dumps(self._score(metrics)), plan_id))
            return {"valid": not problems, "problems": problems, "plan": self.get_plan(plan_id)}

    def _metrics(self, conn: sqlite3.Connection, plan_id: int) -> dict[str, Any]:
        rows = conn.execute("""SELECT a.*,f.passenger_count FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=?""", (plan_id,)).fetchall()
        canceled = sum(1 for row in rows if row["status"] == "canceled")
        return {"flight_count": len(rows), "canceled": canceled, "total_delay_minutes": sum(max(0, row["delay_minutes"]) for row in rows),
                "affected_passengers": sum(row["passenger_count"] for row in rows), "missed_connections": sum(row["missed_connections"] for row in rows)}

    @staticmethod
    def _score(metrics: dict[str, Any]) -> dict[str, int]:
        score = metrics["canceled"] * 100000 + metrics["missed_connections"] * 5000 + metrics["total_delay_minutes"] * 100 + metrics["affected_passengers"]
        return {"cost_score": score, "lower_is_better": 1}

    def lock_plan(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "lock_forbidden", "只有运行经理可以锁定恢复方案")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] == "locked": return self.get_plan(plan_id)
            if plan["revision"] != expected: raise ApiError(409, "revision_conflict", "方案版本已变化")
            problems = self._validate_plan(conn, plan_id)
            if problems: raise ApiError(409, "plan_invalid", "方案未通过约束校验", problems)
            conflicts = []
            for row in conn.execute("SELECT * FROM assignments WHERE plan_id=? AND status!='canceled'", (plan_id,)).fetchall():
                conflicting = conn.execute("""SELECT a.*,p.name plan_name FROM assignments a JOIN recovery_plans p ON p.id=a.plan_id
                    WHERE p.id!=? AND p.status='locked' AND a.status!='canceled' AND (a.aircraft_id=? OR a.crew_id=?)
                    AND a.new_std<? AND a.new_sta>?""",
                    (plan_id, row["aircraft_id"], row["crew_id"], row["new_sta"], row["new_std"])).fetchall()
                conflicts.extend({"assignment_id": row["id"], "conflict_plan_id": item["plan_id"], "conflict_plan": item["plan_name"], "resource": item["aircraft_id"] if item["aircraft_id"] == row["aircraft_id"] else item["crew_id"]} for item in conflicting)
            if conflicts: raise ApiError(409, "locked_resource_conflict", "与已锁定方案存在飞机或机组冲突", conflicts)
            metrics = self._metrics(conn, plan_id)
            conn.execute("""UPDATE recovery_plans SET status='locked',metrics_json=?,score_json=?,locked_at=?,locked_by=? WHERE id=?""",
                         (json.dumps(metrics, ensure_ascii=False), json.dumps(self._score(metrics)), iso(), actor, plan_id))
            for row in conn.execute("""SELECT a.*,f.flight_no FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? AND a.status!='canceled'""", (plan_id,)):
                conn.execute("UPDATE flights SET std=?,sta=?,aircraft_id=?,crew_id=?,delay_minutes=?,revision=revision+1,updated_at=? WHERE id=?",
                             (row["new_std"], row["new_sta"], row["aircraft_id"], row["crew_id"], max(0, row["delay_minutes"]), iso(), row["flight_id"]))
                conn.execute("UPDATE assignments SET status='active' WHERE id=?", (row["id"],))
            Repository.audit(conn, plan_id, actor, role, "plan_locked", {"metrics": metrics})
            return self.get_plan(plan_id)

    def cancel_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "cancel_forbidden", "当前角色不能取消航班")
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["status"] == "canceled": return {"flight": dict(flight), "idempotent": True}
            conn.execute("UPDATE flights SET status='canceled',cancel_reason=?,revision=revision+1,updated_at=? WHERE id=?", (reason, iso(), flight_id))
            Repository.audit(conn, None, actor, role, "flight_canceled", {"flight_id": flight_id, "reason": reason})
            return {"flight": dict(conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()), "idempotent": False}

    def recover_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "recover_forbidden", "当前角色不能恢复航班")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["revision"] != expected: raise ApiError(409, "revision_conflict", "航班版本已变化")
            if flight["status"] != "canceled": raise ApiError(409, "not_canceled", "只有取消航班可以恢复")
            std, sta = parse_time(body.get("new_std")), parse_time(body.get("new_sta"))
            if sta <= std: raise ApiError(400, "invalid_times", "到达时间必须晚于起飞时间")
            aircraft_id, crew_id = body.get("aircraft_id", flight["aircraft_id"]), body.get("crew_id", flight["crew_id"])
            conn.execute("""UPDATE flights SET status='scheduled',std=?,sta=?,aircraft_id=?,crew_id=?,cancel_reason=NULL,
                            delay_minutes=0,revision=revision+1,updated_at=? WHERE id=?""",
                         (iso(std), iso(sta), aircraft_id, crew_id, iso(), flight_id))
            Repository.audit(conn, None, actor, role, "flight_recovered", {"flight_id": flight_id})
            return {"flight": dict(conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone())}

    def get_plan(self, plan_id: int) -> dict[str, Any]:
        conn = self.repo.conn
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        assignments = [dict(r) for r in conn.execute("""SELECT a.*,f.flight_no,f.origin,f.destination,f.passenger_count FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? ORDER BY a.new_std""", (plan_id,))]
        result = dict(plan)
        result["metrics"] = json.loads(plan["metrics_json"]) if plan["metrics_json"] else self._metrics(conn, plan_id)
        result["score"] = json.loads(plan["score_json"]) if plan["score_json"] else None
        result["assignments"] = assignments
        result["ferries"] = [self._ferry_detail(conn, r["id"]) for r in conn.execute("SELECT id FROM ferry_requests WHERE plan_id=? ORDER BY id", (plan_id,))]
        return result

    def compare_plans(self, disruption_id: int) -> dict[str, Any]:
        plans = []
        for row in self.repo.conn.execute("SELECT id FROM recovery_plans WHERE disruption_id=? ORDER BY id", (disruption_id,)):
            plan = self.get_plan(row["id"])
            if not plan["score"]:
                problems = self._validate_plan(self.repo.conn, row["id"])
                plan["valid"] = not problems
            else:
                plan["valid"] = True
            plans.append(plan)
        plans.sort(key=lambda item: item["score"]["cost_score"] if item["score"] else 10**18)
        return {"disruption_id": disruption_id, "recommended_plan_id": plans[0]["id"] if plans else None, "plans": plans}

    def state(self) -> dict[str, Any]:
        conn = self.repo.conn
        flights = [dict(r) for r in conn.execute("SELECT * FROM flights ORDER BY std")]
        plans = [self.get_plan(r["id"]) for r in conn.execute("SELECT id FROM recovery_plans ORDER BY id DESC LIMIT 20")]
        ferries = [self._ferry_detail(conn, r["id"]) for r in conn.execute("SELECT id FROM ferry_requests ORDER BY id DESC LIMIT 20")]
        return {"flights": flights, "disruptions": [dict(r) for r in conn.execute("SELECT * FROM disruptions ORDER BY id DESC")],
                "plans": plans, "ferries": ferries, "server_time": iso()}

    # ------------------------------------------------------------------
    # 调机 (ferry flight) management
    # ------------------------------------------------------------------
    def _slot_window(self, value: datetime) -> datetime:
        return value.replace(minute=0, second=0, microsecond=0)

    def _slot_capacity(self, conn: sqlite3.Connection, airport: str, window: datetime, runway: str) -> int:
        row = conn.execute("SELECT capacity FROM slot_capacity WHERE airport=? AND slot_start=? AND runway=?",
                           (airport, iso(window), runway)).fetchone()
        return row["capacity"] if row else DEFAULT_SLOT_CAPACITY

    def _slot_used(self, conn: sqlite3.Connection, airport: str, window: datetime, runway: str) -> int:
        return conn.execute("SELECT COUNT(*) AS c FROM slot_occupancy WHERE airport=? AND slot_start=? AND runway=?",
                            (airport, iso(window), runway)).fetchone()["c"]

    def _segment_fits(self, conn: sqlite3.Connection, seg: dict[str, Any]) -> bool:
        std, sta = parse_time(seg["std"]), parse_time(seg["sta"])
        for airport, dt, runway in ((seg["origin"], std, "departure"), (seg["destination"], sta, "arrival")):
            window = self._slot_window(dt)
            if self._slot_used(conn, airport, window, runway) >= self._slot_capacity(conn, airport, window, runway):
                return False
        return True

    def _release_segment(self, conn: sqlite3.Connection, seg_id: int) -> None:
        conn.execute("DELETE FROM slot_occupancy WHERE ferry_segment_id=?", (seg_id,))

    def _claim_segment(self, conn: sqlite3.Connection, seg: dict[str, Any]) -> None:
        std, sta = parse_time(seg["std"]), parse_time(seg["sta"])
        for airport, dt, runway in ((seg["origin"], std, "departure"), (seg["destination"], sta, "arrival")):
            conn.execute("INSERT INTO slot_occupancy(airport,slot_start,runway,ferry_segment_id,created_at) VALUES(?,?,?,?,?)",
                         (airport, iso(self._slot_window(dt)), runway, seg["id"], iso()))

    def _process_queue(self, conn: sqlite3.Connection) -> None:
        """Assign slots to queued segments in FIFO order as capacity frees up."""
        rows = [dict(r) for r in conn.execute("SELECT * FROM ferry_segments WHERE status='queued' ORDER BY queued_at,id")]
        for seg in rows:
            if self._segment_fits(conn, seg):
                self._claim_segment(conn, seg)
                conn.execute("UPDATE ferry_segments SET status='assigned' WHERE id=?", (seg["id"],))

    def _compute_earliest(self, conn: sqlite3.Connection, aircraft_id: str, origin: str, turnaround: int) -> datetime:
        """Feasibility from the previous flight's landing location and time."""
        prev = conn.execute("""SELECT * FROM flights WHERE aircraft_id=? AND status!='canceled' AND destination=?
                               ORDER BY sta DESC LIMIT 1""", (aircraft_id, origin)).fetchone()
        if prev:
            return parse_time(prev["sta"]) + timedelta(minutes=turnaround)
        any_prev = conn.execute("""SELECT * FROM flights WHERE aircraft_id=? AND status!='canceled' ORDER BY sta DESC LIMIT 1""",
                                (aircraft_id,)).fetchone()
        if any_prev:
            raise ApiError(409, "ferry_infeasible",
                           f"前任航班落地于 {any_prev['destination']}，与调机出发地 {origin} 不一致，无法在此位置调机")
        raise ApiError(409, "ferry_infeasible", "未找到该飞机的前任航班，无法确定调机出发位置")

    def _build_segments(self, origin: str, destination: str, earliest: datetime, turnaround: int,
                        segments_in: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if segments_in:
            for i, item in enumerate(segments_in):
                seg_origin = str(item.get("origin", "")).upper().strip()
                seg_dest = str(item.get("destination", "")).upper().strip()
                if not seg_origin or not seg_dest:
                    raise ApiError(400, "invalid_segment", "航段 origin/destination 必填")
                std, sta = parse_time(item.get("std")), parse_time(item.get("sta"))
                if sta <= std:
                    raise ApiError(400, "invalid_times", "航段到达时间必须晚于起飞时间")
                if i == 0 and std < earliest:
                    raise ApiError(409, "ferry_infeasible", f"首航段起飞早于前任航班落地后过站时刻 {iso(earliest)}")
                if i > 0:
                    prev = out[-1]
                    if seg_origin != prev["destination"]:
                        raise ApiError(409, "ferry_infeasible", f"航段 {i + 1} 出发地 {seg_origin} 与上一航段到达地 {prev['destination']} 不一致")
                    if std < parse_time(prev["sta"]) + timedelta(minutes=turnaround):
                        raise ApiError(409, "ferry_infeasible", f"航段 {i + 1} 与上一航段过站时间不足 {turnaround} 分钟")
                out.append({"origin": seg_origin, "destination": seg_dest, "std": iso(std), "sta": iso(sta)})
        else:
            out.append({"origin": origin, "destination": destination,
                        "std": iso(earliest), "sta": iso(earliest + timedelta(minutes=SLOT_BLOCK_MINUTES))})
        return out

    def _aircraft_overlaps(self, conn: sqlite3.Connection, aircraft_id: str, seg_rows: list[dict[str, Any]],
                           exclude_ferry_id: int | None = None) -> bool:
        for seg in seg_rows:
            query = """SELECT fs.id FROM ferry_segments fs JOIN ferry_requests fr ON fr.id=fs.ferry_id
                       WHERE fr.aircraft_id=? AND fr.status NOT IN ('canceled','completed') AND fs.status!='canceled'
                       AND fs.std < ? AND fs.sta > ?"""
            params: list[Any] = [aircraft_id, seg["sta"], seg["std"]]
            if exclude_ferry_id:
                query += " AND fr.id<>?"
                params.append(exclude_ferry_id)
            if conn.execute(query, params).fetchone():
                return True
        return False

    def _get_ferry_by_no(self, conn: sqlite3.Connection, ferry_no: str) -> sqlite3.Row:
        ferry = conn.execute("SELECT * FROM ferry_requests WHERE ferry_no=?", (ferry_no,)).fetchone()
        if not ferry:
            raise ApiError(404, "ferry_not_found", "调机单不存在")
        return ferry

    def _ferry_detail(self, conn: sqlite3.Connection, ferry_id: int) -> dict[str, Any]:
        ferry = conn.execute("SELECT * FROM ferry_requests WHERE id=?", (ferry_id,)).fetchone()
        if not ferry:
            raise ApiError(404, "ferry_not_found", "调机单不存在")
        segments = [dict(r) for r in conn.execute("SELECT * FROM ferry_segments WHERE ferry_id=? ORDER BY seq", (ferry_id,))]
        for seg in segments:
            seg["slots"] = [dict(r) for r in conn.execute(
                "SELECT airport,slot_start,runway FROM slot_occupancy WHERE ferry_segment_id=? ORDER BY runway", (seg["id"],))]
        result = dict(ferry)
        result["segments"] = segments
        return result

    def create_ferry(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "ferry_forbidden", "当前角色不能申请调机")
        aircraft_id = str(body.get("aircraft_id", "")).strip()
        origin = str(body.get("origin", "")).upper().strip()
        destination = str(body.get("destination", "")).upper().strip()
        if not aircraft_id or not origin or not destination:
            raise ApiError(400, "missing_fields", "aircraft_id、origin、destination 必填")
        turnaround = body.get("turnaround_minutes", 45)
        if not isinstance(turnaround, int) or turnaround < 0:
            raise ApiError(400, "invalid_turnaround", "turnaround_minutes 必须是非负整数")
        plan_id = body.get("plan_id")
        if plan_id is not None and not isinstance(plan_id, int):
            raise ApiError(400, "invalid_plan", "plan_id 必须是整数")
        ferry_no = str(body.get("ferry_no", "")).strip() or None
        with self.repo.tx() as conn:
            if ferry_no:  # idempotent retry by ferry number
                existing = conn.execute("SELECT * FROM ferry_requests WHERE ferry_no=?", (ferry_no,)).fetchone()
                if existing:
                    return self._ferry_detail(conn, existing["id"])
            aircraft = conn.execute("SELECT * FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
            if not aircraft or aircraft["status"] != "active":
                raise ApiError(409, "resource_unavailable", f"飞机 {aircraft_id} 不可用")
            earliest = self._compute_earliest(conn, aircraft_id, origin, turnaround)
            seg_rows = self._build_segments(origin, destination, earliest, turnaround, body.get("segments"))
            if self._aircraft_overlaps(conn, aircraft_id, seg_rows):
                raise ApiError(409, "ferry_conflict", "同一架飞机在该时段已有调机占用，先占到时隙的一方继续")
            if plan_id is not None and not conn.execute("SELECT 1 FROM recovery_plans WHERE id=?", (plan_id,)).fetchone():
                raise ApiError(404, "plan_not_found", "恢复方案不存在")
            if not ferry_no:
                ferry_no = f"FR-{uuid.uuid4().hex[:8].upper()}"
            cur = conn.execute("""INSERT INTO ferry_requests(ferry_no,aircraft_id,origin,destination,earliest_start,
                                   turnaround_minutes,status,plan_id,created_by,created_at,updated_at)
                                  VALUES(?,?,?,?,?,?,'planned',?,?,?,?)""",
                               (ferry_no, aircraft_id, origin, destination, iso(earliest), turnaround, plan_id, actor, iso(), iso()))
            now = iso()
            for i, seg in enumerate(seg_rows, start=1):
                conn.execute("""INSERT INTO ferry_segments(ferry_id,seq,origin,destination,std,sta,status,queued_at)
                                VALUES(?,?,?,?,?,?, 'queued',?)""",
                             (cur.lastrowid, i, seg["origin"], seg["destination"], seg["std"], seg["sta"], now))
            self._process_queue(conn)
            Repository.audit(conn, plan_id, actor, role, "ferry_created",
                             {"ferry_no": ferry_no, "aircraft_id": aircraft_id, "segments": len(seg_rows)})
            return self._ferry_detail(conn, cur.lastrowid)

    def _requeue_ferry(self, conn: sqlite3.Connection, ferry: sqlite3.Row) -> None:
        """Release unexecuted segments, recompute their times from the last executed segment, then re-allocate."""
        fid = ferry["id"]
        segments = [dict(r) for r in conn.execute("SELECT * FROM ferry_segments WHERE ferry_id=? ORDER BY seq", (fid,))]
        last_exec = next((s for s in reversed(segments) if s["status"] == "executed"), None)
        durations: dict[int, timedelta] = {}
        for seg in segments:
            if seg["status"] != "executed":
                durations[seg["id"]] = parse_time(seg["sta"]) - parse_time(seg["std"])
                self._release_segment(conn, seg["id"])
                conn.execute("UPDATE ferry_segments SET status='queued',queued_at=? WHERE id=?", (iso(), seg["id"]))
        cursor = parse_time(last_exec["sta"]) + timedelta(minutes=ferry["turnaround_minutes"]) if last_exec else parse_time(ferry["earliest_start"])
        for seg in segments:
            if seg["status"] == "executed":
                continue
            std = cursor
            sta = std + durations[seg["id"]]
            conn.execute("UPDATE ferry_segments SET std=?,sta=? WHERE id=?", (iso(std), iso(sta), seg["id"]))
            cursor = sta + timedelta(minutes=ferry["turnaround_minutes"])
        new_status = "completed" if all(s["status"] == "executed" for s in segments) else "active"
        conn.execute("UPDATE ferry_requests SET status=?,updated_at=? WHERE id=?", (new_status, iso(), fid))
        self._process_queue(conn)

    def execute_ferry_segment(self, ferry_no: str, seq: int, actor: str, role: str) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "ferry_forbidden", "当前角色不能执行调机航段")
        with self.repo.tx() as conn:
            ferry = self._get_ferry_by_no(conn, ferry_no)
            seg = conn.execute("SELECT * FROM ferry_segments WHERE ferry_id=? AND seq=?", (ferry["id"], seq)).fetchone()
            if not seg:
                raise ApiError(404, "segment_not_found", "调机航段不存在")
            if seg["status"] == "executed":
                return self._ferry_detail(conn, ferry["id"])
            self._release_segment(conn, seg["id"])
            conn.execute("UPDATE ferry_segments SET status='executed' WHERE id=?", (seg["id"],))
            self._requeue_ferry(conn, ferry)
            Repository.audit(conn, ferry["plan_id"], actor, role, "ferry_segment_executed", {"ferry_no": ferry_no, "seq": seq})
            return self._ferry_detail(conn, ferry["id"])

    def cancel_ferry(self, ferry_no: str, actor: str, role: str) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "ferry_forbidden", "当前角色不能取消调机")
        with self.repo.tx() as conn:
            ferry = self._get_ferry_by_no(conn, ferry_no)
            if ferry["status"] == "canceled":
                return self._ferry_detail(conn, ferry["id"])
            for seg in conn.execute("SELECT id FROM ferry_segments WHERE ferry_id=?", (ferry["id"],)):
                self._release_segment(conn, seg["id"])
            conn.execute("UPDATE ferry_segments SET status='canceled' WHERE ferry_id=?", (ferry["id"],))
            conn.execute("UPDATE ferry_requests SET status='canceled',updated_at=? WHERE id=?", (iso(), ferry["id"]))
            self._process_queue(conn)
            Repository.audit(conn, ferry["plan_id"], actor, role, "ferry_canceled", {"ferry_no": ferry_no})
            return self._ferry_detail(conn, ferry["id"])

    def retry_ferry(self, ferry_no: str, actor: str, role: str) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "ferry_forbidden", "当前角色不能重试调机")
        with self.repo.tx() as conn:
            ferry = self._get_ferry_by_no(conn, ferry_no)
            if ferry["status"] in ("completed", "canceled"):
                raise ApiError(409, "ferry_closed", "已结束的调机不能重试")
            self._requeue_ferry(conn, ferry)
            Repository.audit(conn, ferry["plan_id"], actor, role, "ferry_retried", {"ferry_no": ferry_no})
            return self._ferry_detail(conn, ferry["id"])

    def list_ferries(self) -> dict[str, Any]:
        conn = self.repo.conn
        ferries = [self._ferry_detail(conn, r["id"]) for r in conn.execute("SELECT id FROM ferry_requests ORDER BY id DESC")]
        return {"ferries": ferries}

    def get_ferry(self, ferry_no: str) -> dict[str, Any]:
        conn = self.repo.conn
        ferry = self._get_ferry_by_no(conn, ferry_no)
        return self._ferry_detail(conn, ferry["id"])

    def slot_view(self, airport: str, date_value: str) -> dict[str, Any]:
        airport = airport.upper().strip()
        try:
            day = parse_time(date_value).replace(hour=0, minute=0, second=0, microsecond=0)
        except ApiError:
            raise ApiError(400, "invalid_date", "date 应为 ISO 8601 日期")
        end = day + timedelta(days=1)
        conn = self.repo.conn
        rows = conn.execute("""SELECT so.*, fs.seq, fr.ferry_no, fr.aircraft_id
                                FROM slot_occupancy so
                                JOIN ferry_segments fs ON fs.id=so.ferry_segment_id
                                JOIN ferry_requests fr ON fr.id=fs.ferry_id
                                WHERE so.airport=? AND so.slot_start>=? AND so.slot_start<?
                                ORDER BY so.slot_start,so.runway""", (airport, iso(day), iso(end))).fetchall()
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault((row["slot_start"], row["runway"]), []).append(dict(row))
        slots: list[dict[str, Any]] = []
        for hour in range(24):
            window = day + timedelta(hours=hour)
            for runway in RUNWAYS:
                key = (iso(window), runway)
                occupants = grouped.get(key, [])
                slots.append({"airport": airport, "window": iso(window), "runway": runway,
                              "capacity": self._slot_capacity(conn, airport, window, runway),
                              "used": len(occupants), "occupants": occupants})
        return {"airport": airport, "date": date_value, "slots": slots}

    def set_slot_capacity(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager":
            raise ApiError(403, "slot_forbidden", "只有运行经理可以维护时隙容量")
        airport = str(body.get("airport", "")).upper().strip()
        runway = str(body.get("runway", "departure")).strip()
        capacity = body.get("capacity")
        window = parse_time(body.get("slot_start"))
        if not airport or runway not in RUNWAYS or not isinstance(capacity, int) or capacity <= 0:
            raise ApiError(400, "invalid_slot", "airport、slot_start、runway(departure/arrival) 和正整数 capacity 必填")
        with self.repo.tx() as conn:
            conn.execute("""INSERT OR REPLACE INTO slot_capacity(airport,slot_start,runway,capacity) VALUES(?,?,?,?)""",
                         (airport, iso(window), runway, capacity))
            return {"airport": airport, "slot_start": iso(window), "runway": runway, "capacity": capacity}


def respond(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode()
    handler.send_response(status); handler.send_header("Content-Type", "application/json; charset=utf-8"); handler.send_header("Content-Length", str(len(raw))); handler.end_headers(); handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: AirlineRecoveryService
    web_root: Path
    def log_message(self, fmt: str, *args: Any) -> None: print(f"{self.address_string()} - {fmt % args}")
    def body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if not length: return {}
        try: value = json.loads(self.rfile.read(length))
        except json.JSONDecodeError as exc: raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(value, dict): raise ApiError(400, "invalid_json", "请求体必须是对象")
        return value
    def get_api(self, path: str, query_string: str = "") -> tuple[int, Any]:
        if path == "/health": return 200, {"status": "ok", "service": "airline-recovery"}
        actor, role = self.service.identity(self.headers)
        if path == "/api/state": return 200, self.service.state()
        if path == "/api/ferries": return 200, self.service.list_ferries()
        if path == "/api/slots":
            query = parse_qs(query_string)
            airport = (query.get("airport") or [""])[0]
            date_value = (query.get("date") or [""])[0]
            return 200, self.service.slot_view(airport, date_value)
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[:2] == ["api", "plans"] and parts[2].isdigit(): return 200, self.service.get_plan(int(parts[2]))
        if len(parts) == 3 and parts[:2] == ["api", "ferries"]: return 200, self.service.get_ferry(parts[2])
        if len(parts) == 4 and parts[:2] == ["api", "disruptions"] and parts[2].isdigit() and parts[3] == "compare": return 200, self.service.compare_plans(int(parts[2]))
        raise ApiError(404, "not_found", "接口不存在")
    def post_api(self, path: str) -> tuple[int, Any]:
        actor, role = self.service.identity(self.headers); body = self.body(); parts = [p for p in path.split("/") if p]
        table = {
            "/api/airports": lambda: (201, self.service.seed_airport(actor, role, body)),
            "/api/aircraft": lambda: (201, self.service.seed_aircraft(actor, role, body)),
            "/api/crew": lambda: (201, self.service.seed_crew(actor, role, body)),
            "/api/permits": lambda: (201, self.service.create_permit(actor, role, body)),
            "/api/flights": lambda: (201, self.service.create_flight(actor, role, body)),
            "/api/disruptions": lambda: (201, self.service.create_disruption(actor, role, body)),
            "/api/recovery-plans": lambda: (201, self.service.create_plan(actor, role, body)),
            "/api/ferries": lambda: (201, self.service.create_ferry(actor, role, body)),
            "/api/slots/capacity": lambda: (200, self.service.set_slot_capacity(actor, role, body)),
        }
        if path in table: return table[path]()
        if len(parts) == 4 and parts[:2] == ["api", "plans"] and parts[2].isdigit():
            plan_id, action = int(parts[2]), parts[3]
            if action == "assignments": return 200, self.service.add_assignment(plan_id, actor, role, body)
            if action == "validate": return 200, self.service.validate_plan(plan_id, actor, role)
            if action == "lock": return 200, self.service.lock_plan(plan_id, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "flights"] and parts[2].isdigit():
            flight_id, action = int(parts[2]), parts[3]
            if action == "cancel": return 200, self.service.cancel_flight(flight_id, actor, role, body)
            if action == "recover": return 200, self.service.recover_flight(flight_id, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "ferries"]:
            ferry_no, action = parts[2], parts[3]
            if action == "retry": return 200, self.service.retry_ferry(ferry_no, actor, role)
            if action == "cancel": return 200, self.service.cancel_ferry(ferry_no, actor, role)
        if len(parts) == 6 and parts[:2] == ["api", "ferries"] and parts[3] == "segments" and parts[5] == "execute":
            return 200, self.service.execute_ferry_segment(parts[2], int(parts[4]), actor, role)
        raise ApiError(404, "not_found", "接口不存在")
    def handle_request(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.get_api(parsed.path, parsed.query) if method == "GET" else self.post_api(parsed.path)
            respond(self, status, payload)
        except ApiError as exc:
            payload = {"error": exc.code, "message": exc.message}
            if exc.details is not None: payload["details"] = exc.details
            respond(self, exc.status, payload)
        except Exception as exc:
            print(f"unhandled error: {exc!r}"); respond(self, 500, {"error": "internal_error", "message": str(exc)})
    def do_GET(self) -> None: self.handle_request("GET")
    def do_POST(self) -> None: self.handle_request("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = AirlineRecoveryService(db_path)
    handler = type("AirlineHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"})
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default="127.0.0.1"); parser.add_argument("--port", type=int, default=PORT); parser.add_argument("--db", default=os.environ.get("AIRLINE_DB", "airline_recovery.db")); args = parser.parse_args()
    server = create_server(args.db, args.host, args.port); print(f"airline-recovery listening on http://{args.host}:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__ == "__main__": main()
