#!/usr/bin/env python3
"""Airline disruption recovery engine using standard-library SQLite and HTTP."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, time, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

PORT = 8202
ROLES = {"viewer", "scheduler", "ops_manager", "auditor"}
DEFAULT_SLOT_WINDOW_MINUTES = 60
DEFAULT_SLOT_CAPACITY = 3
DEFAULT_HORIZON_WINDOWS = 24
DEFAULT_FERRY_GROUND_MINUTES = 30


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
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None, timeout=5.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._init()

    @contextmanager
    def tx(self):
        # 单连接跨线程：写事务串行化，先 BEGIN IMMEDIATE 抢到的一方继续，后到者在同一锁下看到冲突
        with self._lock:
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
            CREATE TABLE IF NOT EXISTS airports(code TEXT PRIMARY KEY, country TEXT NOT NULL, curfew_start TEXT NOT NULL, curfew_end TEXT NOT NULL,
                slot_window_minutes INTEGER NOT NULL DEFAULT %d, slot_capacity INTEGER NOT NULL DEFAULT %d, slot_horizon_windows INTEGER NOT NULL DEFAULT %d);
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
            CREATE TABLE IF NOT EXISTS ferries(
                id TEXT PRIMARY KEY, plan_id INTEGER REFERENCES recovery_plans(id) ON DELETE SET NULL,
                aircraft_id TEXT NOT NULL REFERENCES aircraft(id), status TEXT NOT NULL DEFAULT 'proposed',
                revision INTEGER NOT NULL DEFAULT 1, payload_hash TEXT NOT NULL, created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                UNIQUE(plan_id,aircraft_id)
            );
            CREATE TABLE IF NOT EXISTS ferry_legs(
                id INTEGER PRIMARY KEY AUTOINCREMENT, ferry_id TEXT NOT NULL REFERENCES ferries(id) ON DELETE CASCADE, seq INTEGER NOT NULL,
                origin TEXT NOT NULL, destination TEXT NOT NULL, duration_minutes INTEGER NOT NULL,
                predecessor_kind TEXT, predecessor_ref TEXT, requested_std TEXT, ground_minutes INTEGER NOT NULL DEFAULT %d,
                earliest_std TEXT, earliest_sta TEXT, scheduled_std TEXT, scheduled_sta TEXT,
                actual_std TEXT, actual_sta TEXT, status TEXT NOT NULL DEFAULT 'queued', queued_at TEXT,
                UNIQUE(ferry_id,seq)
            );
            CREATE TABLE IF NOT EXISTS slot_windows(
                id INTEGER PRIMARY KEY AUTOINCREMENT, airport TEXT NOT NULL REFERENCES airports(code),
                window_start TEXT NOT NULL, window_end TEXT NOT NULL, capacity INTEGER NOT NULL,
                UNIQUE(airport,window_start)
            );
            CREATE TABLE IF NOT EXISTS slot_reservations(
                id INTEGER PRIMARY KEY AUTOINCREMENT, window_id INTEGER NOT NULL REFERENCES slot_windows(id),
                ferry_leg_id INTEGER REFERENCES ferry_legs(id) ON DELETE CASCADE,
                movement TEXT NOT NULL DEFAULT 'departure', plan_id INTEGER REFERENCES recovery_plans(id) ON DELETE SET NULL,
                status TEXT NOT NULL DEFAULT 'held', created_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_slot_active_leg ON slot_reservations(window_id,ferry_leg_id,movement) WHERE status='held';
            CREATE INDEX IF NOT EXISTS idx_slot_window_status ON slot_reservations(window_id,status);
            CREATE INDEX IF NOT EXISTS idx_ferry_leg_status ON ferry_legs(status,queued_at,id);
            CREATE INDEX IF NOT EXISTS idx_ferry_status ON ferries(status);
            """
            % (DEFAULT_SLOT_WINDOW_MINUTES, DEFAULT_SLOT_CAPACITY, DEFAULT_HORIZON_WINDOWS, DEFAULT_FERRY_GROUND_MINUTES)
        )
        existing = {r["name"] for r in self.conn.execute("PRAGMA table_info(airports)")}
        if "slot_window_minutes" not in existing:
            self.conn.executescript(
                f"""
                ALTER TABLE airports ADD COLUMN slot_window_minutes INTEGER NOT NULL DEFAULT {DEFAULT_SLOT_WINDOW_MINUTES};
                ALTER TABLE airports ADD COLUMN slot_capacity INTEGER NOT NULL DEFAULT {DEFAULT_SLOT_CAPACITY};
                ALTER TABLE airports ADD COLUMN slot_horizon_windows INTEGER NOT NULL DEFAULT {DEFAULT_HORIZON_WINDOWS};
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
        window = body.get("slot_window_minutes", DEFAULT_SLOT_WINDOW_MINUTES)
        capacity = body.get("slot_capacity", DEFAULT_SLOT_CAPACITY)
        horizon = body.get("slot_horizon_windows", DEFAULT_HORIZON_WINDOWS)
        if not isinstance(window, int) or not isinstance(capacity, int) or not isinstance(horizon, int) or window <= 0 or capacity <= 0 or horizon <= 0:
            raise ApiError(400, "invalid_slot_config", "时隙窗口分钟数、容量和前瞻窗口数必须是正整数")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO airports(code,country,curfew_start,curfew_end,slot_window_minutes,slot_capacity,slot_horizon_windows) VALUES(?,?,?,?,?,?,?)",
                         (code, country, start, end, window, capacity, horizon))
            return dict(conn.execute("SELECT * FROM airports WHERE code=?", (code,)).fetchone())

    def configure_slots(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager":
            raise ApiError(403, "slot_forbidden", "只有运行经理可以调整时隙容量")
        code = str(body.get("code", "")).upper().strip()
        window = body.get("slot_window_minutes"); capacity = body.get("slot_capacity"); horizon = body.get("slot_horizon_windows")
        with self.repo.tx() as conn:
            row = conn.execute("SELECT * FROM airports WHERE code=?", (code,)).fetchone()
            if not row: raise ApiError(404, "airport_not_found", "机场不存在")
            window, capacity, horizon = (row["slot_window_minutes"] if window is None else window,
                                         row["slot_capacity"] if capacity is None else capacity,
                                         row["slot_horizon_windows"] if horizon is None else horizon)
            if not all(isinstance(v, int) and v > 0 for v in (window, capacity, horizon)):
                raise ApiError(400, "invalid_slot_config", "时隙窗口分钟数、容量和前瞻窗口数必须是正整数")
            conn.execute("UPDATE airports SET slot_window_minutes=?,slot_capacity=?,slot_horizon_windows=? WHERE code=?",
                         (window, capacity, horizon, code))
            Repository.audit(conn, None, actor, role, "slot_configured", {"airport": code, "capacity": capacity, "window_minutes": window, "horizon_windows": horizon})
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
            ferry_problems = self._ferry_lock_problems(conn, plan_id)
            if ferry_problems: raise ApiError(409, "ferry_not_ready", "方案关联的调机尚未排上时隙或存在飞机冲突", ferry_problems)
            metrics = self._metrics(conn, plan_id)
            conn.execute("""UPDATE recovery_plans SET status='locked',metrics_json=?,score_json=?,locked_at=?,locked_by=? WHERE id=?""",
                         (json.dumps(metrics, ensure_ascii=False), json.dumps(self._score(metrics)), iso(), actor, plan_id))
            for row in conn.execute("""SELECT a.*,f.flight_no FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? AND a.status!='canceled'""", (plan_id,)):
                conn.execute("UPDATE flights SET std=?,sta=?,aircraft_id=?,crew_id=?,delay_minutes=?,revision=revision+1,updated_at=? WHERE id=?",
                             (row["new_std"], row["new_sta"], row["aircraft_id"], row["crew_id"], max(0, row["delay_minutes"]), iso(), row["flight_id"]))
                conn.execute("UPDATE assignments SET status='active' WHERE id=?", (row["id"],))
            # 调机时隙从占用转为已确认：同一航段只有一条 held 记录，UPDATE 幂等，释放容量不会被重复记账
            conn.execute("UPDATE slot_reservations SET status='confirmed' WHERE plan_id=? AND status='held'", (plan_id,))
            conn.execute("UPDATE ferries SET status='confirmed',updated_at=? WHERE plan_id=? AND status='proposed'", (iso(), plan_id))
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
        result["ferries"] = [self._ferry_dict(conn, r, include_legs=True) for r in conn.execute("SELECT * FROM ferries WHERE plan_id=? ORDER BY created_at,id", (plan_id,)).fetchall()]
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

    # ------------------------------------------------------------------
    # 调机链与时隙
    # ------------------------------------------------------------------

    UNEXECUTED = ("queued", "scheduled")
    DONE = ("departed", "arrived")

    @staticmethod
    def _window_floor(moment: datetime, minutes: int) -> datetime:
        total = int(moment.timestamp() // 60)
        aligned = total - total % minutes
        return datetime.fromtimestamp(aligned * 60, tz=timezone.utc)

    def _airport_slot_config(self, conn: sqlite3.Connection, code: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM airports WHERE code=?", (code,)).fetchone()
        if not row: raise ApiError(404, "airport_not_found", f"机场 {code} 不存在")
        return row

    def _ensure_window(self, conn: sqlite3.Connection, airport: str, start: datetime, capacity: int, window_minutes: int) -> int:
        start_s = iso(start)
        row = conn.execute("SELECT id FROM slot_windows WHERE airport=? AND window_start=?", (airport, start_s)).fetchone()
        if row: return row["id"]
        cur = conn.execute("INSERT INTO slot_windows(airport,window_start,window_end,capacity) VALUES(?,?,?,?)",
                           (airport, start_s, iso(start + timedelta(minutes=window_minutes)), capacity))
        return cur.lastrowid

    def _window_used(self, conn: sqlite3.Connection, window_id: int) -> int:
        return conn.execute("SELECT COUNT(*) FROM slot_reservations WHERE window_id=? AND status='held'", (window_id,)).fetchone()[0]

    def _ferry_predecessor(self, conn: sqlite3.Connection, leg: sqlite3.Row | dict[str, Any]):
        """返回前任的 (机场, 落地时间, 可取消?)。无前任时返回 None。"""
        kind, ref = (leg["predecessor_kind"] if isinstance(leg, sqlite3.Row) else leg.get("predecessor_kind")), (leg["predecessor_ref"] if isinstance(leg, sqlite3.Row) else leg.get("predecessor_ref"))
        if not kind or ref in (None, ""):
            return None
        if kind == "flight":
            row = conn.execute("SELECT * FROM flights WHERE id=?", (int(ref),)).fetchone()
            if not row: raise ApiError(404, "predecessor_not_found", f"前任航班 {ref} 不存在")
            if row["status"] == "canceled": raise ApiError(409, "predecessor_canceled", f"前任航班 {row['flight_no']} 已取消，无法串接调机")
            return row["destination"], parse_time(row["sta"]), True
        if kind == "ferry_leg":
            row = conn.execute("SELECT * FROM ferry_legs WHERE id=?", (int(ref),)).fetchone()
            if not row: raise ApiError(404, "predecessor_not_found", f"前任调机航段 {ref} 不存在")
            return row["destination"], self._leg_clock(row), False
        raise ApiError(400, "invalid_predecessor", "predecessor_kind 只能是 flight 或 ferry_leg")

    def _leg_clock(self, leg: sqlite3.Row) -> datetime:
        """航段已有的落地时刻：实际优先，其次已排定，再其次理论最早。"""
        for col in ("actual_sta", "scheduled_sta", "earliest_sta"):
            if leg[col]: return parse_time(leg[col])
        raise ApiError(409, "leg_unready", f"调机航段 {leg['id']} 还没有可用时刻")

    def _leg_interval(self, leg: sqlite3.Row) -> tuple[datetime, datetime]:
        if leg["status"] in ("arrived", "departed") and leg["actual_std"]:
            return parse_time(leg["actual_std"]), parse_time(leg["actual_sta"] or leg["scheduled_sta"])
        if leg["scheduled_std"]:
            return parse_time(leg["scheduled_std"]), parse_time(leg["scheduled_sta"])
        return parse_time(leg["earliest_std"]), parse_time(leg["earliest_sta"])

    def _cursor_for(self, conn: sqlite3.Connection, ferry_id: str, seq: int):
        """链式游标：(当前位置机场, 最早可再起飞时间)。"""
        prev = conn.execute("SELECT * FROM ferry_legs WHERE ferry_id=? AND seq=?", (ferry_id, seq - 1)).fetchone()
        if not prev:
            return None
        if prev["status"] in self.DONE:
            return prev["destination"], parse_time(prev["actual_sta"] or prev["scheduled_sta"])
        if prev["scheduled_sta"]:
            return prev["destination"], parse_time(prev["scheduled_sta"])
        if prev["earliest_sta"]:
            return prev["destination"], parse_time(prev["earliest_sta"])
        return "wait", None

    def _try_schedule_leg(self, conn: sqlite3.Connection, leg: sqlite3.Row) -> bool:
        """在容量窗口内为一个调机航段占用时隙，成功返回 True；容量不足返回 False 保持排队。"""
        cursor = self._cursor_for(conn, leg["ferry_id"], leg["seq"])
        if cursor == "wait" or cursor is None and leg["seq"] != 1:
            return False
        earliest_dep = utcnow()
        origin = None
        if leg["seq"] == 1:
            pred = self._ferry_predecessor(conn, leg)
            if pred:
                origin, pred_arr, _ = pred
                earliest_dep = pred_arr + timedelta(minutes=leg["ground_minutes"])
            else:
                origin = leg["origin"]
        elif cursor:
            origin, ready_at = cursor
            earliest_dep = ready_at + timedelta(minutes=leg["ground_minutes"])
        if origin != leg["origin"]:
            return False
        dep_cfg, arr_cfg = self._airport_slot_config(conn, leg["origin"]), self._airport_slot_config(conn, leg["destination"])
        duration = timedelta(minutes=leg["duration_minutes"])
        if leg["requested_std"]:
            requested = parse_time(leg["requested_std"])
            if requested >= earliest_dep: earliest_dep = requested
        dep_window_min, horizon_steps = dep_cfg["slot_window_minutes"], dep_cfg["slot_horizon_windows"]
        dep_floor = self._window_floor(earliest_dep, dep_window_min)
        if earliest_dep > dep_floor:
            dep_floor += timedelta(minutes=dep_window_min)
        for step in range(horizon_steps):
            dep_start = dep_floor + timedelta(minutes=dep_window_min * step)
            dep_window = self._ensure_window(conn, leg["origin"], dep_start, dep_cfg["slot_capacity"], dep_window_min)
            if self._window_used(conn, dep_window) >= dep_cfg["slot_capacity"]:
                continue
            dep_moment = max(earliest_dep, dep_start)
            arr_moment = dep_moment + duration
            arr_window_min = arr_cfg["slot_window_minutes"]
            arr_start = self._window_floor(arr_moment, arr_window_min)
            arr_window = self._ensure_window(conn, leg["destination"], arr_start, arr_cfg["slot_capacity"], arr_window_min)
            if self._window_used(conn, arr_window) >= arr_cfg["slot_capacity"]:
                continue
            ferry = conn.execute("SELECT plan_id FROM ferries WHERE id=?", (leg["ferry_id"],)).fetchone()
            now = iso()
            conn.execute("""INSERT INTO slot_reservations(window_id,ferry_leg_id,movement,plan_id,status,created_at)
                            VALUES(?,?,?,?,?,?),(?,?,?,?,?,?)""",
                         (dep_window, leg["id"], "departure", ferry["plan_id"], "held", now,
                          arr_window, leg["id"], "arrival", ferry["plan_id"], "held", now))
            conn.execute("UPDATE ferry_legs SET status='scheduled',scheduled_std=?,scheduled_sta=?,queued_at=NULL WHERE id=?",
                         (iso(dep_moment), iso(arr_moment), leg["id"]))
            return True
        return False

    def _promote_queue(self, conn: sqlite3.Connection) -> None:
        """FIFO 重算排队航段：释放后腾出的容量按 queued_at,id 顺序给最早排队者。"""
        while True:
            legs = conn.execute("SELECT * FROM ferry_legs WHERE status='queued' ORDER BY queued_at,id").fetchall()
            progressed = False
            for leg in legs:
                ferry_id = leg["ferry_id"]
                try:
                    if self._try_schedule_leg(conn, leg):
                        self._refresh_ferry_status(conn, ferry_id)
                        progressed = True
                        break
                    if self._cursor_for(conn, ferry_id, leg["seq"]) == "wait":
                        continue
                except ApiError as exc:
                    if exc.code in ("predecessor_canceled", "predecessor_not_found"):
                        self._cancel_from(conn, leg, reason="前任不可用，调机链中断")
                        progressed = True
                        break
                    raise
            if not progressed:
                return

    def _release_leg_reservations(self, conn: sqlite3.Connection, leg_id: int) -> None:
        # 条件 UPDATE 保证重复释放不再产生效果，释放的容量绝不会被同一航段重复占用
        conn.execute("UPDATE slot_reservations SET status='released' WHERE ferry_leg_id=? AND status='held'", (leg_id,))

    def _cancel_from(self, conn: sqlite3.Connection, leg: sqlite3.Row, reason: str) -> None:
        rows = conn.execute("SELECT * FROM ferry_legs WHERE ferry_id=? AND seq>=? AND status IN ('queued','scheduled') ORDER BY seq",
                            (leg["ferry_id"], leg["seq"])).fetchall()
        for row in rows:
            self._release_leg_reservations(conn, row["id"])
        conn.execute("UPDATE ferry_legs SET status='canceled',scheduled_std=NULL,scheduled_sta=NULL,queued_at=NULL WHERE ferry_id=? AND seq>=? AND status IN ('queued','scheduled')",
                     (leg["ferry_id"], leg["seq"]))
        self._refresh_ferry_status(conn, leg["ferry_id"], reason=reason)

    def _refresh_ferry_status(self, conn: sqlite3.Connection, ferry_id: str, reason: str | None = None) -> None:
        legs = conn.execute("SELECT * FROM ferry_legs WHERE ferry_id=? ORDER BY seq", (ferry_id,)).fetchall()
        statuses = {leg["status"] for leg in legs}
        if not statuses:
            return
        if statuses == {"arrived"}:
            new_status = "completed"
        elif "queued" in statuses or "scheduled" in statuses:
            new_status = "in_progress" if statuses & {"departed", "arrived"} else None
        elif "departed" in statuses:
            new_status = "in_progress"
        elif statuses == {"canceled"}:
            new_status = "canceled"
        else:
            new_status = "canceled" if "canceled" in statuses else None
        if new_status:
            conn.execute("UPDATE ferries SET status=? WHERE id=? AND status!=?", (new_status, ferry_id, new_status))

    def _reflow_unexecuted(self, conn: sqlite3.Connection, ferry_id: str, from_seq: int) -> None:
        """调机状态一变：未执行航段退回队列，按前任新落地位置/时间重算。"""
        rows = conn.execute("SELECT * FROM ferry_legs WHERE ferry_id=? AND seq>=? ORDER BY seq", (ferry_id, from_seq)).fetchall()
        for row in rows:
            if row["status"] in self.UNEXECUTED:
                self._release_leg_reservations(conn, row["id"])
        conn.execute("""UPDATE ferry_legs SET status='queued',scheduled_std=NULL,scheduled_sta=NULL,queued_at=?
                        WHERE ferry_id=? AND seq>=? AND status IN ('queued','scheduled')""",
                     (iso(), ferry_id, from_seq))
        self._update_estimates(conn, ferry_id)
        self._refresh_ferry_status(conn, ferry_id)
        self._promote_queue(conn)

    def _update_estimates(self, conn: sqlite3.Connection, ferry_id: str) -> None:
        legs = conn.execute("SELECT * FROM ferry_legs WHERE ferry_id=? ORDER BY seq", (ferry_id,)).fetchall()
        cursor_airport = None
        cursor_time = None
        for leg in legs:
            if leg["status"] == "arrived":
                cursor_airport = leg["destination"]
                cursor_time = parse_time(leg["actual_sta"] or leg["scheduled_sta"])
                continue
            if leg["status"] == "departed":
                return
            if leg["seq"] == 1:
                pred = self._ferry_predecessor(conn, leg)
                if pred:
                    cursor_airport, cursor_time, _ = pred
                else:
                    cursor_airport, cursor_time = leg["origin"], utcnow()
            if cursor_airport != leg["origin"]:
                continue
            earliest_dep = cursor_time + timedelta(minutes=leg["ground_minutes"])
            if leg["requested_std"]:
                requested = parse_time(leg["requested_std"])
                if requested >= earliest_dep: earliest_dep = requested
            earliest_arr = earliest_dep + timedelta(minutes=leg["duration_minutes"])
            conn.execute("UPDATE ferry_legs SET earliest_std=?,earliest_sta=? WHERE id=?",
                         (iso(earliest_dep), iso(earliest_arr), leg["id"]))
            cursor_airport = leg["destination"]
            cursor_time = earliest_arr

    def create_ferry(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        """手工拼调机并接进恢复方案；同机并发由数据库唯一占用仲裁。"""
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "ferry_forbidden", "当前角色不能编排调机")
        ferry_id = str(body.get("ferry_id", "")).strip()
        aircraft_id = str(body.get("aircraft_id", "")).strip()
        plan_id = body.get("plan_id")
        legs = body.get("legs")
        if not ferry_id or not aircraft_id or not isinstance(legs, list) or not legs:
            raise ApiError(400, "invalid_ferry", "ferry_id、aircraft_id 和至少一个 legs 必填")
        if plan_id is not None and not isinstance(plan_id, int):
            raise ApiError(400, "invalid_ferry", "plan_id 必须是整数")
        normalized: list[dict[str, Any]] = []
        for i, item in enumerate(legs, start=1):
            origin = str(item.get("origin", "")).upper(); destination = str(item.get("destination", "")).upper()
            duration = item.get("duration_minutes")
            if not origin or not destination or not isinstance(duration, int) or duration <= 0:
                raise ApiError(400, "invalid_ferry_leg", f"第 {i} 段缺少合法 origin/destination/duration_minutes")
            if origin == destination:
                raise ApiError(400, "invalid_ferry_leg", f"第 {i} 段起飞机场和落地机场不能相同")
            pred_kind = item.get("predecessor_kind")
            pred_ref = item.get("predecessor_ref")
            requested = item.get("requested_std")
            ground = item.get("ground_minutes", DEFAULT_FERRY_GROUND_MINUTES)
            if not isinstance(ground, int) or ground < 0:
                raise ApiError(400, "invalid_ferry_leg", "ground_minutes 必须是非负整数")
            if i > 1 and (pred_kind or pred_ref):
                raise ApiError(400, "invalid_ferry_leg", "只有调机链首段可以手工指定前任航班，后续航段自动链接前一段")
            if i == 1:
                if pred_kind not in (None, "", "flight"):
                    raise ApiError(400, "invalid_predecessor", "调机链首段前任只能是航班(flight)")
                if pred_kind == "flight" and not isinstance(pred_ref, int):
                    raise ApiError(400, "invalid_predecessor", "predecessor_ref 必须是航班 id 整数")
                if not pred_kind and requested:
                    parse_time(requested)
            normalized.append({"seq": i, "origin": origin, "destination": destination, "duration": duration,
                               "pred_kind": pred_kind or None, "pred_ref": pred_ref if pred_kind else None,
                               "requested": iso(parse_time(requested)) if requested else None, "ground": ground})
        for prev, nxt in zip(normalized, normalized[1:]):
            if prev["destination"] != nxt["origin"]:
                raise ApiError(400, "ferry_chain_broken", f"第 {nxt['seq']} 段起飞机场必须衔接上一段落地机场 {prev['destination']}")
        payload = json.dumps({"aircraft_id": aircraft_id, "plan_id": plan_id, "legs": normalized}, ensure_ascii=False, sort_keys=True)
        payload_hash = hashlib.sha256(payload.encode()).hexdigest()
        with self.repo.tx() as conn:
            aircraft = conn.execute("SELECT * FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
            if not aircraft or aircraft["status"] != "active":
                raise ApiError(409, "resource_unavailable", f"飞机 {aircraft_id} 不可用")
            if plan_id is not None:
                plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
                if not plan: raise ApiError(404, "plan_not_found", "恢复方案不存在")
                if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能再挂调机")
            # 写入失败后按调机编号重试：相同编号 + 相同载荷幂等返回，编号撞车但内容不同则报编号冲突
            existing = conn.execute("SELECT * FROM ferries WHERE id=?", (ferry_id,)).fetchone()
            if existing:
                if existing["payload_hash"] != payload_hash:
                    raise ApiError(409, "ferry_id_conflict", f"调机编号 {ferry_id} 已被不同内容占用")
                return dict(self._ferry_dict(conn, existing, include_legs=True), idempotent=True)
            first = normalized[0]
            first_origin = first["origin"]
            first_ready = utcnow()
            if first["pred_kind"] == "flight":
                pred = self._ferry_predecessor(conn, {"predecessor_kind": first["pred_kind"], "predecessor_ref": first["pred_ref"]})
                first_origin, first_ready, _ = pred
            if first_origin != first["origin"]:
                raise ApiError(409, "ferry_origin_mismatch", f"首段必须从飞机前任落地位置 {first_origin} 起飞")
            for leg in normalized:
                self._airport_slot_config(conn, leg["origin"]); self._airport_slot_config(conn, leg["destination"])
            # 同一架飞机被两套恢复方案占用：按最早理论时刻做窗口重叠检测，后到者看到冲突
            candidate = []
            cursor_airport, cursor_time = first_origin, first_ready
            for leg in normalized:
                dep = cursor_time + timedelta(minutes=leg["ground"])
                if leg["requested"]:
                    requested = parse_time(leg["requested"])
                    if requested >= dep: dep = requested
                arr = dep + timedelta(minutes=leg["duration"])
                candidate.append((leg, dep, arr))
                cursor_airport, cursor_time = leg["destination"], arr
            for leg, dep, arr in candidate:
                other = conn.execute("""SELECT fl.*,f.aircraft_id,f.plan_id FROM ferry_legs fl JOIN ferries f ON f.id=fl.ferry_id
                                        WHERE f.aircraft_id=? AND f.id!=? AND fl.status IN ('queued','scheduled','departed')
                                        AND COALESCE(fl.scheduled_std,fl.earliest_std)<? AND COALESCE(fl.scheduled_sta,fl.earliest_sta)>?""",
                                     (aircraft_id, ferry_id, iso(arr), iso(dep))).fetchall()
                if other:
                    raise ApiError(409, "ferry_aircraft_conflict", f"飞机 {aircraft_id} 在该时段已被调机 {other[0]['ferry_id']} 占用",
                                   {"aircraft_id": aircraft_id, "other_ferry_id": other[0]["ferry_id"], "other_leg_id": other[0]["id"]})
                locked = conn.execute("""SELECT a.*,p.name plan_name FROM assignments a JOIN recovery_plans p ON p.id=a.plan_id
                                         WHERE a.aircraft_id=? AND p.status='locked' AND a.status!='canceled'
                                         AND a.new_std<? AND a.new_sta>?""",
                                      (aircraft_id, iso(arr), iso(dep))).fetchall()
                if locked:
                    raise ApiError(409, "ferry_aircraft_conflict", f"飞机 {aircraft_id} 在该时段已被锁定方案 {locked[0]['plan_name']} 占用",
                                   {"aircraft_id": aircraft_id, "conflict_plan_id": locked[0]["plan_id"]})
            try:
                conn.execute("""INSERT INTO ferries(id,plan_id,aircraft_id,status,payload_hash,created_by,created_at,updated_at)
                                VALUES(?,?,?,?,?,?,?,?)""",
                             (ferry_id, plan_id, aircraft_id, "proposed", payload_hash, actor, iso(), iso()))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "ferry_aircraft_conflict", "同一架飞机在该恢复方案下已有调机，或调机编号并发冲突") from exc
            now = iso()
            for leg, dep, arr in candidate:
                conn.execute("""INSERT INTO ferry_legs(ferry_id,seq,origin,destination,duration_minutes,predecessor_kind,predecessor_ref,
                                requested_std,ground_minutes,earliest_std,earliest_sta,status,queued_at)
                                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                             (ferry_id, leg["seq"], leg["origin"], leg["destination"], leg["duration"],
                              leg["pred_kind"], leg["pred_ref"], leg["requested"], leg["ground"],
                              iso(dep), iso(arr), "queued", now))
            Repository.audit(conn, plan_id, actor, role, "ferry_created", {"ferry_id": ferry_id, "aircraft_id": aircraft_id, "legs": len(normalized)})
            self._promote_queue(conn)
            row = conn.execute("SELECT * FROM ferries WHERE id=?", (ferry_id,)).fetchone()
            return self._ferry_dict(conn, row, include_legs=True)

    def ferry_event(self, ferry_id: str, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "ferry_forbidden", "当前角色不能更新调机状态")
        event = str(body.get("event", "")).strip()
        if event not in {"depart", "arrive", "cancel_leg", "cancel"}:
            raise ApiError(400, "invalid_event", "event 必须是 depart、arrive、cancel_leg 或 cancel")
        seq = body.get("seq")
        at = parse_time(body.get("at")) if body.get("at") else utcnow()
        reason = str(body.get("reason", "")).strip()
        with self.repo.tx() as conn:
            ferry = conn.execute("SELECT * FROM ferries WHERE id=?", (ferry_id,)).fetchone()
            if not ferry: raise ApiError(404, "ferry_not_found", "调机不存在")
            expected = body.get("expected_revision")
            if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
            if ferry["revision"] != expected: raise ApiError(409, "revision_conflict", "调机版本已变化，状态更新被拒绝")
            if event == "cancel":
                if not reason: raise ApiError(400, "reason_required", "取消调机必须填写原因")
                pending = conn.execute("SELECT * FROM ferry_legs WHERE ferry_id=? AND status IN ('queued','scheduled') ORDER BY seq", (ferry_id,)).fetchall()
                for leg in pending: self._release_leg_reservations(conn, leg["id"])
                conn.execute("UPDATE ferry_legs SET status='canceled',scheduled_std=NULL,scheduled_sta=NULL,queued_at=NULL WHERE ferry_id=? AND status IN ('queued','scheduled')", (ferry_id,))
                conn.execute("UPDATE ferries SET status='canceled',revision=revision+1,updated_at=? WHERE id=?", (iso(), ferry_id))
                Repository.audit(conn, ferry["plan_id"], actor, role, "ferry_canceled", {"ferry_id": ferry_id, "reason": reason})
                self._promote_queue(conn)
                return self._ferry_dict(conn, conn.execute("SELECT * FROM ferries WHERE id=?", (ferry_id,)).fetchone(), include_legs=True)
            if not isinstance(seq, int): raise ApiError(400, "seq_required", "该事件必须提供航段序号 seq")
            leg = conn.execute("SELECT * FROM ferry_legs WHERE ferry_id=? AND seq=?", (ferry_id, seq)).fetchone()
            if not leg: raise ApiError(404, "ferry_leg_not_found", f"调机没有第 {seq} 段")
            if event == "depart":
                if leg["status"] != "scheduled":
                    raise ApiError(409, "leg_not_scheduled", f"第 {seq} 段当前状态 {leg['status']}，尚未排到时隙不能报起飞")
                conn.execute("UPDATE ferry_legs SET status='departed',actual_std=COALESCE(actual_std,?) WHERE id=?", (iso(at), leg["id"]))
                conn.execute("UPDATE slot_reservations SET status='departed' WHERE ferry_leg_id=? AND movement='departure' AND status='held'", (leg["id"],))
            elif event == "arrive":
                if leg["status"] == "scheduled":
                    conn.execute("UPDATE ferry_legs SET status='departed',actual_std=COALESCE(actual_std,?) WHERE id=?", (leg["scheduled_std"], leg["id"]))
                    conn.execute("UPDATE slot_reservations SET status='departed' WHERE ferry_leg_id=? AND movement='departure' AND status='held'", (leg["id"],))
                elif leg["status"] != "departed":
                    raise ApiError(409, "leg_not_active", f"第 {seq} 段当前状态 {leg['status']}，不能报落地")
                dep_actual = (conn.execute("SELECT actual_std FROM ferry_legs WHERE id=?", (leg["id"],)).fetchone())["actual_std"]
                if at < parse_time(dep_actual):
                    raise ApiError(400, "invalid_event_time", "落地时间不能早于起飞时间")
                conn.execute("UPDATE ferry_legs SET status='arrived',actual_sta=? WHERE id=?", (iso(at), leg["id"]))
                conn.execute("UPDATE slot_reservations SET status='arrived' WHERE ferry_leg_id=? AND movement='arrival' AND status='held'", (leg["id"],))
                # 状态一变：后续未执行航段退回队列，按新的落地位置与时间重算
                self._reflow_unexecuted(conn, ferry_id, seq + 1)
            else:
                if not reason: raise ApiError(400, "reason_required", "取消航段必须填写原因")
                if leg["status"] not in self.UNEXECUTED:
                    raise ApiError(409, "leg_already_active", f"第 {seq} 段已进入执行状态，不能取消")
                self._cancel_from(conn, leg, reason=reason)
            conn.execute("UPDATE ferries SET revision=revision+1,updated_at=? WHERE id=?", (iso(), ferry_id))
            self._refresh_ferry_status(conn, ferry_id)
            self._promote_queue(conn)
            Repository.audit(conn, ferry["plan_id"], actor, role, f"ferry_{event}", {"ferry_id": ferry_id, "seq": seq})
            return self._ferry_dict(conn, conn.execute("SELECT * FROM ferries WHERE id=?", (ferry_id,)).fetchone(), include_legs=True)

    def _ferry_lock_problems(self, conn: sqlite3.Connection, plan_id: int) -> list[dict[str, Any]]:
        problems: list[dict[str, Any]] = []
        for ferry in conn.execute("SELECT * FROM ferries WHERE plan_id=?", (plan_id,)).fetchall():
            queued = conn.execute("SELECT seq FROM ferry_legs WHERE ferry_id=? AND status='queued'", (ferry["id"],)).fetchall()
            if queued:
                problems.append({"code": "ferry_legs_waiting", "ferry_id": ferry["id"], "seqs": [r["seq"] for r in queued]})
            for row in conn.execute("SELECT * FROM ferry_legs WHERE ferry_id=? AND status IN ('scheduled','departed','arrived')", (ferry["id"],)).fetchall():
                start, end = self._leg_interval(row)
                other = conn.execute("""SELECT fl.ferry_id,fl.seq FROM ferry_legs fl JOIN ferries f ON f.id=fl.ferry_id
                                        WHERE fl.ferry_id!=? AND f.aircraft_id=? AND fl.status IN ('scheduled','departed','arrived')
                                        AND COALESCE(fl.scheduled_std,fl.actual_std)<? AND COALESCE(fl.scheduled_sta,fl.actual_sta)>?""",
                                     (ferry["id"], ferry["aircraft_id"], iso(end), iso(start))).fetchall()
                for item in other:
                    problems.append({"code": "ferry_aircraft_overlap", "ferry_id": ferry["id"], "seq": row["seq"],
                                     "other_ferry_id": item["ferry_id"], "other_seq": item["seq"], "resource": ferry["aircraft_id"]})
        return problems

    def _ferry_dict(self, conn: sqlite3.Connection, ferry: sqlite3.Row, include_legs: bool = False) -> dict[str, Any]:
        result = dict(ferry)
        legs = [dict(r) for r in conn.execute("SELECT * FROM ferry_legs WHERE ferry_id=? ORDER BY seq", (ferry["id"],)).fetchall()]
        for leg in legs:
            if leg["status"] == "queued":
                pos = conn.execute("SELECT COUNT(*)+1 FROM ferry_legs WHERE status='queued' AND (queued_at,id)<(?,?)",
                                   (leg["queued_at"], leg["id"])).fetchone()[0]
                leg["queue_position"] = pos
        result["legs"] = legs
        result["waiting_count"] = sum(1 for leg in legs if leg["status"] == "queued")
        if include_legs:
            result["reservations"] = [dict(r) for r in conn.execute(
                """SELECT sr.*,sw.airport,sw.window_start,sw.window_end,sw.capacity FROM slot_reservations sr
                   JOIN slot_windows sw ON sw.id=sr.window_id WHERE sr.ferry_leg_id IN
                   (SELECT id FROM ferry_legs WHERE ferry_id=?) ORDER BY sw.window_start,sr.movement""", (ferry["id"],)).fetchall()]
        return result

    def list_ferries(self, plan_id: int | None = None) -> dict[str, Any]:
        conn = self.repo.conn
        sql = "SELECT * FROM ferries"; args: tuple[Any, ...] = ()
        if plan_id is not None:
            sql += " WHERE plan_id=?"; args = (plan_id,)
        sql += " ORDER BY created_at,id"
        ferries = [self._ferry_dict(conn, row) for row in conn.execute(sql, args).fetchall()]
        queue = [dict(r) for r in conn.execute(
            """SELECT fl.id,fl.ferry_id,fl.seq,fl.origin,fl.destination,fl.queued_at,f.aircraft_id,
                      ROW_NUMBER() OVER (ORDER BY fl.queued_at,fl.id) AS queue_position
               FROM ferry_legs fl JOIN ferries f ON f.id=fl.ferry_id WHERE fl.status='queued' ORDER BY fl.queued_at,fl.id""").fetchall()]
        return {"ferries": ferries, "queue": queue, "queue_depth": len(queue)}

    def slots_view(self, airport: str | None, date: str | None) -> dict[str, Any]:
        conn = self.repo.conn
        if date:
            try:
                day = datetime.fromisoformat(date)
            except ValueError as exc:
                raise ApiError(400, "invalid_date", "date 格式应为 YYYY-MM-DD") from exc
            day_start = day.replace(tzinfo=timezone.utc)
            day_end = day_start + timedelta(days=1)
            window_sql = "SELECT * FROM slot_windows WHERE window_start>=? AND window_start<?"; args: list[Any] = [iso(day_start), iso(day_end)]
            if airport: window_sql += " AND airport=?"; args.append(airport)
        elif airport:
            window_sql = "SELECT * FROM slot_windows WHERE airport=?"; args = [airport]
        else:
            window_sql = "SELECT * FROM slot_windows"; args = []
        windows = []
        for w in conn.execute(window_sql + " ORDER BY airport,window_start", args).fetchall():
            used = conn.execute("SELECT COUNT(*) FROM slot_reservations WHERE window_id=? AND status='held'", (w["id"],)).fetchone()[0]
            confirmed = conn.execute("SELECT COUNT(*) FROM slot_reservations WHERE window_id=? AND status='confirmed'", (w["id"],)).fetchone()[0]
            reservations = [dict(r) for r in conn.execute(
                """SELECT sr.id,sr.ferry_leg_id,sr.movement,sr.status,sr.plan_id,fl.ferry_id,fl.seq,f.aircraft_id
                   FROM slot_reservations sr LEFT JOIN ferry_legs fl ON fl.id=sr.ferry_leg_id
                   LEFT JOIN ferries f ON f.id=fl.ferry_id WHERE sr.window_id=? ORDER BY sr.id""", (w["id"],)).fetchall()]
            window = dict(w)
            window["used"] = used; window["confirmed"] = confirmed
            window["available"] = w["capacity"] - used
            window["reservations"] = reservations
            windows.append(window)
        queue = [dict(r) for r in conn.execute(
            """SELECT fl.id,fl.ferry_id,fl.seq,fl.origin,fl.destination,fl.queued_at,f.aircraft_id
               FROM ferry_legs fl JOIN ferries f ON f.id=fl.ferry_id WHERE fl.status='queued'
               AND (? IS NULL OR fl.origin=? OR fl.destination=?) ORDER BY fl.queued_at,fl.id""",
            (airport, airport, airport)).fetchall()] if airport else []
        return {"airport": airport, "date": date, "windows": windows, "waiting": queue}

    def state(self) -> dict[str, Any]:
        conn = self.repo.conn
        flights = [dict(r) for r in conn.execute("SELECT * FROM flights ORDER BY std")]
        plans = [self.get_plan(r["id"]) for r in conn.execute("SELECT id FROM recovery_plans ORDER BY id DESC LIMIT 20")]
        ferries = [self._ferry_dict(conn, r) for r in conn.execute("SELECT * FROM ferries ORDER BY created_at,id")]
        queue = [dict(r) for r in conn.execute(
            """SELECT fl.id,fl.ferry_id,fl.seq,fl.origin,fl.destination,fl.queued_at,f.aircraft_id,
                      ROW_NUMBER() OVER (ORDER BY fl.queued_at,fl.id) AS queue_position
               FROM ferry_legs fl JOIN ferries f ON f.id=fl.ferry_id WHERE fl.status='queued' ORDER BY fl.queued_at,fl.id""").fetchall()]
        return {"flights": flights, "disruptions": [dict(r) for r in conn.execute("SELECT * FROM disruptions ORDER BY id DESC")],
                "plans": plans, "ferries": ferries, "ferry_queue": queue, "server_time": iso()}


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
    def get_api(self, path: str) -> tuple[int, Any]:
        if path == "/health": return 200, {"status": "ok", "service": "airline-recovery"}
        actor, role = self.service.identity(self.headers)
        if path == "/api/state": return 200, self.service.state()
        parsed = urlparse(path)
        query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) == 3 and parts[:2] == ["api", "plans"] and parts[2].isdigit(): return 200, self.service.get_plan(int(parts[2]))
        if len(parts) == 4 and parts[:2] == ["api", "disruptions"] and parts[2].isdigit() and parts[3] == "compare": return 200, self.service.compare_plans(int(parts[2]))
        if parts == ["api", "ferries"]:
            plan = query.get("plan_id")
            if plan is not None and not plan.isdigit(): raise ApiError(400, "invalid_query", "plan_id 必须是整数")
            return 200, self.service.list_ferries(int(plan) if plan else None)
        if parts == ["api", "slots"]:
            airport = query.get("airport", "").upper().strip() or None
            return 200, self.service.slots_view(airport, query.get("date"))
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
            "/api/slot-config": lambda: (200, self.service.configure_slots(actor, role, body)),
            "/api/ferries": lambda: (201, self.service.create_ferry(actor, role, body)),
        }
        if path in table: return table[path]()
        if len(parts) == 3 and parts[:2] == ["api", "ferries"] and parts[2] != "":
            return 200, self.service.ferry_event(parts[2], actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "plans"] and parts[2].isdigit():
            plan_id, action = int(parts[2]), parts[3]
            if action == "assignments": return 200, self.service.add_assignment(plan_id, actor, role, body)
            if action == "validate": return 200, self.service.validate_plan(plan_id, actor, role)
            if action == "lock": return 200, self.service.lock_plan(plan_id, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "flights"] and parts[2].isdigit():
            flight_id, action = int(parts[2]), parts[3]
            if action == "cancel": return 200, self.service.cancel_flight(flight_id, actor, role, body)
            if action == "recover": return 200, self.service.recover_flight(flight_id, actor, role, body)
        raise ApiError(404, "not_found", "接口不存在")
    def handle_request(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.get_api(parsed.path) if method == "GET" else self.post_api(parsed.path)
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
