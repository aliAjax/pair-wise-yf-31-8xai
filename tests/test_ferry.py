import sys, tempfile, unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import AirlineRecoveryService, ApiError, iso, utcnow


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


class FerryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = AirlineRecoveryService(Path(self.tmp.name) / "ferry.db")
        # 整点时刻，所有窗口边界可预测
        self.t0 = utcnow().replace(minute=0, second=0, microsecond=0) + timedelta(hours=2)
        # 每窗口容量 1、前瞻仅 2 个窗口，便于制造排队
        for code in ("AAA", "BBB", "CCC"):
            self.svc.seed_airport("ops", "ops_manager",
                                  {"code": code, "country": "CN", "slot_capacity": 1, "slot_horizon_windows": 2})
        for ac in ("AC1", "AC2", "AC3", "AC4"):
            self.svc.seed_aircraft("ops", "ops_manager",
                                   {"id": ac, "model": "A320", "maintenance_due": iso(self.t0 + timedelta(days=5))})
        for cr in ("CR1", "CR2"):
            self.svc.seed_crew("ops", "ops_manager", {"id": cr, "name": cr, "base": "AAA",
                                                      "duty_start": iso(self.t0 - timedelta(hours=2)), "max_duty_minutes": 720})
        for o, d in (("AAA", "BBB"), ("BBB", "AAA"), ("BBB", "CCC"), ("CCC", "BBB"),
                     ("CCC", "AAA"), ("AAA", "CCC")):
            self.svc.create_permit("ops", "ops_manager",
                                   {"origin": o, "destination": d,
                                    "valid_from": iso(self.t0 - timedelta(days=1)), "valid_to": iso(self.t0 + timedelta(days=2))})

    def tearDown(self):
        self.tmp.cleanup()

    def make_flight(self, number, aircraft="AC1", crew="CR1"):
        return self.svc.create_flight("sched", "scheduler", {
            "flight_no": number, "origin": "AAA", "destination": "BBB",
            "std": iso(self.t0), "sta": iso(self.t0 + timedelta(hours=2)),
            "aircraft_id": aircraft, "crew_id": crew, "passenger_count": 100})

    def ferry_body(self, fid, aircraft="AC1", flight_id=None, chain=False):
        legs = [{"origin": "BBB", "destination": "CCC" if chain else "AAA", "duration_minutes": 60,
                 "ground_minutes": 30}]
        if flight_id is not None:
            legs[0].update(predecessor_kind="flight", predecessor_ref=flight_id)
        if chain:
            legs.append({"origin": "CCC", "destination": "AAA", "duration_minutes": 60, "ground_minutes": 30})
        return {"ferry_id": fid, "aircraft_id": aircraft, "legs": legs}

    def test_feasibility_from_predecessor_and_fifo_queue(self):
        flight = self.make_flight("AB100")
        f1 = self.svc.create_ferry("a", "scheduler", self.ferry_body("FY1", "AC1", flight["id"]))
        leg1 = f1["legs"][0]
        self.assertEqual(leg1["status"], "scheduled")
        # 前任 12:00 落地 BBB + 30 分钟地面 -> 13:00 窗口离场，14:00 落地
        self.assertEqual(leg1["scheduled_std"], iso(self.t0 + timedelta(hours=3)))
        self.assertEqual(leg1["scheduled_sta"], iso(self.t0 + timedelta(hours=4)))
        # 不同飞机但同一机场时隙：容量 1，前两架各占 13:00、14:00 窗口，第三架起排队
        f2 = self.svc.create_ferry("b", "scheduler", self.ferry_body("FY2", "AC2", flight["id"]))
        self.assertEqual(f2["legs"][0]["status"], "scheduled")
        f3 = self.svc.create_ferry("c", "scheduler", self.ferry_body("FY3", "AC3", flight["id"]))
        self.assertEqual(f3["legs"][0]["status"], "queued")
        self.assertEqual(f3["legs"][0]["queue_position"], 1)
        f4 = self.svc.create_ferry("d", "scheduler", self.ferry_body("FY4", "AC4", flight["id"]))
        self.assertEqual(f4["legs"][0]["queue_position"], 2)
        self.assertEqual(self.svc.list_ferries()["queue_depth"], 2)

    def test_wrong_origin_rejected(self):
        flight = self.make_flight("AB101")  # 落地 BBB
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_ferry("a", "scheduler", {
                "ferry_id": "FY9", "aircraft_id": "AC1",
                "legs": [{"origin": "CCC", "destination": "AAA", "duration_minutes": 60, "ground_minutes": 30,
                          "predecessor_kind": "flight", "predecessor_ref": flight["id"]}]})
        self.assertEqual(ctx.exception.code, "ferry_origin_mismatch")

    def test_release_promotes_queue_without_double_occupancy(self):
        flight = self.make_flight("AB102")
        f1 = self.svc.create_ferry("a", "scheduler", self.ferry_body("FY10", "AC1", flight["id"]))
        self.svc.create_ferry("b", "scheduler", self.ferry_body("FY11", "AC2", flight["id"]))
        f3 = self.svc.create_ferry("c", "scheduler", self.ferry_body("FY12", "AC3", flight["id"]))
        self.assertEqual(f3["legs"][0]["status"], "queued")
        self.svc.ferry_event("FY10", "ops", "ops_manager",
                             {"event": "arrive", "seq": 1, "expected_revision": f1["revision"],
                              "at": iso(self.t0 + timedelta(hours=4))})
        fy12 = next(f for f in self.svc.list_ferries()["ferries"] if f["id"] == "FY12")
        self.assertEqual(fy12["legs"][0]["status"], "scheduled")
        # 每个窗口占用不得超过容量；同一航段也不能有重复 held
        for w in self.svc.slots_view(None, None)["windows"]:
            self.assertLessEqual(w["used"], 1, w)
        dupes = self.svc.repo.conn.execute(
            "SELECT ferry_leg_id,window_id,movement,COUNT(*) FROM slot_reservations WHERE status='held' GROUP BY 1,2,3 HAVING COUNT(*)>1").fetchall()
        self.assertEqual(dupes, [])

    def test_retry_by_ferry_id_is_idempotent(self):
        flight = self.make_flight("AB103")
        body = self.ferry_body("FY20", "AC1", flight["id"])
        first = self.svc.create_ferry("a", "scheduler", body)
        second = self.svc.create_ferry("a", "scheduler", body)  # 写入失败后按编号重试
        self.assertTrue(second.get("idempotent"))
        self.assertEqual(second["id"], first["id"])
        self.assertEqual(len(self.svc.list_ferries()["ferries"]), 1)
        self.assertEqual(self.svc.repo.conn.execute("SELECT COUNT(*) FROM slot_reservations WHERE status='held'").fetchone()[0], 2)

    def test_same_aircraft_concurrent_submission(self):
        flight = self.make_flight("AB104")

        def submit(fid):
            try:
                self.svc.create_ferry(fid, "scheduler", self.ferry_body(fid, "AC1", flight["id"]))
                return "ok"
            except ApiError as exc:
                return exc.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(submit, ["FY30", "FY31"]))
        self.assertEqual(sorted(results), ["ferry_aircraft_conflict", "ok"])

    def test_late_arrival_reflows_remaining_legs(self):
        flight = self.make_flight("AB105")
        ferry = self.svc.create_ferry("a", "scheduler", self.ferry_body("FY40", "AC1", flight["id"], chain=True))
        self.assertEqual([l["status"] for l in ferry["legs"]], ["scheduled", "scheduled"])
        second_before = ferry["legs"][1]["scheduled_std"]
        late = self.t0 + timedelta(hours=7)  # 第一段晚 3 小时落地 CCC
        updated = self.svc.ferry_event("FY40", "ops", "ops_manager",
                                       {"event": "arrive", "seq": 1, "expected_revision": 1, "at": iso(late)})
        second = updated["legs"][1]
        self.assertEqual(second["status"], "scheduled")
        self.assertGreater(second["scheduled_std"], second_before)
        # 第二段重排后不早于新落地 + 30 分钟地面（落在下一窗口）
        self.assertGreaterEqual(parse_iso(second["scheduled_std"]), late + timedelta(minutes=30))
        self.assertEqual(updated["status"], "in_progress")

    def test_stale_revision_event_rejected(self):
        flight = self.make_flight("AB106")
        self.svc.create_ferry("a", "scheduler", self.ferry_body("FY50", "AC1", flight["id"]))
        with self.assertRaises(ApiError) as ctx:
            self.svc.ferry_event("FY50", "ops", "ops_manager",
                                 {"event": "depart", "seq": 1, "expected_revision": 99})
        self.assertEqual(ctx.exception.code, "revision_conflict")

    def test_cancel_ferry_releases_and_promotes(self):
        flight = self.make_flight("AB107")
        f1 = self.svc.create_ferry("a", "scheduler", self.ferry_body("FY60", "AC1", flight["id"]))
        self.svc.create_ferry("b", "scheduler", self.ferry_body("FY61", "AC2", flight["id"]))
        f3 = self.svc.create_ferry("c", "scheduler", self.ferry_body("FY62", "AC3", flight["id"]))
        self.assertEqual(f3["legs"][0]["status"], "queued")
        self.svc.ferry_event("FY60", "ops", "ops_manager",
                             {"event": "cancel", "expected_revision": f1["revision"], "reason": "飞机修复无需调机"})
        fy62 = next(f for f in self.svc.list_ferries()["ferries"] if f["id"] == "FY62")
        self.assertEqual(fy62["legs"][0]["status"], "scheduled")
        held_after_cancel = self.svc.repo.conn.execute(
            "SELECT COUNT(*) FROM slot_reservations sr JOIN ferry_legs fl ON fl.id=sr.ferry_leg_id WHERE fl.ferry_id='FY60' AND sr.status='held'").fetchone()[0]
        self.assertEqual(held_after_cancel, 0)

    def test_lock_plan_confirms_ferry_slots(self):
        flight = self.make_flight("AB108")
        disruption = self.svc.create_disruption("sched", "scheduler", {
            "kind": "airport_closure", "resource_id": "AAA",
            "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(hours=1))})
        plan = self.svc.create_plan("sched", "scheduler", {
            "disruption_id": disruption["id"], "name": "含调机方案",
            "assignments": [{"flight_id": flight["id"], "aircraft_id": "AC1", "crew_id": "CR1",
                             "new_std": iso(self.t0 + timedelta(hours=6)),
                             "new_sta": iso(self.t0 + timedelta(hours=8))}]})
        ferry = self.svc.create_ferry("a", "scheduler",
                                      dict(self.ferry_body("FY70", "AC1", flight["id"]), plan_id=plan["id"]))
        self.assertEqual(ferry["legs"][0]["status"], "scheduled")
        locked = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.assertEqual(locked["status"], "locked")
        confirmed = self.svc.repo.conn.execute(
            "SELECT DISTINCT status FROM slot_reservations WHERE plan_id=?", (plan["id"],)).fetchall()
        self.assertEqual({r["status"] for r in confirmed}, {"confirmed"})
        # 已锁定方案不能再挂调机
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_ferry("b", "scheduler",
                                  dict(self.ferry_body("FY71", "AC2", flight["id"]), plan_id=plan["id"]))
        self.assertEqual(ctx.exception.code, "plan_locked")

    def test_lock_blocked_while_ferry_waits(self):
        flight = self.make_flight("AB109")
        disruption = self.svc.create_disruption("sched", "scheduler", {
            "kind": "airport_closure", "resource_id": "AAA",
            "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(hours=1))})
        plan = self.svc.create_plan("sched", "scheduler", {
            "disruption_id": disruption["id"], "name": "排队中的调机",
            "assignments": [{"flight_id": flight["id"], "aircraft_id": "AC3", "crew_id": "CR1",
                             "new_std": iso(self.t0 + timedelta(hours=6)),
                             "new_sta": iso(self.t0 + timedelta(hours=8))}]})
        self.svc.create_ferry("a", "scheduler", self.ferry_body("FY80", "AC1", flight["id"]))
        self.svc.create_ferry("b", "scheduler", self.ferry_body("FY82", "AC3", flight["id"]))
        waiting = self.svc.create_ferry("c", "scheduler",
                                        dict(self.ferry_body("FY81", "AC2", flight["id"]), plan_id=plan["id"]))
        self.assertEqual(waiting["legs"][0]["status"], "queued")
        with self.assertRaises(ApiError) as ctx:
            self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.assertEqual(ctx.exception.code, "ferry_not_ready")

    def test_dispatch_views_show_chain_and_slots(self):
        flight = self.make_flight("AB111")
        self.svc.create_ferry("a", "scheduler", self.ferry_body("FY90", "AC1", flight["id"], chain=True))
        listing = self.svc.list_ferries()
        self.assertEqual(len(listing["ferries"][0]["legs"]), 2)
        slots = self.svc.slots_view("BBB", None)
        self.assertTrue(any(w["used"] >= 1 for w in slots["windows"]))
        reservation = slots["windows"][0]["reservations"][0]
        self.assertIn("ferry_id", reservation)
        state = self.svc.state()
        self.assertIn("ferries", state)
        self.assertIn("ferry_queue", state)


if __name__ == "__main__":
    unittest.main()
