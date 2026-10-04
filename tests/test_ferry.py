import sys, tempfile, threading, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import AirlineRecoveryService, ApiError, iso, utcnow


class FerryFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = AirlineRecoveryService(Path(self.tmp.name) / "test.db")
        self.base = utcnow().replace(minute=0, second=0, microsecond=0) + timedelta(days=1)
        for code in ("AAA", "BBB", "CCC"):
            self.svc.seed_airport("ops", "ops_manager", {"code": code, "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC1", "model": "A320", "maintenance_due": iso(self.base + timedelta(days=5))})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC2", "model": "A320", "maintenance_due": iso(self.base + timedelta(days=5))})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR1", "name": "甲组", "base": "AAA", "duty_start": iso(self.base - timedelta(hours=2)), "max_duty_minutes": 720})
        # Both aircraft have a previous flight landing at the BBB outstation.
        self.svc.create_flight("sched", "scheduler", {"flight_no": "AB200", "origin": "AAA", "destination": "BBB",
                                                      "std": iso(self.base - timedelta(hours=2)), "sta": iso(self.base - timedelta(hours=1)),
                                                      "aircraft_id": "AC1", "crew_id": "CR1", "passenger_count": 100})
        self.svc.create_flight("sched", "scheduler", {"flight_no": "AB201", "origin": "AAA", "destination": "BBB",
                                                      "std": iso(self.base - timedelta(hours=2)), "sta": iso(self.base - timedelta(hours=1)),
                                                      "aircraft_id": "AC2", "crew_id": "CR1", "passenger_count": 100})
        self.turnaround = 45
        self.ferry_std = self.base - timedelta(hours=1) + timedelta(minutes=self.turnaround)
        self.dep_window = self.ferry_std.replace(minute=0, second=0, microsecond=0)

    def tearDown(self):
        self.tmp.cleanup()

    def _occupancy_rows(self, seg_id):
        return self.svc.repo.conn.execute(
            "SELECT COUNT(*) c FROM slot_occupancy WHERE ferry_segment_id=?", (seg_id,)).fetchone()["c"]

    def test_feasibility_from_previous_flight_location_and_time(self):
        # earliest_start is previous flight landing + turnaround
        ferry = self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-1", "aircraft_id": "AC1", "origin": "BBB",
                                                             "destination": "AAA", "turnaround_minutes": self.turnaround})
        self.assertEqual(ferry["earliest_start"], iso(self.base - timedelta(hours=1) + timedelta(minutes=self.turnaround)))
        self.assertEqual(ferry["segments"][0]["std"], iso(self.ferry_std))

        # location mismatch: aircraft's previous flight lands at BBB, but ferry departs AAA
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-2", "aircraft_id": "AC2", "origin": "AAA",
                                                        "destination": "BBB", "turnaround_minutes": self.turnaround})
        self.assertEqual(ctx.exception.code, "ferry_infeasible")

        # segment std before earliest start is rejected
        too_early = self.ferry_std - timedelta(minutes=10)
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-3", "aircraft_id": "AC1", "origin": "BBB",
                                                        "destination": "AAA", "turnaround_minutes": self.turnaround,
                                                        "segments": [{"origin": "BBB", "destination": "AAA", "std": iso(too_early),
                                                                      "sta": iso(too_early + timedelta(hours=1))}]})
        self.assertEqual(ctx.exception.code, "ferry_infeasible")

    def test_slot_capacity_queues_then_frees_on_cancel(self):
        self.svc.set_slot_capacity("ops", "ops_manager", {"airport": "BBB", "slot_start": iso(self.dep_window),
                                                          "runway": "departure", "capacity": 1})
        f1 = self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-1", "aircraft_id": "AC1", "origin": "BBB",
                                                          "destination": "AAA", "turnaround_minutes": self.turnaround})
        self.assertEqual(f1["segments"][0]["status"], "assigned")
        f2 = self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-2", "aircraft_id": "AC2", "origin": "BBB",
                                                          "destination": "AAA", "turnaround_minutes": self.turnaround})
        self.assertEqual(f2["segments"][0]["status"], "queued")
        self.assertEqual(f2["segments"][0]["slots"], [])

        # cancel f1 -> capacity frees, f2 moves from queue to assigned
        self.svc.cancel_ferry("FR-1", "sched", "scheduler")
        f2 = self.svc.get_ferry("FR-2")
        self.assertEqual(f2["segments"][0]["status"], "assigned")
        self.assertEqual({(s["airport"], s["runway"]) for s in f2["segments"][0]["slots"]},
                         {("BBB", "departure"), ("AAA", "arrival")})

    def test_same_aircraft_overlapping_ferry_sees_conflict(self):
        self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-1", "aircraft_id": "AC1", "origin": "BBB",
                                                     "destination": "AAA", "turnaround_minutes": self.turnaround})
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-2", "aircraft_id": "AC1", "origin": "BBB",
                                                         "destination": "AAA", "turnaround_minutes": self.turnaround})
        self.assertEqual(ctx.exception.code, "ferry_conflict")

    def test_concurrent_submission_one_wins_slot(self):
        barrier = threading.Barrier(2)
        results = {}

        def submit(name, ferry_no):
            barrier.wait()
            try:
                ferry = self.svc.create_ferry("sched", "scheduler", {"ferry_no": ferry_no, "aircraft_id": "AC1",
                                                                     "origin": "BBB", "destination": "AAA",
                                                                     "turnaround_minutes": self.turnaround})
                results[name] = ("ok", ferry["segments"][0]["status"])
            except ApiError as exc:
                results[name] = ("conflict", exc.code)

        t1 = threading.Thread(target=submit, args=("A", "FR-A"))
        t2 = threading.Thread(target=submit, args=("B", "FR-B"))
        t1.start(); t2.start(); t1.join(); t2.join()
        outcomes = list(results.values())
        self.assertEqual(sorted(o[0] for o in outcomes), ["conflict", "ok"])
        self.assertEqual([o[1] for o in outcomes if o[0] == "conflict"][0], "ferry_conflict")

    def test_status_change_requeues_and_recomputes_unexecuted_segments(self):
        seg1_std = self.ferry_std
        seg1_sta = seg1_std + timedelta(hours=1)
        seg2_std = seg1_sta + timedelta(hours=2)  # slack: later than the tightest feasible
        seg2_sta = seg2_std + timedelta(hours=1)
        ferry = self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-1", "aircraft_id": "AC1", "origin": "BBB",
                                                              "destination": "AAA", "turnaround_minutes": self.turnaround,
                                                              "segments": [
                                                                  {"origin": "BBB", "destination": "CCC", "std": iso(seg1_std), "sta": iso(seg1_sta)},
                                                                  {"origin": "CCC", "destination": "AAA", "std": iso(seg2_std), "sta": iso(seg2_sta)}]})
        self.assertEqual([s["status"] for s in ferry["segments"]], ["assigned", "assigned"])

        executed = self.svc.execute_ferry_segment("FR-1", 1, "sched", "scheduler")
        self.assertEqual(executed["status"], "active")
        seg2 = next(s for s in executed["segments"] if s["seq"] == 2)
        # unexecuted segment is recomputed to the earliest feasible time (seg1 landing + turnaround)
        self.assertEqual(seg2["std"], iso(seg1_sta + timedelta(minutes=self.turnaround)))
        self.assertEqual(seg2["status"], "assigned")
        # segment 1's released slots are gone; segment 2 holds exactly one dep + one arr slot
        self.assertEqual(self._occupancy_rows(next(s["id"] for s in executed["segments"] if s["seq"] == 1)), 0)
        self.assertEqual(self._occupancy_rows(seg2["id"]), 2)

        done = self.svc.execute_ferry_segment("FR-1", 2, "sched", "scheduler")
        self.assertEqual(done["status"], "completed")
        with self.assertRaises(ApiError) as ctx:
            self.svc.retry_ferry("FR-1", "sched", "scheduler")
        self.assertEqual(ctx.exception.code, "ferry_closed")

    def test_idempotent_retry_by_ferry_number_no_double_occupancy(self):
        first = self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-1", "aircraft_id": "AC1", "origin": "BBB",
                                                            "destination": "AAA", "turnaround_minutes": self.turnaround})
        # write "fails" and client retries with the same ferry number
        retry = self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-1", "aircraft_id": "AC1", "origin": "BBB",
                                                             "destination": "AAA", "turnaround_minutes": self.turnaround})
        self.assertEqual(retry["id"], first["id"])
        self.assertEqual(retry["ferry_no"], "FR-1")
        # no duplicate occupancy: exactly one departure + one arrival claim
        self.assertEqual(self._occupancy_rows(first["segments"][0]["id"]), 2)
        total = self.svc.repo.conn.execute("SELECT COUNT(*) c FROM slot_occupancy").fetchone()["c"]
        self.assertEqual(total, 2)

    def test_retry_endpoint_reallocates_after_capacity_frees(self):
        self.svc.set_slot_capacity("ops", "ops_manager", {"airport": "BBB", "slot_start": iso(self.dep_window),
                                                          "runway": "departure", "capacity": 1})
        self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-1", "aircraft_id": "AC1", "origin": "BBB",
                                                      "destination": "AAA", "turnaround_minutes": self.turnaround})
        queued = self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-2", "aircraft_id": "AC2", "origin": "BBB",
                                                               "destination": "AAA", "turnaround_minutes": self.turnaround})
        self.assertEqual(queued["segments"][0]["status"], "queued")
        self.svc.cancel_ferry("FR-1", "sched", "scheduler")
        # retry the queued ferry by number -> picks up freed capacity
        retried = self.svc.retry_ferry("FR-2", "sched", "scheduler")
        self.assertEqual(retried["segments"][0]["status"], "assigned")

    def test_dispatch_console_views_chain_and_slot_occupancy(self):
        self.svc.set_slot_capacity("ops", "ops_manager", {"airport": "BBB", "slot_start": iso(self.dep_window),
                                                          "runway": "departure", "capacity": 3})
        self.svc.create_ferry("sched", "scheduler", {"ferry_no": "FR-1", "aircraft_id": "AC1", "origin": "BBB",
                                                      "destination": "AAA", "turnaround_minutes": self.turnaround})
        listing = self.svc.list_ferries()
        self.assertEqual(len(listing["ferries"]), 1)
        self.assertEqual(listing["ferries"][0]["ferry_no"], "FR-1")
        detail = self.svc.get_ferry("FR-1")
        self.assertEqual({(s["airport"], s["runway"]) for s in detail["segments"][0]["slots"]},
                         {("BBB", "departure"), ("AAA", "arrival")})

        view = self.svc.slot_view("BBB", iso(self.ferry_std)[:10])
        occupied = [s for s in view["slots"] if s["used"] > 0]
        self.assertTrue(occupied)
        self.assertTrue(all(s["used"] <= s["capacity"] for s in occupied))
        self.assertEqual(occupied[0]["occupants"][0]["ferry_no"], "FR-1")

    def test_ferry_requires_scheduler_role(self):
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_ferry("viewer", "viewer", {"ferry_no": "FR-1", "aircraft_id": "AC1", "origin": "BBB",
                                                       "destination": "AAA", "turnaround_minutes": self.turnaround})
        self.assertEqual(ctx.exception.status, 403)


if __name__ == "__main__":
    unittest.main()
