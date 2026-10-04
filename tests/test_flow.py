import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import AirlineRecoveryService, ApiError, iso, utcnow


class AirlineFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = AirlineRecoveryService(Path(self.tmp.name) / "test.db")
        base = utcnow() + timedelta(days=1)
        self.svc.seed_airport("ops", "ops_manager", {"code": "AAA", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_airport("ops", "ops_manager", {"code": "BBB", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC1", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC2", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR1", "name": "甲组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR2", "name": "乙组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.create_permit("ops", "ops_manager", {"origin": "AAA", "destination": "BBB", "valid_from": iso(base - timedelta(days=1)), "valid_to": iso(base + timedelta(days=2))})
        self.base = base

    def tearDown(self): self.tmp.cleanup()

    def make_flight(self, number, aircraft, crew):
        return self.svc.create_flight("sched", "scheduler", {"flight_no": number, "origin": "AAA", "destination": "BBB", "std": iso(self.base), "sta": iso(self.base + timedelta(hours=2)), "aircraft_id": aircraft, "crew_id": crew, "passenger_count": 150})

    def test_complete_recovery_and_manual_recovery(self):
        flight = self.make_flight("AB100", "AC1", "CR1")
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1", "starts_at": iso(self.base - timedelta(hours=1)), "ends_at": iso(self.base + timedelta(hours=3))})
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "换飞机并延误", "assignments": [{"flight_id": flight["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=3)), "new_sta": iso(self.base + timedelta(hours=5)), "missed_connections": 4}]})
        check = self.svc.validate_plan(plan["id"], "auditor", "auditor")
        self.assertTrue(check["valid"], check["problems"])
        locked = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.assertEqual(locked["status"], "locked")
        self.assertEqual(locked["metrics"]["affected_passengers"], 150)
        canceled = self.svc.cancel_flight(flight["id"], "sched", "scheduler", {"reason": "后续机务检查"})["flight"]
        recovered = self.svc.recover_flight(flight["id"], "sched", "scheduler", {"expected_revision": canceled["revision"], "new_std": iso(self.base + timedelta(hours=8)), "new_sta": iso(self.base + timedelta(hours=10))})["flight"]
        self.assertEqual(recovered["status"], "scheduled")

    def test_locked_resource_conflict_and_permissions(self):
        flight1 = self.make_flight("AB101", "AC1", "CR1")
        disruption1 = self.svc.create_disruption("sched", "scheduler", {"kind": "crew_timeout", "resource_id": "CR1", "starts_at": iso(self.base), "ends_at": iso(self.base + timedelta(hours=2))})
        plan1 = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption1["id"], "name": "方案一", "assignments": [{"flight_id": flight1["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=2)), "new_sta": iso(self.base + timedelta(hours=4))}]})
        self.svc.lock_plan(plan1["id"], "ops", "ops_manager", {"expected_revision": 1})
        flight2 = self.make_flight("AB102", "AC2", "CR2")
        disruption2 = self.svc.create_disruption("sched", "scheduler", {"kind": "airport_closure", "resource_id": "AAA", "starts_at": iso(self.base), "ends_at": iso(self.base + timedelta(hours=1))})
        plan2 = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption2["id"], "name": "冲突方案", "assignments": [{"flight_id": flight2["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=2, minutes=30)), "new_sta": iso(self.base + timedelta(hours=4, minutes=30))}]})
        with self.assertRaises(ApiError) as ctx:
            self.svc.lock_plan(plan2["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.assertEqual(ctx.exception.code, "locked_resource_conflict")
        with self.assertRaises(ApiError) as ctx:
            self.svc.lock_plan(plan2["id"], "sched", "scheduler", {"expected_revision": 1})
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(ApiError) as ctx:
            self.svc.add_assignment(plan1["id"], "sched", "scheduler", {"expected_revision": 1, "flight_id": flight1["id"], "aircraft_id": "AC1", "crew_id": "CR1", "new_std": iso(self.base), "new_sta": iso(self.base + timedelta(hours=2))})
        self.assertEqual(ctx.exception.code, "plan_locked")


if __name__ == "__main__": unittest.main()
