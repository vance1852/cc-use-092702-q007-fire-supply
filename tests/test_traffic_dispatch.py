from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from collection_logistics.api import JsonApplication
from collection_logistics.clock import FrozenClock
from collection_logistics.errors import Conflict, Forbidden, InventoryIncompatible, InventoryShortage
from collection_logistics.planning import AllocationRequest, RiskPoint, allocate_capacity, latest_streak
from collection_logistics.service import CollectionLogisticsService
from collection_logistics.risk import DemandBucket, inventory_coverage, mark_to_risk, traffic_gap


class PlanningTests(unittest.TestCase):
    def test_latest_down_streak_uses_first_close_as_base(self) -> None:
        streak = latest_streak([
            RiskPoint("2026-09-18", Decimal("108")),
            RiskPoint("2026-09-19", Decimal("105")),
            RiskPoint("2026-09-20", Decimal("102")),
            RiskPoint("2026-09-21", Decimal("98")),
        ])
        self.assertEqual(streak.direction, "down")
        self.assertEqual(streak.sessions, 4)
        self.assertEqual(streak.start_date, "2026-09-18")
        self.assertEqual(streak.end_close, Decimal("98"))

    def test_allocation_is_stable_and_does_not_exceed_capacity(self) -> None:
        rows = allocate_capacity(Decimal("100"), [
            AllocationRequest("later", Decimal("80"), 20, "2026-09-24T09:00:00Z"),
            AllocationRequest("first", Decimal("70"), 10, "2026-09-24T10:00:00Z"),
        ])
        self.assertEqual(rows[0]["dispatch_id"], "first")
        self.assertEqual(rows[0]["allocated_units"], "70.000")
        self.assertEqual(rows[1]["allocated_units"], "30.000")

    def test_inventory_coverage_and_traffic_gap(self) -> None:
        coverage = inventory_coverage(
            [{"center_id": "receiving-vault", "preservation_resource_kind": "tow-truck", "available_units": "250"}],
            [DemandBucket("receiving-vault", "tow-truck", Decimal("100"), Decimal("20"))],
        )
        self.assertEqual(coverage[0]["coverage_days"], "2.30")
        self.assertTrue(coverage[0]["below_three_days"])
        gap = traffic_gap(
            opening_inventory=Decimal("100"),
            confirmed_inbound=Decimal("30"),
            forecast_demand=Decimal("120"),
            protected_reserve=Decimal("40"),
        )
        self.assertEqual(gap["traffic_gap"], "30.000")

    def test_mark_to_risk_groups_deterministically(self) -> None:
        result = mark_to_risk(
            [{"position_id": "p1", "risk_index": "HUMIDITY", "quantity_units": "100", "baseline_value": "105"}],
            {"HUMIDITY": Decimal("98")},
        )
        self.assertEqual(result["unrealized_pnl_cny"], "-700.00")


class CollectionLogisticsServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = CollectionLogisticsService(self.connection, self.clock)
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_facility("plan", {"center_id": "collection-east", "name": "北部标本事件保藏中心", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_units": "500000"})
        self.service.create_facility("plan", {"center_id": "receiving-vault-b", "name": "沿海终端", "kind": "receiving-vault", "timezone": "Asia/Shanghai", "capacity_units": "800000"})
        self.service.create_route("plan", {"corridor_id": "transfer-east-1", "origin_center_id": "collection-east", "destination_center_id": "receiving-vault-b", "preservation_resource_kind": "preservation-box", "hourly_capacity": "100000", "delay_basis_points": 25, "response_minutes": 36})

    def tearDown(self) -> None:
        self.connection.close()

    def risk_record(self, day: int, close: str) -> dict[str, object]:
        return self.service.record_risk_record("plan", {"risk_index": "HUMIDITY", "duty_date": f"2026-09-{day}", "index_value": close, "source_revision": f"r-{day}", "observed_at": f"2026-09-{day}T21:00:00Z"})

    def test_risk_record_revisions_preserve_history(self) -> None:
        first = self.risk_record(23, "98")
        second = self.service.record_risk_record("plan", {"risk_index": "HUMIDITY", "duty_date": "2026-09-23", "index_value": "97.8", "source_revision": "r-23-corrected", "observed_at": "2026-09-23T22:00:00Z"})
        self.assertNotEqual(first["risk_record_id"], second["risk_record_id"])
        rows = self.connection.execute("SELECT * FROM risk_index_risk_records ORDER BY risk_record_id").fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["supersedes_risk_record_id"], rows[0]["risk_record_id"])

    def test_dispatch_request_replay_and_payload_conflict(self) -> None:
        payload = {"dispatch_id": "nom-1", "corridor_id": "transfer-east-1", "specimen_event_id": "herbarium-room", "duty_date": "2026-09-25", "requested_units": "80000", "priority": 10, "idempotency_key": "key-1"}
        first = self.service.submit_dispatch("dispatch", payload)
        self.assertEqual(first, self.service.submit_dispatch("dispatch", payload))
        changed = dict(payload, requested_units="81000")
        with self.assertRaises(Conflict):
            self.service.submit_dispatch("dispatch", changed)

    def test_outage_reduces_allocation_and_deployment_consumes_inventory(self) -> None:
        self.service.announce_restriction("risk", "transfer-east-1", "2026-09-25T00:00:00Z", "2026-09-25T23:59:59Z", "50", "检修")
        for number, requested, priority in ((1, "40000", 10), (2, "30000", 20)):
            self.service.submit_dispatch("dispatch", {"dispatch_id": f"nom-{number}", "corridor_id": "transfer-east-1", "specimen_event_id": f"specimen_event-{number}", "duty_date": "2026-09-25", "requested_units": requested, "priority": priority, "idempotency_key": f"key-{number}"})
        allocation = self.service.allocate("dispatch", "transfer-east-1", "2026-09-25")
        self.assertEqual(allocation["available_units"], "50000.000")
        self.assertEqual(allocation["allocations"][1]["allocated_units"], "10000.000")
        self.service.add_inventory_lot("dispatch", {"preservation_resource_lot_id": "lot-1", "center_id": "collection-east", "preservation_resource_kind": "preservation-box", "grade": "HUMIDITY", "quantity_units": "60000", "unit_cost_cny": "91", "received_at": "2026-09-24T06:00:00Z"})
        deployment = self.service.confirm_deployment("dispatch", {"deployment_id": "deployment-1", "dispatch_id": "nom-1", "expected_revision": 2, "idempotency_key": "deploy-key-1"})
        self.assertEqual(deployment["deployed_units"], "40000.000")
        self.assertEqual(self.service.inventory_lot("lot-1")["available_units"], "20000.000")
        self.assertEqual([item["preservation_resource_lot_id"] for item in deployment["selected_lots"]], ["lot-1"])

    def test_scenario_is_approved_and_replayed_by_input(self) -> None:
        self.risk_record(23, "98")
        self.service.add_inventory_lot("dispatch", {"preservation_resource_lot_id": "lot-1", "center_id": "collection-east", "preservation_resource_kind": "preservation-box", "grade": "HUMIDITY", "quantity_units": "60000", "unit_cost_cny": "91", "received_at": "2026-09-24T06:00:00Z"})
        self.service.create_scenario("plan", {"scenario_id": "restart", "name": "库房环境恢复", "risk_index_drop_percent": "9", "route_capacity_changes": {"transfer-east-1": "20"}, "demand_changes": {"collection-east:preservation-box": "-5"}})
        with self.assertRaises(Forbidden):
            self.service.approve_scenario("plan", "restart", 1)
        self.service.approve_scenario("risk", "restart", 1)
        first = self.service.run_scenario("plan", "restart", "2026-09-23")
        second = self.service.run_scenario("plan", "restart", "2026-09-23")
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["run_id"], second["run_id"])

    def _seed_fire_line(self) -> None:
        """东部站高压水泵任务，外加各类看似有货实则不可选的干扰批次。"""
        self.service.create_facility("plan", {"center_id": "station-east", "name": "东部保护站", "kind": "patrol-station", "timezone": "Asia/Shanghai", "capacity_units": "2000"})
        self.service.create_facility("plan", {"center_id": "station-west", "name": "西部保护站", "kind": "patrol-station", "timezone": "Asia/Shanghai", "capacity_units": "2000"})
        self.service.create_facility("plan", {"center_id": "fire-front", "name": "火线", "kind": "road-section", "timezone": "Asia/Shanghai", "capacity_units": "2000"})
        self.service.create_route("plan", {"corridor_id": "fire-line-east", "origin_center_id": "station-east", "destination_center_id": "fire-front", "preservation_resource_kind": "water-bag", "required_grade": "HP-PUMP", "hourly_capacity": "500", "delay_basis_points": 0, "response_minutes": 20})

    def _lot(self, lot_id: str, center: str, grade: str, quantity: str, **extra: object) -> None:
        payload = {"preservation_resource_lot_id": lot_id, "center_id": center, "preservation_resource_kind": "water-bag", "grade": grade, "quantity_units": quantity, "unit_cost_cny": "120", "received_at": "2026-09-20T06:00:00Z"}
        payload.update(extra)
        self.service.add_inventory_lot("dispatch", payload)

    def _submit_and_allocate(self, dispatch_id: str, units: str, key: str, priority: int = 10) -> None:
        self.service.submit_dispatch("dispatch", {"dispatch_id": dispatch_id, "corridor_id": "fire-line-east", "specimen_event_id": f"drill-{dispatch_id}", "duty_date": "2026-09-25", "requested_units": units, "priority": priority, "idempotency_key": key})
        self.service.allocate("dispatch", "fire-line-east", "2026-09-25")

    def _confirm(self, deployment_id: str, dispatch_id: str, key: str) -> dict[str, object]:
        return self.service.confirm_deployment("dispatch", {"deployment_id": deployment_id, "dispatch_id": dispatch_id, "expected_revision": 2, "idempotency_key": key})

    def test_confirmation_selects_only_lots_meeting_all_constraints(self) -> None:
        self._seed_fire_line()
        self._lot("east-hp-1", "station-east", "HP-PUMP", "30", expires_at="2026-10-01T00:00:00Z")
        self._lot("east-hp-2", "station-east", "HP-PUMP", "70", expires_at="2027-03-01T00:00:00Z")
        # 干扰项：同站但等级不符、已过期、已冻结；异站账面货物。
        self._lot("east-normal", "station-east", "NORMAL", "200")
        self._lot("east-expired", "station-east", "HP-PUMP", "60", expires_at="2026-09-01T00:00:00Z")
        self._lot("east-frozen", "station-east", "HP-PUMP", "60", lot_status="frozen")
        self._lot("west-normal", "station-west", "NORMAL", "500")
        self._lot("west-hp", "station-west", "HP-PUMP", "500")
        self._submit_and_allocate("fire-1", "100", "fire-key-1")
        result = self._confirm("fire-deploy-1", "fire-1", "fire-deploy-key-1")
        # FEFO：先到期的合格批次 east-hp-1 出 30，再由 east-hp-2 补足 70。
        self.assertEqual([item["preservation_resource_lot_id"] for item in result["selected_lots"]], ["east-hp-1", "east-hp-2"])
        self.assertEqual(result["deployed_units"], "100.000")
        self.assertEqual(result["created_by"], "dispatch")
        self.assertEqual(result["constraints"]["center_id"], "station-east")
        self.assertEqual(result["constraints"]["required_grade"], "HP-PUMP")
        # 干扰批次库存原样不动。
        self.assertEqual(self.service.inventory_lot("east-normal")["available_units"], "200")
        self.assertEqual(self.service.inventory_lot("east-expired")["available_units"], "60")
        self.assertEqual(self.service.inventory_lot("east-frozen")["available_units"], "60")
        self.assertEqual(self.service.inventory_lot("west-normal")["available_units"], "500")
        self.assertEqual(self.service.inventory_lot("west-hp")["available_units"], "500")

    def test_incompatible_and_shortage_return_distinct_business_reasons(self) -> None:
        self._seed_fire_line()
        self._lot("east-normal", "station-east", "NORMAL", "200")
        self._lot("east-expired", "station-east", "HP-PUMP", "60", expires_at="2026-09-01T00:00:00Z")
        self._submit_and_allocate("fire-1", "10", "fire-key-1")
        with self.assertRaises(InventoryIncompatible) as incompatible:
            self._confirm("fire-deploy-1", "fire-1", "fire-deploy-key-1")
        self.assertEqual(incompatible.exception.code, "inventory_incompatible")
        rejected = {row["preservation_resource_lot_id"] for row in incompatible.exception.details["rejected_lots"]}
        self.assertIn("east-normal", rejected)
        self.assertIn("east-expired", rejected)
        # 失败不能扣减任何库存。
        self.assertEqual(self.service.inventory_lot("east-normal")["available_units"], "200")
        self.assertEqual(self.service.inventory_lot("east-expired")["available_units"], "60")

        # 补一个合格但数量不足的批次后，错误原因应变为库存不足。
        self._lot("east-hp", "station-east", "HP-PUMP", "4", expires_at="2026-12-01T00:00:00Z")
        with self.assertRaises(InventoryShortage) as shortage:
            self._confirm("fire-deploy-1", "fire-1", "fire-deploy-key-1")
        self.assertEqual(shortage.exception.code, "inventory_shortage")
        self.assertEqual(shortage.exception.details["required_units"], "10.000")
        self.assertEqual(shortage.exception.details["compatible_available_units"], "4.000")
        self.assertEqual(self.service.inventory_lot("east-hp")["available_units"], "4")

    def test_identical_confirmation_is_safe_to_replay(self) -> None:
        self._seed_fire_line()
        self._lot("east-hp", "station-east", "HP-PUMP", "100", expires_at="2027-01-01T00:00:00Z")
        self._submit_and_allocate("fire-1", "100", "fire-key-1")
        payload = {"deployment_id": "fire-deploy-1", "dispatch_id": "fire-1", "expected_revision": 2, "idempotency_key": "fire-deploy-key-1"}
        first = self.service.confirm_deployment("dispatch", payload)
        second = self.service.confirm_deployment("dispatch", payload)
        self.assertTrue(second["replayed"])
        self.assertEqual(second["selected_lots"], first["selected_lots"])
        self.assertEqual(second["deployed_units"], first["deployed_units"])
        # 重放不二次扣减。
        self.assertEqual(self.service.inventory_lot("east-hp")["available_units"], "0.000")
        deployment_rows = self.connection.execute("SELECT COUNT(*) c FROM deployments").fetchone()["c"]
        self.assertEqual(deployment_rows, 1)
        # 同幂等键不同内容必须拒绝。
        changed = dict(payload, deployment_id="fire-deploy-other")
        with self.assertRaises(Conflict):
            self.service.confirm_deployment("dispatch", changed)

    def test_failed_confirmation_leaves_no_inventory_or_audit_change(self) -> None:
        self._seed_fire_line()
        self._lot("east-normal", "station-east", "NORMAL", "200")
        self._submit_and_allocate("fire-1", "10", "fire-key-1")
        audit_before = self.connection.execute("SELECT COUNT(*) c FROM traffic_audit_events").fetchone()["c"]
        with self.assertRaises(InventoryIncompatible):
            self._confirm("fire-deploy-1", "fire-1", "fire-deploy-key-1")
        audit_after = self.connection.execute("SELECT COUNT(*) c FROM traffic_audit_events").fetchone()["c"]
        self.assertEqual(audit_before, audit_after)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) c FROM deployments").fetchone()["c"], 0)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) c FROM deployment_lot_items").fetchone()["c"], 0)

    def test_audit_chain_detects_tampering(self) -> None:
        self.assertTrue(self.service.audit_chain("audit")["valid"])
        self.connection.execute("UPDATE traffic_audit_events SET payload_json='{}' WHERE event_id=1")
        self.assertFalse(self.service.audit_chain("audit")["valid"])

    def test_api_exposes_browser_free_boundary(self) -> None:
        app = JsonApplication(self.service)
        self.assertEqual(app.handle("GET", "/health").status, 200)
        response = app.handle("GET", "/risk_records/summary/HUMIDITY", {"X-Actor-Id": "plan"})
        self.assertEqual(response.status, 404)
        self.assertEqual(response.body["error"]["code"], "not_found")


if __name__ == "__main__":
    unittest.main()
