from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from collection_logistics.api import JsonApplication
from collection_logistics.clock import FrozenClock
from collection_logistics.errors import Conflict, Forbidden, InventoryIncompatible, InventoryInsufficient
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
        deployment = self.service.confirm_deployment("dispatch", {"deployment_id": "deployment-1", "dispatch_id": "nom-1", "required_grade": "HUMIDITY", "idempotency_key": "deploy-key-1", "note": ""})
        self.assertEqual(deployment["deployed_units"], "40000.000")
        self.assertEqual(self.service.inventory_lot("lot-1")["available_units"], "20000.000")

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

    def test_audit_chain_detects_tampering(self) -> None:
        self.assertTrue(self.service.audit_chain("audit")["valid"])
        self.connection.execute("UPDATE traffic_audit_events SET payload_json='{}' WHERE event_id=1")
        self.assertFalse(self.service.audit_chain("audit")["valid"])

    def _prepare_dispatch(self, requested: str = "40000", dispatch_id: str = "nom-east", key: str = "nom-east-key") -> None:
        self.service.submit_dispatch("dispatch", {"dispatch_id": dispatch_id, "corridor_id": "transfer-east-1", "specimen_event_id": "fire-drill-east", "duty_date": "2026-09-25", "requested_units": requested, "priority": 10, "idempotency_key": key})
        self.service.allocate("dispatch", "transfer-east-1", "2026-09-25")

    def _lot(self, lot_id: str, center: str, grade: str, quantity: str, *, expires_on=None, active=True, received_at="2026-09-20T06:00:00Z") -> None:
        payload = {"preservation_resource_lot_id": lot_id, "center_id": center, "preservation_resource_kind": "preservation-box", "grade": grade, "quantity_units": quantity, "unit_cost_cny": "91", "received_at": received_at, "active": active}
        if expires_on is not None:
            payload["expires_on"] = expires_on
        self.service.add_inventory_lot("dispatch", payload)

    def test_cross_station_lot_cannot_cover_east_shortage(self) -> None:
        # 西部站有充足普通储水袋，但东部站账面为零：必须判库存不足，绝不允许跨站扣减。
        self.service.create_facility("plan", {"center_id": "collection-west", "name": "西部保护站", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_units": "500000"})
        self._prepare_dispatch()
        self._lot("lot-west", "collection-west", "HIGH-PRESSURE", "40000")
        with self.assertRaises(InventoryInsufficient) as caught:
            self.service.confirm_deployment("dispatch", {"deployment_id": "dep-1", "dispatch_id": "nom-east", "required_grade": "HIGH-PRESSURE", "idempotency_key": "dep-key-1", "note": ""})
        self.assertEqual(caught.exception.code, "inventory_insufficient")
        self.assertEqual(self.service.inventory_lot("lot-west")["available_units"], "40000")
        rows = self.connection.execute("SELECT COUNT(*) c FROM traffic_audit_events WHERE event_type='deployment.dispatched'").fetchone()
        self.assertEqual(rows["c"], 0)

    def test_stock_present_but_grade_incompatible_is_distinct_reason(self) -> None:
        # 东部站账面有货且物理可用，但只是普通储水袋，不适配高压水泵接口。
        self._prepare_dispatch()
        self._lot("lot-plain", "collection-east", "STANDARD", "40000")
        with self.assertRaises(InventoryIncompatible) as caught:
            self.service.confirm_deployment("dispatch", {"deployment_id": "dep-1", "dispatch_id": "nom-east", "required_grade": "HIGH-PRESSURE", "idempotency_key": "dep-key-1", "note": ""})
        self.assertEqual(caught.exception.code, "inventory_incompatible")
        self.assertIn("HIGH-PRESSURE", str(caught.exception))
        self.assertEqual(self.service.inventory_lot("lot-plain")["available_units"], "40000")

    def test_expired_and_inactive_lots_are_excluded(self) -> None:
        self._prepare_dispatch()
        self._lot("lot-expired", "collection-east", "HIGH-PRESSURE", "30000", expires_on="2026-09-10")
        self._lot("lot-inactive", "collection-east", "HIGH-PRESSURE", "30000", active=False)
        with self.assertRaises(InventoryIncompatible):
            self.service.confirm_deployment("dispatch", {"deployment_id": "dep-1", "dispatch_id": "nom-east", "required_grade": "HIGH-PRESSURE", "idempotency_key": "dep-key-1", "note": ""})

    def test_successful_confirmation_records_selection_operator_and_basis(self) -> None:
        self._prepare_dispatch()
        self._lot("lot-hp-1", "collection-east", "HIGH-PRESSURE", "40000", expires_on="2027-01-01")
        result = self.service.confirm_deployment("dispatch", {"deployment_id": "dep-1", "dispatch_id": "nom-east", "required_grade": "HIGH-PRESSURE", "idempotency_key": "dep-key-1", "note": "防火演练"})
        self.assertEqual(result["state"], "in_transit")
        self.assertEqual(result["deployed_units"], "40000.000")
        self.assertEqual(result["operator"]["user_id"], "dispatch")
        self.assertEqual(len(result["selected_lots"]), 1)
        chosen = result["selected_lots"][0]
        self.assertEqual(chosen["preservation_resource_lot_id"], "lot-hp-1")
        self.assertEqual(chosen["units_allocated"], "40000.000")
        self.assertIn("grade=HIGH-PRESSURE", chosen["constraint_basis"])
        self.assertIn("center=collection-east", chosen["constraint_basis"])
        self.assertEqual(result["constraints"]["preservation_resource_kind"], "preservation-box")
        self.assertEqual(self.service.inventory_lot("lot-hp-1")["available_units"], "0.000")
        item = self.connection.execute("SELECT * FROM deployment_lot_items WHERE deployment_id='dep-1'").fetchone()
        self.assertEqual(item["preservation_resource_lot_id"], "lot-hp-1")

    def test_identical_request_replays_without_double_deduction(self) -> None:
        self._prepare_dispatch()
        self._lot("lot-hp-1", "collection-east", "HIGH-PRESSURE", "40000", expires_on="2027-01-01")
        payload = {"deployment_id": "dep-1", "dispatch_id": "nom-east", "required_grade": "HIGH-PRESSURE", "idempotency_key": "dep-key-1", "note": "防火演练"}
        first = self.service.confirm_deployment("dispatch", payload)
        second = self.service.confirm_deployment("dispatch", payload)
        self.assertEqual(first, second)
        self.assertEqual(self.service.inventory_lot("lot-hp-1")["available_units"], "0.000")
        deployments = self.connection.execute("SELECT COUNT(*) c FROM deployments").fetchone()["c"]
        deductions = self.connection.execute("SELECT revision FROM preservation_resource_lots WHERE preservation_resource_lot_id='lot-hp-1'").fetchone()["revision"]
        self.assertEqual(deployments, 1)
        self.assertEqual(deductions, 2)
        with self.assertRaises(Conflict):
            self.service.confirm_deployment("dispatch", dict(payload, note="不同内容"))

    def test_multi_lot_selection_uses_first_expiry_first_out(self) -> None:
        self._prepare_dispatch(requested="50000")
        self._lot("lot-later", "collection-east", "HIGH-PRESSURE", "30000", expires_on="2028-06-01", received_at="2026-09-20T06:00:00Z")
        self._lot("lot-sooner", "collection-east", "HIGH-PRESSURE", "30000", expires_on="2027-06-01", received_at="2026-09-21T06:00:00Z")
        result = self.service.confirm_deployment("dispatch", {"deployment_id": "dep-1", "dispatch_id": "nom-east", "required_grade": "HIGH-PRESSURE", "idempotency_key": "dep-key-1", "note": ""})
        self.assertEqual([item["preservation_resource_lot_id"] for item in result["selected_lots"]], ["lot-sooner", "lot-later"])
        self.assertEqual(result["selected_lots"][0]["units_allocated"], "30000.000")
        self.assertEqual(result["selected_lots"][1]["units_allocated"], "20000.000")
        self.assertEqual(self.service.inventory_lot("lot-sooner")["available_units"], "0.000")
        self.assertEqual(self.service.inventory_lot("lot-later")["available_units"], "10000.000")

    def test_api_exposes_browser_free_boundary(self) -> None:
        app = JsonApplication(self.service)
        self.assertEqual(app.handle("GET", "/health").status, 200)
        response = app.handle("GET", "/risk_records/summary/HUMIDITY", {"X-Actor-Id": "plan"})
        self.assertEqual(response.status, 404)
        self.assertEqual(response.body["error"]["code"], "not_found")


if __name__ == "__main__":
    unittest.main()
