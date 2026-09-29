"""贯通风险指数、转运路线、应急资源库存、调度申请和情景分析的离线验收。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .errors import InventoryIncompatible, InventoryShortage
from .service import CollectionLogisticsService


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    service = CollectionLogisticsService(connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
    for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
        service.create_user(user_id, user_id, role)
    for index, close in enumerate(("108", "105", "102", "100", "98", "96"), start=18):
        service.record_risk_record("plan", {"risk_index": "HUMIDITY", "duty_date": f"2026-09-{index}", "index_value": close, "source_revision": f"rev-{index}", "observed_at": f"2026-09-{index}T21:00:00Z"})
    service.create_facility("plan", {"center_id": "collection-east", "name": "北部标本事件保藏中心", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_units": "500000"})
    service.create_facility("plan", {"center_id": "receiving-vault-b", "name": "沿海终端", "kind": "receiving-vault", "timezone": "Asia/Shanghai", "capacity_units": "800000"})
    service.create_route("plan", {"corridor_id": "transfer-east-1", "origin_center_id": "collection-east", "destination_center_id": "receiving-vault-b", "preservation_resource_kind": "preservation-box", "hourly_capacity": "100000", "delay_basis_points": 25, "response_minutes": 36})
    service.add_inventory_lot("dispatch", {"preservation_resource_lot_id": "lot-001", "center_id": "collection-east", "preservation_resource_kind": "preservation-box", "grade": "HUMIDITY", "quantity_units": "150000", "unit_cost_cny": "91.25", "received_at": "2026-09-24T06:00:00Z"})
    service.submit_dispatch("dispatch", {"dispatch_id": "nom-001", "corridor_id": "transfer-east-1", "specimen_event_id": "herbarium-room-east", "duty_date": "2026-09-25", "requested_units": "80000", "priority": 10, "idempotency_key": "nom-key-001"})
    allocation = service.allocate("dispatch", "transfer-east-1", "2026-09-25")
    deployment = service.confirm_deployment("dispatch", {"deployment_id": "deployment-001", "dispatch_id": "nom-001", "expected_revision": 2, "idempotency_key": "deploy-key-001"})
    replayed = service.confirm_deployment("dispatch", {"deployment_id": "deployment-001", "dispatch_id": "nom-001", "expected_revision": 2, "idempotency_key": "deploy-key-001"})

    # 防火演练：高压水泵任务只能选取东部站高压接口、在有效期且可用的储水袋批次。
    fire_drill = _fire_drill(service)

    service.create_scenario("plan", {"scenario_id": "storage-recovery", "name": "主干路恢复通行与标本事件需求回落", "risk_index_drop_percent": "9", "route_capacity_changes": {"transfer-east-1": "20"}, "demand_changes": {"collection-east:preservation-box": "-5"}})
    service.approve_scenario("risk", "storage-recovery", 1)
    scenario = service.run_scenario("plan", "storage-recovery", "2026-09-23")
    result = {"status": "ok", "index": service.risk_summary("HUMIDITY"), "plan_id": allocation["plan_id"], "deployment": deployment, "deployment_replayed": replayed, "fire_drill": fire_drill, "scenario_run_id": scenario["run_id"], "audit": service.audit_chain("audit"), "workspace": workspace.name}
    connection.close()
    return result


def _fire_drill(service: CollectionLogisticsService) -> dict[str, object]:
    service.create_facility("plan", {"center_id": "station-east", "name": "东部保护站", "kind": "patrol-station", "timezone": "Asia/Shanghai", "capacity_units": "2000"})
    service.create_facility("plan", {"center_id": "station-west", "name": "西部保护站", "kind": "patrol-station", "timezone": "Asia/Shanghai", "capacity_units": "2000"})
    service.create_facility("plan", {"center_id": "fire-front", "name": "防火演练火线", "kind": "road-section", "timezone": "Asia/Shanghai", "capacity_units": "2000"})
    service.create_route("plan", {"corridor_id": "fire-line-east", "origin_center_id": "station-east", "destination_center_id": "fire-front", "preservation_resource_kind": "water-bag", "required_grade": "HP-PUMP", "hourly_capacity": "500", "delay_basis_points": 0, "response_minutes": 20})
    # 东部站：两个高压批次（先到期的数量不足，需跨批次凑齐），另有一个不适配高压泵的普通储水袋。
    service.add_inventory_lot("dispatch", {"preservation_resource_lot_id": "east-hp-1", "center_id": "station-east", "preservation_resource_kind": "water-bag", "grade": "HP-PUMP", "quantity_units": "30", "unit_cost_cny": "120", "received_at": "2026-09-20T06:00:00Z", "expires_at": "2026-10-01T00:00:00Z"})
    service.add_inventory_lot("dispatch", {"preservation_resource_lot_id": "east-hp-2", "center_id": "station-east", "preservation_resource_kind": "water-bag", "grade": "HP-PUMP", "quantity_units": "70", "unit_cost_cny": "120", "received_at": "2026-09-22T06:00:00Z", "expires_at": "2027-03-01T00:00:00Z"})
    service.add_inventory_lot("dispatch", {"preservation_resource_lot_id": "east-normal", "center_id": "station-east", "preservation_resource_kind": "water-bag", "grade": "NORMAL", "quantity_units": "200", "unit_cost_cny": "40", "received_at": "2026-09-22T06:00:00Z"})
    # 西部站：账面有大量普通储水袋，但既不在车辆出发地也不适配高压接口，绝不能被选中。
    service.add_inventory_lot("dispatch", {"preservation_resource_lot_id": "west-normal", "center_id": "station-west", "preservation_resource_kind": "water-bag", "grade": "NORMAL", "quantity_units": "500", "unit_cost_cny": "40", "received_at": "2026-09-18T06:00:00Z"})

    service.submit_dispatch("dispatch", {"dispatch_id": "fire-001", "corridor_id": "fire-line-east", "specimen_event_id": "drill-A", "duty_date": "2026-09-25", "requested_units": "100", "priority": 10, "idempotency_key": "fire-key-001"})
    service.allocate("dispatch", "fire-line-east", "2026-09-25")
    confirmed = service.confirm_deployment("dispatch", {"deployment_id": "fire-deploy-001", "dispatch_id": "fire-001", "expected_revision": 2, "idempotency_key": "fire-deploy-key-001"})
    replay = service.confirm_deployment("dispatch", {"deployment_id": "fire-deploy-001", "dispatch_id": "fire-001", "expected_revision": 2, "idempotency_key": "fire-deploy-key-001"})

    # 有库存但不兼容：第二车队要 10 个，可站内普通储水袋不适配高压泵且高压批次已耗尽。
    service.submit_dispatch("dispatch", {"dispatch_id": "fire-002", "corridor_id": "fire-line-east", "specimen_event_id": "drill-B", "duty_date": "2026-09-25", "requested_units": "10", "priority": 20, "idempotency_key": "fire-key-002"})
    service.allocate("dispatch", "fire-line-east", "2026-09-25")
    incompatible: dict[str, object] = {}
    try:
        service.confirm_deployment("dispatch", {"deployment_id": "fire-deploy-002", "dispatch_id": "fire-002", "expected_revision": 2, "idempotency_key": "fire-deploy-key-002"})
    except InventoryIncompatible as exc:
        incompatible = {"code": exc.code, "details": exc.details}

    # 兼容但数量不足：临时补充少量高压批次后仍不够一车次（需要 10，只有 4）。
    service.add_inventory_lot("dispatch", {"preservation_resource_lot_id": "east-hp-3", "center_id": "station-east", "preservation_resource_kind": "water-bag", "grade": "HP-PUMP", "quantity_units": "4", "unit_cost_cny": "120", "received_at": "2026-09-23T06:00:00Z", "expires_at": "2026-12-01T00:00:00Z"})
    shortage: dict[str, object] = {}
    try:
        service.confirm_deployment("dispatch", {"deployment_id": "fire-deploy-002", "dispatch_id": "fire-002", "expected_revision": 2, "idempotency_key": "fire-deploy-key-002"})
    except InventoryShortage as exc:
        shortage = {"code": exc.code, "details": exc.details}

    untouched = {
        "east-normal": service.inventory_lot("east-normal")["available_units"],
        "west-normal": service.inventory_lot("west-normal")["available_units"],
    }
    return {
        "selected_lot_ids": [item["preservation_resource_lot_id"] for item in confirmed["selected_lots"]],
        "deployed_units": confirmed["deployed_units"],
        "created_by": confirmed["created_by"],
        "constraints": confirmed["constraints"],
        "replay_safe": replay.get("replayed") is True and replay["selected_lots"] == confirmed["selected_lots"],
        "incompatible_reason": incompatible,
        "shortage_reason": shortage,
        "untouched_lots": untouched,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行标本事件保藏中心调度服务离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
