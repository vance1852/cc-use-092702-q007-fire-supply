"""标本事件快处服务的 SQLite 模式和事务辅助。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS traffic_users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('planner','dispatcher','risk','auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_index_risk_records (
    risk_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    risk_index TEXT NOT NULL,
    duty_date TEXT NOT NULL,
    index_value TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    supersedes_risk_record_id INTEGER REFERENCES risk_index_risk_records(risk_record_id),
    recorded_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    recorded_at TEXT NOT NULL,
    UNIQUE(risk_index, duty_date, source_revision)
);

CREATE INDEX IF NOT EXISTS idx_risk_records_series
ON risk_index_risk_records(risk_index, duty_date, risk_record_id);

CREATE TABLE IF NOT EXISTS response_centers (
    center_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    timezone TEXT NOT NULL,
    capacity_units TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS road_corridors (
    corridor_id TEXT PRIMARY KEY,
    origin_center_id TEXT NOT NULL REFERENCES response_centers(center_id),
    destination_center_id TEXT NOT NULL REFERENCES response_centers(center_id),
    preservation_resource_kind TEXT NOT NULL,
    required_grade TEXT,
    hourly_capacity TEXT NOT NULL,
    delay_basis_points INTEGER NOT NULL,
    response_minutes INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','suspended','retired')),
    created_at TEXT NOT NULL,
    CHECK(origin_center_id <> destination_center_id)
);

CREATE TABLE IF NOT EXISTS corridor_restrictions (
    restriction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    corridor_id TEXT NOT NULL REFERENCES road_corridors(corridor_id),
    starts_at TEXT NOT NULL,
    ends_at TEXT,
    capacity_percent TEXT NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'announced' CHECK(state IN ('announced','active','closed','cancelled')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outages_route_time
ON corridor_restrictions(corridor_id, starts_at, ends_at);

CREATE TABLE IF NOT EXISTS preservation_resource_lots (
    preservation_resource_lot_id TEXT PRIMARY KEY,
    center_id TEXT NOT NULL REFERENCES response_centers(center_id),
    preservation_resource_kind TEXT NOT NULL,
    grade TEXT NOT NULL,
    quantity_units TEXT NOT NULL,
    available_units TEXT NOT NULL,
    unit_cost_cny TEXT NOT NULL,
    received_at TEXT NOT NULL,
    expires_at TEXT,
    lot_status TEXT NOT NULL DEFAULT 'available' CHECK(lot_status IN ('available','frozen','depleted','expired')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_inventory_available
ON preservation_resource_lots(center_id, preservation_resource_kind, received_at);

CREATE INDEX IF NOT EXISTS idx_inventory_candidates
ON preservation_resource_lots(center_id, preservation_resource_kind, lot_status, expires_at);

CREATE TABLE IF NOT EXISTS preservation_resource_adjustments (
    adjustment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    preservation_resource_lot_id TEXT NOT NULL REFERENCES preservation_resource_lots(preservation_resource_lot_id),
    delta_units TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    note TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dispatch_requests (
    dispatch_id TEXT PRIMARY KEY,
    corridor_id TEXT NOT NULL REFERENCES road_corridors(corridor_id),
    specimen_event_id TEXT NOT NULL,
    duty_date TEXT NOT NULL,
    requested_units TEXT NOT NULL,
    allocated_units TEXT NOT NULL DEFAULT '0',
    arrived_units TEXT NOT NULL DEFAULT '0',
    priority INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'submitted'
        CHECK(state IN ('submitted','allocated','in_transit','delivered','cancelled')),
    revision INTEGER NOT NULL DEFAULT 1,
    idempotency_key TEXT NOT NULL UNIQUE,
    submitted_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    submitted_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dispatch_requests_schedule
ON dispatch_requests(corridor_id, duty_date, priority, submitted_at);

CREATE TABLE IF NOT EXISTS dispatch_plans (
    plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
    corridor_id TEXT NOT NULL REFERENCES road_corridors(corridor_id),
    duty_date TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    available_units TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE(corridor_id, duty_date, input_sha256)
);

CREATE TABLE IF NOT EXISTS deployments (
    deployment_id TEXT PRIMARY KEY,
    dispatch_id TEXT NOT NULL UNIQUE REFERENCES dispatch_requests(dispatch_id),
    deployed_units TEXT NOT NULL,
    expected_arrived_units TEXT NOT NULL,
    departed_at TEXT NOT NULL,
    arrived_at TEXT,
    state TEXT NOT NULL DEFAULT 'in_transit' CHECK(state IN ('in_transit','delivered','disputed')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS deployment_lot_items (
    deployment_id TEXT NOT NULL REFERENCES deployments(deployment_id),
    preservation_resource_lot_id TEXT NOT NULL REFERENCES preservation_resource_lots(preservation_resource_lot_id),
    allocated_units TEXT NOT NULL,
    PRIMARY KEY(deployment_id, preservation_resource_lot_id)
);

CREATE INDEX IF NOT EXISTS idx_deployment_lot_items_lot
ON deployment_lot_items(preservation_resource_lot_id);

CREATE TABLE IF NOT EXISTS response_scenarios (
    scenario_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'draft' CHECK(state IN ('draft','approved','retired')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS response_scenario_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario_id TEXT NOT NULL REFERENCES response_scenarios(scenario_id),
    as_of_date TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE(scenario_id, as_of_date, input_sha256)
);

CREATE TABLE IF NOT EXISTS traffic_idempotency (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, idempotency_key)
);

CREATE TABLE IF NOT EXISTS traffic_audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_traffic_audit_entity
ON traffic_audit_events(entity_type, entity_id, event_id);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(path), isolation_level=None, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    initialize(connection)
    return connection


def initialize(connection: sqlite3.Connection) -> None:
    _prepare_legacy_columns(connection)
    connection.executescript(SCHEMA)
    _migrate_legacy_deployments(connection)


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _column_names(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}


def _prepare_legacy_columns(connection: sqlite3.Connection) -> None:
    """在执行新 SCHEMA 前，为早期数据库补齐有效期、可用状态和任务兼容等级列。

    必须先补列，SCHEMA 中引用这些列的新索引才能在旧表上创建成功。
    """
    if _table_exists(connection, "preservation_resource_lots"):
        lot_columns = _column_names(connection, "preservation_resource_lots")
        if "expires_at" not in lot_columns:
            connection.execute("ALTER TABLE preservation_resource_lots ADD COLUMN expires_at TEXT")
        if "lot_status" not in lot_columns:
            connection.execute(
                "ALTER TABLE preservation_resource_lots ADD COLUMN lot_status TEXT NOT NULL DEFAULT 'available'"
            )
    if _table_exists(connection, "road_corridors"):
        route_columns = _column_names(connection, "road_corridors")
        if "required_grade" not in route_columns:
            connection.execute("ALTER TABLE road_corridors ADD COLUMN required_grade TEXT")


def _migrate_legacy_deployments(connection: sqlite3.Connection) -> None:
    """把带人工点名列的旧出库单迁移到批次明细，并重建为不含该列的新结构。"""
    if not _table_exists(connection, "deployments"):
        return
    deployment_columns = _column_names(connection, "deployments")
    if "inventory_preservation_resource_lot_id" not in deployment_columns:
        return
    connection.execute(
        "INSERT INTO deployment_lot_items(deployment_id,preservation_resource_lot_id,allocated_units) "
        "SELECT deployment_id,inventory_preservation_resource_lot_id,deployed_units FROM deployments "
        "WHERE inventory_preservation_resource_lot_id IS NOT NULL"
    )
    # deployment_lot_items 仍引用 deployments，重建父表期间需临时关闭外键强制。
    foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]
    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        connection.executescript(
            """
            CREATE TABLE deployments_v2 (
                deployment_id TEXT PRIMARY KEY,
                dispatch_id TEXT NOT NULL UNIQUE REFERENCES dispatch_requests(dispatch_id),
                deployed_units TEXT NOT NULL,
                expected_arrived_units TEXT NOT NULL,
                departed_at TEXT NOT NULL,
                arrived_at TEXT,
                state TEXT NOT NULL DEFAULT 'in_transit' CHECK(state IN ('in_transit','delivered','disputed')),
                revision INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
                created_at TEXT NOT NULL
            );
            INSERT INTO deployments_v2(deployment_id,dispatch_id,deployed_units,expected_arrived_units,
                departed_at,arrived_at,state,revision,created_by,created_at)
            SELECT deployment_id,dispatch_id,deployed_units,expected_arrived_units,departed_at,arrived_at,
                state,revision,created_by,created_at FROM deployments;
            DROP TABLE deployments;
            ALTER TABLE deployments_v2 RENAME TO deployments;
            """
        )
    finally:
        connection.execute(f"PRAGMA foreign_keys={'ON' if foreign_keys else 'OFF'}")


@contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def row_dict(row: sqlite3.Row | None) -> dict[str, object] | None:
    return None if row is None else dict(row)
