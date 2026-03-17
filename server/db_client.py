from dotenv import load_dotenv
import os
import pymysql
from contextlib import contextmanager
from typing import Any

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_NAME", "hdl")

print(f"[DB INIT] host={DB_HOST}:{DB_PORT} db={DB_DATABASE}")

required_env = {
    "DB_HOST": DB_HOST,
    "DB_USER": DB_USER,
    "DB_PASSWORD": DB_PASS,
    "DB_NAME": DB_DATABASE,
}
missing = [k for k, v in required_env.items() if not v]
if missing:
    raise RuntimeError(f"Missing required DB env vars: {', '.join(missing)}")


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_DATABASE,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        charset="utf8mb4",
    )


@contextmanager
def connect():
    print(f"[DB CONNECT] host={DB_HOST}:{DB_PORT} db={DB_DATABASE}")
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


def _fetchall(sql: str, params: list[Any] | tuple[Any, ...] | None = None):
    params = params or []
    with connect() as c:
        cur = c.cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            cur.close()


def test_connection():
    with connect() as c:
        cur = c.cursor()
        try:
            cur.execute("SELECT 1 AS ok")
            row = cur.fetchone()
            return bool(row and row.get("ok") == 1)
        finally:
            cur.close()


def _normalize_dispatch_id(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    changed = True
    while changed and len(s) >= 2:
        changed = False
        if (s[0] == "'" and s[-1] == "'") or (s[0] == '"' and s[-1] == '"'):
            s = s[1:-1].strip()
            changed = True
    return s


def _placeholder() -> str:
    return "%s"


def _placeholders(n: int) -> str:
    return ",".join([_placeholder()] * n)


def _cast_int(expr: str) -> str:
    return f"CAST({expr} AS UNSIGNED)"


def _qualify(table: str, alias: str | None = None) -> str:
    alias_sql = f" {alias}" if alias else ""
    return f"{DB_DATABASE}.{table}{alias_sql}"


def get_operations_catalog():
    operation_tbl = _qualify("operation", "o")

    sql = f"""
    SELECT
      o.operationID,
      o.vehicleID,
      o.VehicleType AS vehicleType,
      o.operationServiceType
    FROM {operation_tbl}
    """
    return _fetchall(sql)


def get_routes_for_day(date_yyyymmdd: int):
    start = int(f"{date_yyyymmdd}0000")
    end = int(f"{date_yyyymmdd}2359")

    route_tbl = _qualify("route", "r")
    operation_tbl = _qualify("operation", "o")

    cast_origin = _cast_int("r.originDeptTime")
    cast_dest = _cast_int("r.destArrivalTime")

    sql = f"""
    SELECT
      r.routeID,
      r.routeSeq,
      r.operationID,
      r.vehicleID,
      {cast_origin} AS originDeptTime,
      {cast_dest} AS destArrivalTime,
      r.dispatchIDs,
      o.VehicleType AS vehicleType,
      o.operationServiceType,
      o.vehicleID AS op_vehicleID
    FROM {route_tbl}
    JOIN {operation_tbl}
      ON o.operationID = r.operationID
     AND o.vehicleID   = r.vehicleID
    WHERE {cast_origin} BETWEEN {_placeholder()} AND {_placeholder()}
    ORDER BY r.operationID, r.routeSeq
    """
    return _fetchall(sql, [start, end])


def get_reservations_by_dispatch_ids(dispatch_ids: list[str]):
    if not dispatch_ids:
        return []

    cleaned = []
    seen = set()
    for x in dispatch_ids:
        nd = _normalize_dispatch_id(x)
        if not nd or nd in seen:
            continue
        seen.add(nd)
        cleaned.append(nd)

    if not cleaned:
        return []

    reservation_tbl = _qualify("reservation_request", "rr")
    placeholders = _placeholders(len(cleaned))

    sql = f"""
    SELECT
      rr.dispatchID,
      rr.passengerCount,
      rr.wheelchairCount,
      rr.reserveType,
      rr.pickupStationID,
      rr.dropoffStationID
    FROM {reservation_tbl}
    WHERE rr.dispatchID IN ({placeholders})
    """
    return _fetchall(sql, cleaned)


def get_dispatches_by_dispatch_ids(dispatch_ids: list[str]):
    if not dispatch_ids:
        return []

    cleaned = []
    seen = set()
    for x in dispatch_ids:
        nd = _normalize_dispatch_id(x)
        if not nd or nd in seen:
            continue
        seen.add(nd)
        cleaned.append(nd)

    if not cleaned:
        return []

    dispatch_tbl = _qualify("dispatch", "d")
    placeholders = _placeholders(len(cleaned))

    sql = f"""
    SELECT
      d.dispatchID,
      d.pickupStationName,
      d.dropoffStationName,
      d.pickupStationID,
      d.dropoffStationID,
      d.reserveType
    FROM {dispatch_tbl}
    WHERE d.dispatchID IN ({placeholders})
    """
    return _fetchall(sql, cleaned)