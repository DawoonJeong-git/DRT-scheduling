# server/db_client.py

import os
import pymysql
from typing import Any

# --------------------------------------------------
# DB profile selection
# --------------------------------------------------

PROFILE_NAME = "hdl"   # "nzero" or "hdl"

# --------------------------------------------------
# Load config: local(db_config.py) first, then Render env fallback
# --------------------------------------------------

DB = None

try:
    from db_config import DB_CONFIGS  # local only

    if PROFILE_NAME in DB_CONFIGS:
        DB = DB_CONFIGS[PROFILE_NAME]
        print(f"[DB INIT] Using local db_config.py | PROFILE={PROFILE_NAME}")
    else:
        raise KeyError(f"PROFILE_NAME '{PROFILE_NAME}' not found in DB_CONFIGS")

except Exception as e:
    print(f"[DB INIT] Local db_config.py unavailable or invalid: {e}")
    print(f"[DB INIT] Falling back to environment variables | PROFILE={PROFILE_NAME}")

    # Render / deployment env fallback
    if PROFILE_NAME == "nzero":
        DB = {
            "host": os.getenv("NZERO_DB_HOST"),
            "port": int(os.getenv("NZERO_DB_PORT", "3306")),
            "user": os.getenv("NZERO_DB_USER"),
            "password": os.getenv("NZERO_DB_PASSWORD"),
            "database": os.getenv("NZERO_DB_NAME"),
            "charset": "utf8mb4",
            "use_unicode": True,
        }

    elif PROFILE_NAME == "hdl":
        DB = {
            "host": os.getenv("HDL_DB_HOST"),
            "port": int(os.getenv("HDL_DB_PORT", "3306")),
            "user": os.getenv("HDL_DB_USER"),
            "password": os.getenv("HDL_DB_PASSWORD"),
            "database": os.getenv("HDL_DB_NAME"),
            "charset": "utf8mb4",
            "use_unicode": True,
        }

    else:
        raise ValueError(f"Unsupported PROFILE_NAME: {PROFILE_NAME}")

# --------------------------------------------------
# Final DB vars
# --------------------------------------------------

DB_HOST = DB["host"]
DB_PORT = DB["port"]
DB_USER = DB["user"]
DB_PASS = DB["password"]
DB_DATABASE = DB["database"]
DB_SCHEMA = DB_DATABASE

if not all([DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_DATABASE]):
    raise ValueError(
        f"[DB INIT ERROR] Missing DB config values for PROFILE={PROFILE_NAME}. "
        f"Check db_config.py or Render environment variables."
    )

print(f"[DB INIT] PROFILE={PROFILE_NAME} | host={DB_HOST}:{DB_PORT} | db={DB_DATABASE}")


# --------------------------------------------------
# connection / fetch
# --------------------------------------------------

def _conn():
    print(f"[DB] CONNECT -> MARIADB ({DB_HOST}:{DB_PORT} / {DB_DATABASE})")
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_DATABASE,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        charset=DB["charset"],
        use_unicode=DB["use_unicode"],
    )


def _fetchall(sql: str, params: list[Any] | tuple[Any, ...] | None = None):
    params = params or []

    with _conn() as c:
        cur = c.cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            cur.close()


# --------------------------------------------------
# helpers
# --------------------------------------------------

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


def _qualify(schema: str, table: str, alias: str | None = None) -> str:
    alias_sql = f" {alias}" if alias else ""
    return f"{schema}.{table}{alias_sql}"


# --------------------------------------------------
# queries
# --------------------------------------------------

def get_operations_catalog(schema: str | None = None):
    schema = schema or DB_SCHEMA
    operation_tbl = _qualify(schema, "operation", "o")

    sql = f"""
    SELECT
      o.operationID,
      o.vehicleID,
      o.VehicleType AS vehicleType,
      o.operationServiceType
    FROM {operation_tbl}
    """
    return _fetchall(sql)


def get_routes_for_day(schema: str | None, date_yyyymmdd: int):
    schema = schema or DB_SCHEMA
    start = int(f"{date_yyyymmdd}0000")
    end = int(f"{date_yyyymmdd}2359")

    route_tbl = _qualify(schema, "route", "r")
    operation_tbl = _qualify(schema, "operation", "o")

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


def get_reservations_by_dispatch_ids(schema: str | None, dispatch_ids: list[str]):
    schema = schema or DB_SCHEMA
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

    reservation_tbl = _qualify(schema, "reservation_request", "rr")
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


def get_dispatches_by_dispatch_ids(schema: str | None, dispatch_ids: list[str]):
    schema = schema or DB_SCHEMA
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

    dispatch_tbl = _qualify(schema, "dispatch", "d")
    placeholders = _placeholders(len(cleaned))

    sql = f"""
    SELECT
      d.dispatchID,
      d.pickupStationID,
      d.pickupStationName,
      d.dropoffStationID,
      d.dropoffStationName,
      d.reserveType
    FROM {dispatch_tbl}
    WHERE d.dispatchID IN ({placeholders})
    """
    return _fetchall(sql, cleaned)