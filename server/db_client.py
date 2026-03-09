# server/db_client.py
import os
from typing import Any

import pymysql
import pyodbc


DB_ENGINE = os.getenv("DB_ENGINE", "azure").lower()
# mysql | azure

# ---- MariaDB defaults (기존 유지) ----
MYSQL_USER = os.getenv("DB_USER", "root")
MYSQL_PASS = os.getenv("DB_PASS", "3644")
MYSQL_HOST = os.getenv("DB_HOST", "143.248.121.90")
MYSQL_PORT = int(os.getenv("DB_PORT", "3306"))
MYSQL_DB = os.getenv("DB_NAME", "hdl")

# ---- Azure SQL defaults ----
AZURE_USER = os.getenv("AZURE_DB_USER", "drt-kaist@drt-kaist")
AZURE_PASS = os.getenv("AZURE_DB_PASS", "hdl3644@")
AZURE_SERVER = os.getenv("AZURE_DB_SERVER", "drt-kaist.database.windows.net")
AZURE_PORT = int(os.getenv("AZURE_DB_PORT", "1433"))
AZURE_DB = os.getenv("AZURE_DB_NAME", "HDL")
AZURE_DRIVER = os.getenv("AZURE_DB_DRIVER", "{ODBC Driver 18 for SQL Server}")
AZURE_ENCRYPT = os.getenv("AZURE_DB_ENCRYPT", "yes")
AZURE_TRUST_SERVER_CERT = os.getenv("AZURE_DB_TRUST_SERVER_CERT", "no")


def _is_azure() -> bool:
    return DB_ENGINE in {"azure", "sqlserver", "mssql"}


def _conn():
    """
    DB_ENGINE=mysql  -> pymysql connection
    DB_ENGINE=azure  -> pyodbc connection
    """
    if _is_azure():
        print("[DB] CONNECT → AZURE SQL")
        conn_str = (
            f"DRIVER={AZURE_DRIVER};"
            f"SERVER={AZURE_SERVER},{AZURE_PORT};"
            f"DATABASE={AZURE_DB};"
            f"UID={AZURE_USER};"
            f"PWD={AZURE_PASS};"
            f"Encrypt={AZURE_ENCRYPT};"
            f"TrustServerCertificate={AZURE_TRUST_SERVER_CERT};"
        )
        conn = pyodbc.connect(conn_str)
        conn.autocommit = True
        return conn

    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        charset="utf8mb4",
        use_unicode=True,
    )


def _fetchall(sql: str, params: list[Any] | tuple[Any, ...] | None = None):
    """
    MariaDB / Azure 공통 fetchall -> list[dict]
    """
    params = params or []

    with _conn() as c:
        cur = c.cursor()
        try:
            cur.execute(sql, params)

            if _is_azure():
                columns = [col[0] for col in cur.description]
                rows = cur.fetchall()
                return [dict(zip(columns, row)) for row in rows]

            return cur.fetchall()
        finally:
            cur.close()


def _normalize_dispatch_id(x) -> str:
    """
    Defensive normalize:
    - strips whitespace
    - repeatedly removes wrapping single/double quotes
    """
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
    return "?" if _is_azure() else "%s"


def _placeholders(n: int) -> str:
    return ",".join([_placeholder()] * n)


def _cast_int(expr: str) -> str:
    """
    MariaDB: CAST(x AS UNSIGNED)
    Azure  : TRY_CAST(x AS BIGINT)
    """
    if _is_azure():
        return f"TRY_CAST({expr} AS BIGINT)"
    return f"CAST({expr} AS UNSIGNED)"


def _qualify(schema: str, table: str, alias: str | None = None) -> str:
    """
    MariaDB: hdl.route r
    Azure  : [dbo].[route] r

    주의:
    - 기존 호출부가 schema='hdl' 를 넘기더라도,
      Azure SQL에서는 보통 schema가 dbo 이므로 자동 보정.
    """
    alias_sql = f" {alias}" if alias else ""

    if _is_azure():
        sql_schema = schema
        if not sql_schema or sql_schema.lower() in {"hdl", "azure_hdl"}:
            sql_schema = "dbo"
        return f"[{sql_schema}].[{table}]{alias_sql}"

    return f"{schema}.{table}{alias_sql}"


def get_operations_catalog(schema: str):
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


def get_routes_for_day(schema: str, date_yyyymmdd: int):
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


def get_reservations_by_dispatch_ids(schema: str, dispatch_ids: list[str]):
    """
    reservation_request에는 stationName이 없음.
    passengerCount / wheelchairCount 등을 위해 유지.
    """
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


def get_dispatches_by_dispatch_ids(schema: str, dispatch_ids: list[str]):
    """
    dispatch 테이블의 pickup/dropoff station 정보를 가져옴.
    """
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
      d.pickupStationName,
      d.dropoffStationName,
      d.pickupStationID,
      d.dropoffStationID,
      d.reserveType
    FROM {dispatch_tbl}
    WHERE d.dispatchID IN ({placeholders})
    """
    return _fetchall(sql, cleaned)