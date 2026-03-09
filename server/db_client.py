# server/db_client.py
import os
from typing import Any

import pymysql
import pyodbc


def _env(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip() != "":
            return value
    return default


DB_ENGINE = (_env("DB_ENGINE", default="azure") or "azure").lower()
# mysql | azure


# ---- MySQL / MariaDB ----
MYSQL_USER = _env("DB_USER", "MYSQL_USER", default="root")
MYSQL_PASS = _env("DB_PASS", "DB_PASSWORD", "MYSQL_PASSWORD", default="")
MYSQL_HOST = _env("DB_HOST", "MYSQL_HOST", default="localhost")
MYSQL_PORT = int(_env("DB_PORT", "MYSQL_PORT", default="3306"))
MYSQL_DB = _env("DB_NAME", "MYSQL_DATABASE", default="hdl")

# ---- Azure SQL ----
AZURE_USER = _env("AZURE_DB_USER", "AZURE_SQL_USER", default="")
AZURE_PASS = _env("AZURE_DB_PASS", "AZURE_DB_PASSWORD", "AZURE_SQL_PASSWORD", default="")
AZURE_SERVER = _env("AZURE_DB_SERVER", "AZURE_SQL_SERVER", default="")
AZURE_PORT = int(_env("AZURE_DB_PORT", "AZURE_SQL_PORT", default="1433"))
AZURE_DB = _env("AZURE_DB_NAME", "AZURE_SQL_DATABASE", default="")
AZURE_DRIVER = _env("AZURE_DB_DRIVER", default="{ODBC Driver 18 for SQL Server}")
AZURE_ENCRYPT = _env("AZURE_DB_ENCRYPT", default="yes")
AZURE_TRUST_SERVER_CERT = _env("AZURE_DB_TRUST_SERVER_CERT", default="no")


def _is_azure() -> bool:
    return DB_ENGINE in {"azure", "sqlserver", "mssql"}


def _require_azure_env() -> None:
    missing = []
    required = {
        "AZURE_DB_USER": AZURE_USER,
        "AZURE_DB_PASS": AZURE_PASS,
        "AZURE_DB_SERVER": AZURE_SERVER,
        "AZURE_DB_NAME": AZURE_DB,
    }
    for key, value in required.items():
        if not value:
            missing.append(key)

    if missing:
        raise RuntimeError(
            "Missing Azure SQL environment variables: " + ", ".join(missing)
        )


def _conn():
    """
    DB_ENGINE=mysql  -> pymysql connection
    DB_ENGINE=azure  -> pyodbc connection
    """
    if _is_azure():
        _require_azure_env()

        print(f"[DB] CONNECT -> AZURE SQL ({AZURE_SERVER}:{AZURE_PORT} / {AZURE_DB})")

        conn_str = (
            f"DRIVER={AZURE_DRIVER};"
            f"SERVER={AZURE_SERVER},{AZURE_PORT};"
            f"DATABASE={AZURE_DB};"
            f"UID={AZURE_USER};"
            f"PWD={AZURE_PASS};"
            f"Encrypt={AZURE_ENCRYPT};"
            f"TrustServerCertificate={AZURE_TRUST_SERVER_CERT};"
        )
        conn = pyodbc.connect(conn_str, timeout=10)
        conn.autocommit = True
        return conn

    print(f"[DB] CONNECT -> MYSQL ({MYSQL_HOST}:{MYSQL_PORT} / {MYSQL_DB})")
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