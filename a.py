import pyodbc
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=drt-kaist-2.database.windows.net,1433;"
    "DATABASE=HDL;"
    "UID=drt-kaist;"
    "PWD=hdl3644@;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)
conn = pyodbc.connect(conn_str, timeout=10)
cursor = conn.cursor()
cursor.execute("SELECT 1")
print(cursor.fetchone())
cursor.close()
conn.close()
