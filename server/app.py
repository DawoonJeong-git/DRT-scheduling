# server/app.py
import os
from pathlib import Path
import socket
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS

BASE_DIR = Path(__file__).resolve().parent

# backend env 먼저 로드
load_dotenv(BASE_DIR / ".env", override=True)

# 필요 시 루트 env 추가 로드
load_dotenv(BASE_DIR.parent / ".env", override=False)

print("\n=== ENV CHECK ===")
print("PORT =", os.getenv("PORT"))
print("FLASK_PORT =", os.getenv("FLASK_PORT"))
print("CORS_ALLOWED_ORIGINS =", os.getenv("CORS_ALLOWED_ORIGINS"))
print("=================\n")

# env 로드가 끝난 뒤 import
from gantt_builder import build_gantt_payload

app = Flask(__name__)

cors_origins_raw = os.getenv("CORS_ALLOWED_ORIGINS", "*").strip()

if cors_origins_raw == "*" or not cors_origins_raw:
    CORS(app)
else:
    cors_origins = [x.strip() for x in cors_origins_raw.split(",") if x.strip()]
    CORS(app, origins=cors_origins)


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.get("/api/gantt")
def api_gantt():
    date = request.args.get("date", "").strip()
    if not date:
        return jsonify({"error": "Missing required query param: date=YYYY-MM-DD"}), 400

    try:
        payload = build_gantt_payload(date)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", os.getenv("FLASK_PORT", "5056")))
    app.run(host="0.0.0.0", port=port)




@app.route("/api/socket-check")
def socket_check():
    host = "143.248.121.90"
    port = 3306
    try:
        socket.create_connection((host, port), timeout=5)
        return jsonify({
            "ok": True,
            "message": f"TCP reachable: {host}:{port}"
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "message": f"TCP failed: {repr(e)}",
            "host": host,
            "port": port
        }), 500