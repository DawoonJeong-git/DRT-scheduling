# server/app.py
import os

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS

from gantt_builder import build_gantt_payload

load_dotenv()

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