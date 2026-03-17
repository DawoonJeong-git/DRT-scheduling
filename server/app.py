from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import os

from .gantt_builder import build_gantt_payload
from .db_client import test_connection

load_dotenv()

app = Flask(__name__)
CORS(app)


@app.get("/health")
@app.get("/api/health")
def health():
    try:
        db_ok = test_connection()
        return jsonify({"ok": True, "db": db_ok})
    except Exception as e:
        return jsonify({"ok": False, "db": False, "error": str(e)}), 500


@app.get("/api/gantt")
def api_gantt():
    date = request.args.get("date", "").strip()
    if not date:
        return jsonify({"error": "Missing required query param: date=YYYY-MM-DD"}), 400

    payload = build_gantt_payload(date)
    return jsonify(payload)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5056"))
    app.run(host="0.0.0.0", port=port, debug=False)