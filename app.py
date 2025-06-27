import os
from flask import Flask, request, jsonify
import requests
from datetime import datetime

app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    return {"message": "Adverity Fetch API ready 🎉"}

# 🔧 Logging-Funktion (nicht als Route!)
def log_fetch(info: dict):
    log_entry = (
        f"{datetime.now().isoformat()} | "
        f"Datastream: {info.get('datastreamId')} | "
        f"{info.get('start')} → {info.get('end')} | "
        f"From: {info.get('instance')} | "
        f"Prompt: {info.get('rawPrompt', 'n/a')}\n"
    )
    with open("fetch_log.txt", "a") as f:
        f.write(log_entry)

# 🚀 Haupt-Endpoint für den Fetch
@app.route("/start-fetch", methods=["POST"])
def start_fetch():
    data = request.get_json()
    log_fetch(data)

    instance = data.get("instance")
    token = data.get("token")
    auth_type = data.get("authType", "Bearer")
    datastream_id = data.get("datastreamId")
    start = data.get("start")
    end = data.get("end")

    if not all([instance, token, datastream_id, start, end]):
        return jsonify({"error": "Fehlende Parameter"}), 400

    url = f"https://{instance}/api/datastreams/{datastream_id}/fetch_fixed/"
    headers = {
        "Authorization": f"{auth_type} {token}",
        "Content-Type": "application/json"
    }
    body = {
        "start": start,
        "end": end
    }

    try:
        response = requests.post(url, headers=headers, json=body, timeout=30)
        response.raise_for_status()
        return response.json(), 200
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e), "details": e.response.text if e.response else None}), 500

# 📄 Route zur Anzeige des Logfiles im Browser
@app.route("/log", methods=["GET"])
def show_log():
    try:
        with open("fetch_log.txt", "r") as f:
            content = f.read()
        return f"<pre>{content}</pre>", 200
    except FileNotFoundError:
        return "Noch keine Logeinträge vorhanden.", 200

# 🔁 Server starten (lokal oder auf Render)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
