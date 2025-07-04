import os
import json
from flask import Flask, request, jsonify
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# Google Sheets Setup
SHEET_ID = "1EZlvkKLfTBiYEbCrQIpFTDviXw3JOeSLTyYOCdPEEec"

def log_to_google_sheet(info: dict):
    # Eintrag vorbereiten
    log_entry = [
        datetime.now().isoformat(),
        info.get('datastreamId'),
        info.get('start'),
        info.get('end'),
        info.get('instance'),
        info.get('rawPrompt', 'n/a')
    ]

    # Google Sheets API via Umgebungsvariable laden
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_json = json.loads(os.environ["GOOGLE_CREDS_JSON"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)

    # Einfügen in Google Sheet
    sheet = client.open_by_key(SHEET_ID)
    worksheet = sheet.sheet1
    worksheet.insert_row(log_entry, index=2)

@app.route("/", methods=["GET"])
def index():
    return {"message": "Adverity Fetch API with Google Sheets Logging 🎉"}

@app.route("/start-fetch", methods=["POST"])
def start_fetch():
    data = request.get_json()

    try:
        log_to_google_sheet(data)
    except Exception as log_error:
        print(f"Log-Fehler: {log_error}")

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
    body = {"start": start, "end": end}

    try:
        response = requests.post(url, headers=headers, json=body, timeout=30)
        response.raise_for_status()
        return response.json(), 200
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e), "details": e.response.text if e.response else None}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
