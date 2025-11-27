import os
import json
from datetime import datetime

import requests
from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# Google Sheets Setup
SHEET_ID = "1EZlvkKLfTBiYEbCrQIpFTDviXw3JOeSLTyYOCdPEEec"


def log_to_google_sheet(info: dict):
    """
    Loggt Basisinfos in ein Google Sheet.
    Spalten:
    - Timestamp
    - Datastream ID
    - Start
    - End
    - Instance
    - Raw Prompt
    - Status
    - Error Detail
    """
    log_entry = [
        datetime.now().isoformat(),
        info.get("datastreamId"),
        info.get("start"),
        info.get("end"),
        info.get("instance"),
        info.get("rawPrompt", "n/a"),
        info.get("status", "n/a"),
        info.get("errorDetail", "n/a"),
    ]

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_json = json.loads(os.environ["GOOGLE_CREDS_JSON"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(SHEET_ID)
    worksheet = sheet.sheet1
    worksheet.insert_row(log_entry, index=2)


@app.route("/", methods=["GET"])
def index():
    return {"message": "Adverity Fetch API with Google Sheets Logging 🎉"}


@app.route("/slack", methods=["POST"])
def slack_command():
    """
    Empfängt Slack Slash Commands wie: /fetch meta 01.06.-02.06.25
    und triggert einen Adverity-Fetch.
    """
    # Slack sendet Form-Data, nicht JSON
    text = request.form.get("text", "")  # z.B. "meta 01.06.-02.06.25"
    user_name = request.form.get("user_name", "unknown")

    if not text:
        return jsonify(
            {
                "response_type": "ephemeral",
                "text": "❌ Bitte Format nutzen: `/fetch datastream-name DD.MM.-DD.MM.YY`",
            }
        )

    parts = text.strip().split()
    if len(parts) < 2:
        return jsonify(
            {
                "response_type": "ephemeral",
                "text": "❌ Zu wenig Infos. Beispiel: `/fetch meta 01.06.-02.06.25`",
            }
        )

    datastream_name = parts[0]  # z.B. "meta"
    date_range = parts[1]       # z.B. "01.06.-02.06.25"

    # Datastream-Mapping (case-insensitive)
    DATASTREAM_MAP = {
        "meta": "674",
        "google": "678",
        "snapchat": "679",
        "tiktok": "675",
    }

    datastream_id = DATASTREAM_MAP.get(datastream_name.l_
