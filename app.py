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

    datastream_id = DATASTREAM_MAP.get(datastream_name.lower())
    if not datastream_id:
        available = ", ".join(DATASTREAM_MAP.keys())
        return jsonify(
            {
                "response_type": "ephemeral",
                "text": f"❌ Datastream '{datastream_name}' nicht gefunden.\nVerfügbar: {available}",
            }
        )

    # Datums-Parsing: "01.06.-02.06.25" -> "YYYY-MM-DD"
    try:
        date_parts = date_range.split("-")
        if len(date_parts) != 2:
            raise ValueError("Ungültiges Format")

        start_str = date_parts[0].strip()  # "01.06."
        end_str = date_parts[1].strip()    # "02.06.25"

        # Start-Datum: "01.06."
        start_day, start_month = start_str.rstrip(".").split(".")

        # End-Datum: "02.06.25"
        end_parts = end_str.rstrip(".").split(".")
        end_day = end_parts[0]
        end_month = end_parts[1]
        end_year = end_parts[2] if len(end_parts) > 2 else None

        # Jahr ableiten
        if end_year:
            year = f"20{end_year}" if len(end_year) == 2 else end_year
        else:
            year = str(datetime.now().year)

        start_date = f"{year}-{start_month.zfill(2)}-{start_day.zfill(2)}"
        end_date = f"{year}-{end_month.zfill(2)}-{end_day.zfill(2)}"

        # An Adverity schicken wir wie ursprünglich nur das Datum (ohne Zeit),
        # weil damit bei dir der Job zuverlässig gestartet wurde.
        start = start_date
        end = end_date

    except Exception as parse_error:
        return jsonify(
            {
                "response_type": "ephemeral",
                "text": (
                    f"❌ Datumsformat ungültig: {str(parse_error)}\n"
                    "Nutze: DD.MM.-DD.MM.YY (z.B. 01.06.-02.06.25)"
                ),
            }
        )

    # Credentials aus Umgebungsvariablen
    instance = os.environ.get("ADVERITY_INSTANCE")
    token = os.environ.get("ADVERITY_TOKEN")

    if not instance or not token:
        return jsonify(
            {
                "response_type": "ephemeral",
                "text": "❌ Server-Konfigurationsfehler (Credentials fehlen)",
            }
        )

    url = f"https://{instance}/api/datastreams/{datastream_id}/fetch_fixed/"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    body = {"start": start, "end": end}

    log_base = {
        "datastreamId": datastream_id,
        "start": start,
        "end": end,
        "instance": instance,
        "rawPrompt": f"{user_name}: {text}",
    }

    # --- Request an Adverity ---
    try:
        response = requests.post(url, headers=headers, json=body, timeout=30)
    except requests.exceptions.RequestException as e:
        # Netzwerkfehler / HTTP-Timeout etc.
        log_data = {**log_base, "status": "request_exception", "errorDetail": str(e)}
        try:
            log_to_google_sheet(log_data)
        except Exception as log_err:
            print(f"Log-Fehler beim Logging: {log_err}")

        return jsonify(
            {
                "response_type": "ephemeral",
                "text": f"❌ Fehler beim Fetch (RequestException): {str(e)}",
            }
        )

    status_code = response.status_code
    json_body = None
    detail = None
    status_field = None
    message_field = None
    job_id = None
    jobs = None

    try:
        json_body = response.json()
        status_field = json_body.get("status")
        message_field = json_body.get("message")
        jobs = json_body.get("jobs")
        # generische Fehlerdetails
        detail = (
            json_body.get("detail")
            or json_body.get("error")
            or json_body.get("message")
        )
        # Job-ID, falls vorhanden
        if isinstance(jobs, list) and jobs:
            # Laut Doku hat der Job Einträge mit id/url
            job_id = jobs[0].get("id")
    except ValueError:
        # keine JSON-Response
        detail = response.text

    # Logging in Google Sheet
    log_data = {
        **log_base,
        "status": f"http_{status_code}",
        "errorDetail": detail or "n/a",
    }
    try:
        log_to_google_sheet(log_data)
    except Exception as log_err:
        print(f"Log-Fehler beim Logging: {log_err}")

    # --- Variante 1: Nur "gestartet", wenn Response exakt dem Erfolgsfall entspricht ---

    is_http_ok = 200 <= status_code < 300
    is_adverity_ok = (
        status_field == "ok"
        and message_field == "Fetch scheduled."
        and isinstance(jobs, list)
        and len(jobs) > 0
    )

    if is_http_ok and is_adverity_ok:
        job_id_text = job_id or "unbekannt"
        return jsonify(
            {
                "response_type": "in_channel",
                "text": (
                    "✅ *Fetch gestartet (laut Adverity-API).* \n"
                    f"📊 Stream: {datastream_name}\n"
                    f"📅 Zeitraum: {start_date} – {end_date}\n"
                    f"🆔 Job ID: {job_id_text}\n"
                    f"<https://{instance}/app/datastreams/{datastream_id}|Zu Adverity>"
                ),
            }
        )

    # Alle anderen Fälle – inkl. operation_timeout – sind echte Fehler/Hinweise.
    # Keine Spekulation, ob ein Job „vielleicht“ trotzdem läuft.
    error_msg_lines = [
        "❌ Adverity-Fehler beim Fetch.",
        f"HTTP-Statuscode: {status_code}",
    ]

    if status_field is not None:
        error_msg_lines.append(f"API-Status: {status_field}")
    if message_field:
        error_msg_lines.append(f"API-Message: {message_field}")
    if detail:
        error_msg_lines.append(f"Details: {detail}")

    error_msg_lines.append(
        f"Bitte prüfe den Datastream direkt in Adverity: "
        f"https://{instance}/app/datastreams/{datastream_id}"
    )

    return jsonify(
        {
            "response_type": "ephemeral",
            "text": "\n".join(error_msg_lines),
        }
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
