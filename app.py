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
        info.get("datastreamId"),
        info.get("start"),
        info.get("end"),
        info.get("instance"),
        info.get("rawPrompt", "n/a"),
        info.get("status", "n/a"),
        info.get("errorDetail", "n/a"),
    ]

    # Google Sheets API via Umgebungsvariable laden
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
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
    """
    Technischer Endpoint (ohne Slack), bei Bedarf direkt aufrufbar.
    """
    data = request.get_json()

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
        "Content-Type": "application/json",
    }
    body = {"start": start, "end": end}

    try:
        response = requests.post(url, headers=headers, json=body, timeout=30)
    except requests.exceptions.RequestException as e:
        # Netzwerk-/HTTP-Timeout etc.
        log_data = {
            "datastreamId": datastream_id,
            "start": start,
            "end": end,
            "instance": instance,
            "rawPrompt": data.get("rawPrompt", "n/a"),
            "status": "request_exception",
            "errorDetail": str(e),
        }
        try:
            log_to_google_sheet(log_data)
        except Exception as log_err:
            print(f"Log-Fehler: {log_err}")

        return (
            jsonify(
                {
                    "error": "RequestException beim Fetch",
                    "details": str(e),
                }
            ),
            500,
        )

    # Wir haben eine HTTP-Response, egal ob 2xx oder nicht
    status_code = response.status_code
    json_body = None
    detail = None
    job_id = None

    try:
        json_body = response.json()
        detail = (
            json_body.get("detail")
            or json_body.get("error")
            or json_body.get("message")
        )
        job_id = json_body.get("id") or json_body.get("job_id")
    except ValueError:
        # kein JSON
        detail = response.text

    # Logging
    log_data = {
        "datastreamId": datastream_id,
        "start": start,
        "end": end,
        "instance": instance,
        "rawPrompt": data.get("rawPrompt", "n/a"),
        "status": f"http_{status_code}",
        "errorDetail": detail or "n/a",
    }
    try:
        log_to_google_sheet(log_data)
    except Exception as log_err:
        print(f"Log-Fehler: {log_err}")

    # Erfolgsfall
    if 200 <= status_code < 300:
        return jsonify(json_body or {"status": "ok"}), 200

    # Spezieller Fall: operation_timeout – Job läuft trotzdem weiter
    if detail and "operation_timeout" in str(detail).lower():
        msg = {
            "warning": "operation_timeout",
            "info": "Adverity meldet ein Timeout, der Fetch-Job wurde aber sehr wahrscheinlich gestartet.",
            "datastreamId": datastream_id,
            "jobId": job_id,
            "rawResponse": json_body or response.text,
        }
        return jsonify(msg), 202

    # Sonst: echter Fehler
    return (
        jsonify(
            {
                "error": "Fetch fehlgeschlagen",
                "status_code": status_code,
                "details": detail,
            }
        ),
        status_code,
    )


@app.route("/slack", methods=["POST"])
def slack_command():
    """
    Empfängt Slack Slash Commands wie: /fetch meta 01.06.-02.06.25
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

    # Text parsen
    parts = text.strip().split()

    if len(parts) < 2:
        return jsonify(
            {
                "response_type": "ephemeral",
                "text": "❌ Zu wenig Infos. Beispiel: `/fetch meta 01.06.-02.06.25`",
            }
        )

    datastream_name = parts[0]  # z.B. "meta"
    date_range = parts[1]  # z.B. "01.06.-02.06.25"

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

    # Datums-Parsing: "01.06.-02.06.25" -> ISO-Strings
    try:
        date_parts = date_range.split("-")
        if len(date_parts) != 2:
            raise ValueError("Ungültiges Format")

        start_str = date_parts[0].strip()  # "01.06."
        end_str = date_parts[1].strip()  # "02.06.25"

        # Start-Datum parsen (z.B. "01.06.")
        start_day, start_month = start_str.rstrip(".").split(".")

        # End-Datum parsen (z.B. "02.06.25")
        end_parts = end_str.rstrip(".").split(".")
        end_day = end_parts[0]
        end_month = end_parts[1]
        end_year = end_parts[2] if len(end_parts) > 2 else None

        # Jahr ermitteln (wenn nicht angegeben, aktuelles Jahr nehmen)
        if end_year:
            year = f"20{end_year}" if len(end_year) == 2 else end_year
        else:
            year = str(datetime.now().year)

        # ISO-Format (nur Datum)
        start_date = f"{year}-{start_month.zfill(2)}-{start_day.zfill(2)}"
        end_date = f"{year}-{end_month.zfill(2)}-{end_day.zfill(2)}"

        # Adverity erwartet vollständige ISO-Strings mit Uhrzeit und Z-Suffix
        start = f"{start_date}T00:00:00Z"
        end = f"{end_date}T23:59:59Z"

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

    # Log-Basisdaten
    log_base = {
        "datastreamId": datastream_id,
        "start": start,
        "end": end,
        "instance": instance,
        "rawPrompt": f"{user_name}: {text}",
    }

    try:
        response = requests.post(url, headers=headers, json=body, timeout=30)
    except requests.exceptions.RequestException as e:
        # Netzwerkfehler / Timeout etc.
        log_data = {**log_base, "status": "request_exception", "errorDetail": str(e)}
        try:
            log_to_google_sheet(log_data)
        except Exception as log_err:
            print(f"Log-Fehler: {log_err}")

        return jsonify(
            {
                "response_type": "ephemeral",
                "text": f"❌ Fehler beim Fetch (RequestException): {str(e)}",
            }
        )

    status_code = response.status_code
    json_body = None
    detail = None
    job_id = None

    try:
        json_body = response.json()
        detail = (
            json_body.get("detail")
            or json_body.get("error")
            or json_body.get("message")
        )
        job_id = json_body.get("id") or json_body.get("job_id")
    except ValueError:
        detail = response.text

    # Logging
    log_data = {
        **log_base,
        "status": f"http_{status_code}",
        "errorDetail": detail or "n/a",
    }
    try:
        log_to_google_sheet(log_data)
    except Exception as log_err:
        print(f"Log-Fehler: {log_err}")

    # Klarer Erfolgsfall (2xx)
    if 200 <= status_code < 300:
        job_id_text = job_id or "unbekannt"
        return jsonify(
            {
                "response_type": "in_channel",
                "text": (
                    "✅ *Fetch gestartet!*\n"
                    f"📊 Stream: {datastream_name}\n"
                    f"📅 Zeitraum: {start_date} – {end_date}\n"
                    f"🆔 Job ID: {job_id_text}\n"
                    f"<https://{instance}/app/datastreams/{datastream_id}|Zu Adverity>"
                ),
            }
        )

    # Spezieller Fall: Adverity schickt operation_timeout, Job läuft aber trotzdem
    if detail and "operation_timeout" in str(detail).lower():
        job_id_text = job_id or "siehe Adverity UI"
        return jsonify(
            {
                "response_type": "in_channel",
                "text": (
                    "⚠️ *Fetch vermutlich gestartet, aber Adverity meldet `operation_timeout`.*\n"
                    f"📊 Stream: {datastream_name}\n"
                    f"📅 Zeitraum: {start_date} – {end_date}\n"
                    f"🆔 Job ID: {job_id_text}\n"
                    f"ℹ️ Bitte Status und Ergebnis direkt in Adverity prüfen.\n"
                    f"<https://{instance}/app/datastreams/{datastream_id}|Zu Adverity>"
                ),
            }
        )

    # Alle anderen Fehler
    msg = "❌ Fetch fehlgeschlagen."
    if detail:
        msg += f"\nDetails: {detail}"

    return jsonify(
        {
            "response_type": "ephemeral",
            "text": msg,
        }
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
