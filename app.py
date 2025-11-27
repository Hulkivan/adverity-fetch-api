import os
import json
from datetime import datetime
from threading import Thread

import requests
from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# Google Sheets Setup
SHEET_ID = "1EZlvkKLfTBiYEbCrQIpFTDviXw3JOeSLTyYOCdPEEec"


def get_gsheet_worksheet():
    """
    Stellt eine Verbindung zum Google Sheet her und gibt worksheet (sheet1) zurück.
    Wird von Logging und Polling genutzt.
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_json = json.loads(os.environ["GOOGLE_CREDS_JSON"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(SHEET_ID)
    worksheet = sheet.sheet1
    return worksheet


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
    - JobId
    - TriggerUserId
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
        info.get("jobId", ""),
        info.get("triggerUserId", ""),
    ]

    worksheet = get_gsheet_worksheet()
    # Neue Einträge immer in Zeile 2 einfügen (unter der evtl. Headerzeile)
    worksheet.insert_row(log_entry, index=2)


def slack_dm(user_id: str, text: str):
    """
    Schickt eine DM an einen Slack-User über die Slack Web API (chat.postMessage).
    Voraussetzung: SLACK_BOT_TOKEN ist gesetzt und die App hat chat:write-Rechte.
    """
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        print("SLACK_BOT_TOKEN nicht gesetzt – DM wird übersprungen.")
        return
    if not user_id:
        print("user_id fehlt – DM wird übersprungen.")
        return

    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    data = {
        "channel": user_id,  # bei DMs ist channel = user_id
        "text": text,
    }

    try:
        resp = requests.post(url, headers=headers, json=data, timeout=5)
        if resp.status_code >= 400:
            print(f"Slack DM HTTP Fehler: {resp.status_code} - {resp.text}")
            return
        try:
            body = resp.json()
            if not body.get("ok", False):
                print(f"Slack DM API Fehler: {body}")
        except ValueError:
            print(f"Slack DM: keine gültige JSON-Antwort: {resp.text}")
    except Exception as e:
        print(f"Slack DM Exception: {e}")


@app.route("/", methods=["GET"])
def index():
    return {"message": "Adverity Fetch API with Google Sheets Logging & Job Polling 🎉"}


# Mapping Datastream-Name -> ID
DATASTREAM_MAP = {
    "meta": "674",
    "google": "678",
    "snapchat": "679",
    "tiktok": "675",
    "instafollows": "573",
}

# Inverses Mapping ID -> Name (für Polling / DMs)
ID_TO_NAME_MAP = {v: k for k, v in DATASTREAM_MAP.items()}


# --------------------------
# Hintergrund-Job für Adverity
# --------------------------

def run_adverity_fetch_async(
    datastream_id: str,
    datastream_name: str,
    start_date: str,
    end_date: str,
    instance: str,
    token: str,
    user_name: str,
    user_id: str,
    raw_text: str,
):
    """
    Läuft in einem separaten Thread.
    Macht den Adverity-Fetch und Logging, ohne den Slack-Request zu blockieren.
    """
    start = start_date
    end = end_date

    log_base = {
        "datastreamId": datastream_id,
        "start": start,
        "end": end,
        "instance": instance,
        "rawPrompt": f"{user_name}: {raw_text}",
        "triggerUserId": user_id,
    }

    url = f"https://{instance}/api/datastreams/{datastream_id}/fetch_fixed/"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    body = {"start": start, "end": end}

    try:
        response = requests.post(url, headers=headers, json=body, timeout=30)
    except requests.exceptions.RequestException as e:
        log_data = {**log_base, "status": "request_exception", "errorDetail": str(e), "jobId": ""}
        try:
            log_to_google_sheet(log_data)
        except Exception as log_err:
            print(f"Log-Fehler beim Logging (RequestException): {log_err}")
        return

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
        detail = (
            json_body.get("detail")
            or json_body.get("error")
            or json_body.get("message")
        )
        if isinstance(jobs, list) and jobs:
            job_id = jobs[0].get("id")
        if not job_id:
            job_id = json_body.get("id") or json_body.get("job_id")
    except ValueError:
        detail = response.text

    log_detail = detail or ""
    if json_body is not None:
        try:
            log_detail = json.dumps(json_body)[:1000]
        except Exception:
            pass

    log_data = {
        **log_base,
        "status": f"http_{status_code}",
        "errorDetail": log_detail or "n/a",
        "jobId": job_id or "",
    }
    try:
        log_to_google_sheet(log_data)
    except Exception as log_err:
        print(f"Log-Fehler beim Logging: {log_err}")

    # Keine weitere Slack-Nachricht hier – Benachrichtigung kommt über /poll-jobs,
    # wenn der Job wirklich fertig ist.


# -------------
# Slack-Command
# -------------

@app.route("/slack", methods=["POST"])
def slack_command():
    """
    Empfängt Slack Slash Commands wie: /fetch meta 01.06.-02.06.25
    und triggert einen Adverity-Fetch asynchron.
    """
    text = request.form.get("text", "")
    user_name = request.form.get("user_name", "unknown")
    user_id = request.form.get("user_id", "")

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

    datastream_name = parts[0]
    date_range = parts[1]

    datastream_id = DATASTREAM_MAP.get(datastream_name.lower())
    if not datastream_id:
        available = ", ".join(DATASTREAM_MAP.keys())
        return jsonify(
            {
                "response_type": "ephemeral",
                "text": f"❌ Datastream '{datastream_name}' nicht gefunden.\nVerfügbar: {available}",
            }
        )

    # Datums-Parsing
    try:
        date_parts = date_range.split("-")
        if len(date_parts) != 2:
            raise ValueError("Ungültiges Format")

        start_str = date_parts[0].strip()  # "01.06."
        end_str = date_parts[1].strip()    # "02.06.25"

        start_day, start_month = start_str.rstrip(".").split(".")

        end_parts = end_str.rstrip(".").split(".")
        end_day = end_parts[0]
        end_month = end_parts[1]
        end_year = end_parts[2] if len(end_parts) > 2 else None

        if end_year:
            year = f"20{end_year}" if len(end_year) == 2 else end_year
        else:
            year = str(datetime.now().year)

        start_date = f"{year}-{start_month.zfill(2)}-{start_day.zfill(2)}"
        end_date = f"{year}-{end_month.zfill(2)}-{end_day.zfill(2)}"

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

    instance = os.environ.get("ADVERITY_INSTANCE")
    token = os.environ.get("ADVERITY_TOKEN")

    if not instance or not token:
        return jsonify(
            {
                "response_type": "ephemeral",
                "text": "❌ Server-Konfigurationsfehler (Credentials fehlen)",
            }
        )

    # Adverity-Call NICHT synchron ausführen, sondern Thread starten:
    t = Thread(
        target=run_adverity_fetch_async,
        args=(
            datastream_id,
            datastream_name,
            start_date,
            end_date,
            instance,
            token,
            user_name,
            user_id,
            text,
        ),
        daemon=True,
    )
    t.start()

    # Sofortige Antwort (unter 3 Sekunden) → kein Slack-Timeout
    return jsonify(
        {
            "response_type": "ephemeral",
            "text": (
                f"⏳ Dein Fetch für *{datastream_name}* "
                f"({start_date} – {end_date}) wird jetzt gestartet.\n"
                "Du bekommst eine DM, sobald der Job abgeschlossen ist."
            ),
        }
    )


# -------------
# Polling-Route
# -------------

@app.route("/poll-jobs", methods=["GET", "POST"])
def poll_jobs():
    """
    Pollt alle Jobs aus dem Google Sheet, die noch "offen" sind (Status nicht done_*,
    aber mit JobId), holt den Status über die Adverity Jobs API und:
    - aktualisiert den Status im Sheet
    - schickt bei Success/Failed/Cancelled/Discarded eine Slack-DM an den ursprünglichen Auslöser
    """
    instance_env = os.environ.get("ADVERITY_INSTANCE")
    token = os.environ.get("ADVERITY_TOKEN")

    if not instance_env or not token:
        return jsonify(
            {
                "message": "ADVERITY_INSTANCE oder ADVERITY_TOKEN fehlen. Polling abgebrochen."
            }
        ), 500

    worksheet = get_gsheet_worksheet()
    rows = worksheet.get_all_values()

    if not rows or len(rows) < 2:
        return jsonify({"message": "Keine Log-Einträge gefunden."})

    data_rows = rows[1:]

    checked = 0
    updated = 0

    for idx, row in enumerate(data_rows, start=2):
        # Erwartet mindestens 10 Spalten (inkl. JobId + TriggerUserId)
        if len(row) < 10:
            continue

        timestamp = row[0]
        datastream_id = row[1]
        start = row[2]
        end = row[3]
        instance = row[4] or instance_env
        status = row[6]
        job_id = row[8].strip()
        trigger_user_id = row[9].strip()

        # Nur Zeilen mit JobId und Status nicht schon "done_*"
        if not job_id:
            continue
        if status.startswith("done_"):
            continue

        checked += 1

        try:
            # WICHTIG: Direktes Jobs-Objekt abfragen, nicht /imported/,
            # damit wir sicher an state_label kommen.
            job_url = f"https://{instance}/api/jobs/{job_id}/"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            resp = requests.get(job_url, headers=headers, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"Fehler beim Abfragen von Job {job_id}: {e}")
            continue

        if resp.status_code >= 400:
            print(
                f"Adverity Jobs API Fehler für Job {job_id}: HTTP {resp.status_code} - {resp.text}"
            )
            continue

        try:
            job_info = resp.json()
        except ValueError:
            print(f"Keine JSON-Response für Job {job_id}: {resp.text}")
            continue

        state_label = job_info.get("state_label") or job_info.get("status")
        if not state_label:
            print(f"Job {job_id}: kein state_label/status in Response.")
            continue

        state_label_upper = str(state_label).upper()

        # Terminale Zustände: alles, was nicht mehr läuft
        TERMINAL_STATES = ("SUCCESS", "FAILED", "CANCELLED", "DISCARDED")

        if state_label_upper in TERMINAL_STATES:
            suffix = state_label_upper.lower()
            new_status = f"done_{suffix}"
            try:
                worksheet.update_cell(idx, 7, new_status)  # Status
                worksheet.update_cell(
                    idx,
                    8,
                    json.dumps(job_info)[:1000],  # ErrorDetail / Jobinfo
                )
                updated += 1
            except Exception as e:
                print(f"Fehler beim Update des Sheets für Job {job_id}: {e}")

            state_icon = "✅" if state_label_upper == "SUCCESS" else "❌"

            # Datastream-Namen für die DM rekonstruieren (falls möglich)
            stream_name = ID_TO_NAME_MAP.get(datastream_id, datastream_id)

            msg_user = (
                f"{state_icon} *Dein Adverity-Job ist abgeschlossen.*\n"
                f"📊 Stream: {stream_name}\n"
                f"📅 Zeitraum: {start} – {end}\n"
                f"🆔 Job ID: {job_id}\n"
                f"📈 Status: *{state_label}*\n"
                f"⏱️ Erstellt (Log): {timestamp}"
            )

            if trigger_user_id:
                slack_dm(trigger_user_id, msg_user)

    return jsonify(
        {
            "message": "Polling abgeschlossen.",
            "checked_rows": checked,
            "updated_rows": updated,
        }
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
