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


def send_slack_notification(text: str):
    """
    Optional: Nachricht über einen Slack Incoming Webhook in einen Channel schicken.
    Wird aktuell von /poll-jobs nicht genutzt, bleibt aber als Fallback da.
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("SLACK_WEBHOOK_URL nicht gesetzt – Channel-Notification wird übersprungen.")
        return

    try:
        resp = requests.post(webhook_url, json={"text": text}, timeout=5)
        if resp.status_code >= 400:
            print(f"Slack Webhook Fehler: HTTP {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"Slack Webhook Exception: {e}")


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


@app.route("/slack", methods=["POST"])
def slack_command():
    """
    Empfängt Slack Slash Commands wie: /fetch meta 01.06.-02.06.25
    und triggert einen Adverity-Fetch.
    """
    # Slack sendet Form-Data, nicht JSON
    text = request.form.get("text", "")       # z.B. "meta 01.06.-02.06.25"
    user_name = request.form.get("user_name", "unknown")
    user_id = request.form.get("user_id", "")  # wichtig für spätere DM

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
        "instafollows": "573",
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

        # An Adverity schicken wir wie bei dir erfolgreich verwendet nur das Datum (ohne Zeit).
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
        "triggerUserId": user_id,
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
        # Felder aus dem offiziellen Erfolgsformat und evtl. Fehlern
        status_field = json_body.get("status")
        message_field = json_body.get("message")
        jobs = json_body.get("jobs")
        # generische Fehlerdetails (können auch Warnungen sein)
        detail = (
            json_body.get("detail")
            or json_body.get("error")
            or json_body.get("message")
        )
        # Job-ID aus jobs[] oder alternativem Feld
        if isinstance(jobs, list) and jobs:
            job_id = jobs[0].get("id")
        if not job_id:
            job_id = json_body.get("id") or json_body.get("job_id")
    except ValueError:
        # keine JSON-Response
        detail = response.text

    # Logging in Google Sheet – inkl. kompletter Detailinfo
    log_detail = detail or ""
    if json_body is not None:
        try:
            # komplette API-Response als String (ggf. gekürzt)
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

    # --- Interpretation der Antwort ---

    # 1. Harte Info: Gibt es einen Job in der Response?
    has_job = bool(job_id) or (isinstance(jobs, list) and len(jobs) > 0)

    # 2. Gibt es ein operation_timeout in den Details / JSON?
    op_timeout = False
    txt_for_check = ""
    if detail:
        txt_for_check += str(detail) + " "
    if json_body is not None:
        try:
            txt_for_check += json.dumps(json_body)
        except Exception:
            pass
    if "operation_timeout" in txt_for_check.lower():
        op_timeout = True

    # 3. Fälle:

    # 3a) Job vorhanden -> wir kommunizieren: Job wurde von der API zurückgeliefert.
    #     Wenn zusätzlich operation_timeout gemeldet wird, sagen wir das klar dazu.
    if has_job:
        job_id_text = job_id or "unbekannt"
        base_text = (
            "✅ *Fetch-Job von Adverity bestätigt.*\n"
            f"📊 Stream: {datastream_name}\n"
            f"📅 Zeitraum: {start_date} – {end_date}\n"
            f"🆔 Job ID: {job_id_text}\n"
           # f"<https://{instance}/app/datastreams/{datastream_id}|Zu Adverity>\n"
        )

        if op_timeout:
            base_text += (
                "\n⚠️ Hinweis der Adverity-API: `operation_timeout`.\n"
                "Die API liefert sowohl Job-Informationen als auch diese Meldung zurück. "
                "Bitte den endgültigen Status des Jobs direkt in Adverity prüfen."
            )
        else:
            # Falls z.B. nur eine andere Warnung/Message vorliegt
            if status_code < 200 or status_code >= 300 or status_field not in (None, "ok"):
                base_text += (
                    f"\nℹ️ Die API hat zusätzlich folgende Informationen geliefert "
                    f"(HTTP {status_code}, status={status_field}):\n{detail or '–'}"
                )

        return jsonify(
            {
                "response_type": "in_channel",
                "text": base_text,
            }
        )

    # 3b) Kein Job in der Response -> echter Fehler, egal ob operation_timeout oder was anderes
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


@app.route("/poll-jobs", methods=["GET", "POST"])
def poll_jobs():
    """
    Pollt alle Jobs aus dem Google Sheet, die noch "offen" sind (Status nicht done_*,
    aber mit JobId), holt den Status über die Adverity Jobs API und:
    - aktualisiert den Status im Sheet
    - schickt bei Success/Failed eine Slack-DM an den ursprünglichen Auslöser
    Diese Route kannst du z.B. per Cron (Render Scheduler) alle X Minuten aufrufen.
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

    header = rows[0]
    data_rows = rows[1:]

    checked = 0
    updated = 0

    for idx, row in enumerate(data_rows, start=2):  # row index in Sheet = idx
        # Erwartet mindestens 10 Spalten (inkl. JobId + TriggerUserId)
        if len(row) < 10:
            continue

        timestamp = row[0]
        datastream_id = row[1]
        start = row[2]
        end = row[3]
        instance = row[4] or instance_env
        raw_prompt = row[5]
        status = row[6]
        error_detail = row[7]
        job_id = row[8].strip()
        trigger_user_id = row[9].strip()

        # Nur Zeilen mit JobId und Status nicht schon "done_*"
        if not job_id:
            continue
        if status.startswith("done_"):
            continue

        checked += 1

        # Status für diesen Job bei Adverity abfragen
        try:
            # Jobs-API, z.B. /api/jobs/{JOB_ID}/imported/ für Status/Details
            job_url = f"https://{instance}/api/jobs/{job_id}/imported/"
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

        # Typische Felder: state_label, status etc.
        state_label = job_info.get("state_label") or job_info.get("status")
        # Mögliche Werte: 'Scheduled', 'Running', 'Success', 'Failed', ...
        if not state_label:
            print(f"Job {job_id}: kein state_label/status in Response.")
            continue

        state_label_upper = str(state_label).upper()

        if state_label_upper in ("SUCCESS", "FAILED"):
            # Status im Sheet updaten
            new_status = f"done_{state_label_upper.lower()}"
            try:
                # Spalte 7 = Status, Spalte 8 = ErrorDetail (job_info)
                worksheet.update_cell(idx, 7, new_status)
                worksheet.update_cell(
                    idx,
                    8,
                    json.dumps(job_info)[:1000],
                )
                updated += 1
            except Exception as e:
                print(f"Fehler beim Update des Sheets für Job {job_id}: {e}")

            # Slack-DM an ursprünglichen Auslöser schicken (falls vorhanden)
            state_icon = "✅" if state_label_upper == "SUCCESS" else "❌"
            msg_user = (
                f"{state_icon} *Dein Adverity-Job ist abgeschlossen.*\n"
                f"📊 Datastream ID: {datastream_id}\n"
                f"📅 Zeitraum: {start} – {end}\n"
                f"🆔 Job ID: {job_id}\n"
                f"📈 Status: *{state_label}*\n"
                f"⏱️ Erstellt (Log): {timestamp}\n"
               # f"<https://{instance}/app/datastreams/{datastream_id}|Zu Adverity>"
            )
            if trigger_user_id:
                slack_dm(trigger_user_id, msg_user)
            else:
                # Fallback: ggf. in einen Channel posten (optional)
                send_slack_notification(msg_user)

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
