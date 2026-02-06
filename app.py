import os
import json
from datetime import datetime, timezone
from threading import Thread

import requests
from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# -----------------------------
# Configuration
# -----------------------------
SHEET_ID = os.environ.get("SHEET_ID", "").strip()

DATASTREAM_MAP = {
    "meta": "674",
    "google": "678",
    "snapchat": "679",
    "tiktok": "675",
    "instafollows": "573",
}
ID_TO_NAME_MAP = {v: k for k, v in DATASTREAM_MAP.items()}

TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED", "DISCARDED"}


# -----------------------------
# Time helpers
# -----------------------------
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# -----------------------------
# Google Sheets helpers
# -----------------------------
def get_gsheet_worksheet():
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID fehlt (bitte als ENV setzen).")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_json = json.loads(os.environ["GOOGLE_CREDS_JSON"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID)
    return sheet.sheet1


def ensure_header():
    """
    Header (Row 1) expected:
    Timestamp | Stream | DatastreamId | Start | End | Instance | RawPrompt | Status | ErrorDetail | JobId | TriggerUserId | TriggerChannelId | NotifiedAt
    """
    ws = get_gsheet_worksheet()
    rows = ws.get_all_values()
    if not rows:
        ws.append_row([
            "Timestamp",
            "Stream",
            "DatastreamId",
            "Start",
            "End",
            "Instance",
            "RawPrompt",
            "Status",
            "ErrorDetail",
            "JobId",
            "TriggerUserId",
            "TriggerChannelId",
            "NotifiedAt",
        ])
        return


def log_job_row(info: dict) -> int:
    ws = get_gsheet_worksheet()
    row = [
        info.get("timestamp", _utc_now_iso()),
        info.get("stream", ""),
        info.get("datastream_id", ""),
        info.get("start", ""),
        info.get("end", ""),
        info.get("instance", ""),
        info.get("raw_prompt", ""),
        info.get("status", ""),
        info.get("error_detail", ""),
        info.get("job_id", ""),
        info.get("trigger_user_id", ""),
        info.get("trigger_channel_id", ""),
        info.get("notified_at", ""),
    ]
    ws.insert_row(row, index=2)
    return 2


# -----------------------------
# Slack helpers
# -----------------------------
def slack_post_ephemeral(channel_id: str, user_id: str, text: str):
    """
    Returns: (ok: bool, error: str|None)
    """
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        return False, "SLACK_BOT_TOKEN missing"

    url = "https://slack.com/api/chat.postEphemeral"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"channel": channel_id, "user": user_id, "text": text}

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=8)
        if resp.status_code >= 400:
            return False, f"http_{resp.status_code}: {resp.text}"
        body = resp.json()
        if not body.get("ok"):
            return False, body.get("error", "unknown_error")
        return True, None
    except Exception as e:
        return False, str(e)


def slack_dm(user_id: str, text: str):
    """
    Fallback DM. Returns (ok, error)
    """
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        return False, "SLACK_BOT_TOKEN missing"

    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"channel": user_id, "text": text}

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=8)
        if resp.status_code >= 400:
            return False, f"http_{resp.status_code}: {resp.text}"
        body = resp.json()
        if not body.get("ok"):
            return False, body.get("error", "unknown_error")
        return True, None
    except Exception as e:
        return False, str(e)


def notify_user(trigger_channel_id: str, trigger_user_id: str, text: str):
    """
    Try ephemeral in the original channel.
    If that fails (bot not in channel etc.), fallback to DM.
    Returns (ok, method, error)
    """
    ok, err = slack_post_ephemeral(trigger_channel_id, trigger_user_id, text)
    if ok:
        return True, "ephemeral", None

    # fallback DM
    ok2, err2 = slack_dm(trigger_user_id, text)
    if ok2:
        return True, "dm_fallback", None

    # both failed
    return False, "failed", f"ephemeral_error={err}; dm_error={err2}"


# -----------------------------
# Parsing
# -----------------------------
def parse_date_range(date_range: str):
    parts = date_range.split("-")
    if len(parts) != 2:
        raise ValueError("Ungültiges Datumsformat (erwartet DD.MM.-DD.MM.YY)")

    start_str = parts[0].strip()  # "09.11."
    end_str = parts[1].strip()    # "09.11.25"

    start_day, start_month = start_str.rstrip(".").split(".")
    end_parts = end_str.rstrip(".").split(".")
    end_day, end_month = end_parts[0], end_parts[1]
    end_year = end_parts[2] if len(end_parts) > 2 else None

    if not end_year:
        year = str(datetime.now().year)
    else:
        year = f"20{end_year}" if len(end_year) == 2 else end_year

    start_date = f"{year}-{start_month.zfill(2)}-{start_day.zfill(2)}"
    end_date = f"{year}-{end_month.zfill(2)}-{end_day.zfill(2)}"
    return start_date, end_date


# -----------------------------
# Adverity
# -----------------------------
def adverity_start_fetch(datastream_id: str, start_date: str, end_date: str):
    instance = os.environ.get("ADVERITY_INSTANCE")
    token = os.environ.get("ADVERITY_TOKEN")
    if not instance or not token:
        raise RuntimeError("ADVERITY_INSTANCE oder ADVERITY_TOKEN fehlt")

    url = f"https://{instance}/api/datastreams/{datastream_id}/fetch_fixed/"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"start": start_date, "end": end_date}

    resp = requests.post(url, headers=headers, json=payload, timeout=30)

    try:
        data = resp.json()
        detail = json.dumps(data)[:1000]
    except ValueError:
        data = None
        detail = (resp.text or "")[:1000]

    job_id = ""
    if isinstance(data, dict):
        jobs = data.get("jobs")
        if isinstance(jobs, list) and jobs:
            job_id = jobs[0].get("id") or ""
        if not job_id:
            job_id = data.get("job_id") or data.get("id") or ""

    if not job_id:
        raise RuntimeError(f"Keine JobId von Adverity erhalten. HTTP {resp.status_code}. Detail: {detail}")

    return job_id, detail, resp.status_code


def adverity_get_job_state(job_id: str):
    instance = os.environ.get("ADVERITY_INSTANCE")
    token = os.environ.get("ADVERITY_TOKEN")
    if not instance or not token:
        raise RuntimeError("ADVERITY_INSTANCE oder ADVERITY_TOKEN fehlt")

    url = f"https://{instance}/api/jobs/{job_id}/"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    state_label = data.get("state_label") or data.get("status") or ""
    return state_label, data


# -----------------------------
# Background start to avoid Slack timeout
# -----------------------------
def start_fetch_async(stream: str, datastream_id: str, start_date: str, end_date: str,
                     trigger_user_id: str, trigger_channel_id: str, raw_prompt: str):
    try:
        ensure_header()
    except Exception as e:
        print(f"Sheet header check failed: {e}")

    instance = os.environ.get("ADVERITY_INSTANCE", "")

    try:
        job_id, detail, http_code = adverity_start_fetch(datastream_id, start_date, end_date)
    except Exception as e:
        # Log failure
        try:
            log_job_row({
                "timestamp": _utc_now_iso(),
                "stream": stream,
                "datastream_id": datastream_id,
                "start": start_date,
                "end": end_date,
                "instance": instance,
                "raw_prompt": raw_prompt,
                "status": "start_failed",
                "error_detail": str(e)[:1000],
                "job_id": "",
                "trigger_user_id": trigger_user_id,
                "trigger_channel_id": trigger_channel_id,
                "notified_at": "",
            })
        except Exception as log_err:
            print(f"Logging failed (start_failed): {log_err}")

        # Notify user about start failure (best-effort)
        notify_user(trigger_channel_id, trigger_user_id, f"❌ Fetch konnte nicht gestartet werden: {e}")
        return

    # Log running
    try:
        log_job_row({
            "timestamp": _utc_now_iso(),
            "stream": stream,
            "datastream_id": datastream_id,
            "start": start_date,
            "end": end_date,
            "instance": instance,
            "raw_prompt": raw_prompt,
            "status": "running",
            "error_detail": f"http_{http_code} {detail}",
            "job_id": job_id,
            "trigger_user_id": trigger_user_id,
            "trigger_channel_id": trigger_channel_id,
            "notified_at": "",
        })
    except Exception as log_err:
        print(f"Logging failed (running): {log_err}")

    # Optional: user feedback that fetch started (not completion)
    notify_user(
        trigger_channel_id,
        trigger_user_id,
        f"⏳ Fetch gestartet (Stream: {stream}, Zeitraum: {start_date} – {end_date}). "
        f"Abschlussmeldung kommt automatisch."
    )


# -----------------------------
# Routes
# -----------------------------
@app.route("/", methods=["GET"])
def health():
    return {"ok": True, "service": "adverity-fetcher"}


@app.route("/slack", methods=["POST"])
def slack_fetch():
    text = (request.form.get("text") or "").strip()
    trigger_user_id = request.form.get("user_id", "")
    trigger_channel_id = request.form.get("channel_id", "")
    user_name = request.form.get("user_name", "unknown")

    if not text:
        return jsonify({"response_type": "ephemeral", "text": "Bitte nutze: `/fetch <stream> <DD.MM.-DD.MM.YY>`"})

    parts = text.split()
    if len(parts) < 2:
        return jsonify({"response_type": "ephemeral", "text": "Beispiel: `/fetch instafollows 09.11.-09.11.25`"})

    stream = parts[0].lower()
    date_range = parts[1]

    if stream not in DATASTREAM_MAP:
        return jsonify({
            "response_type": "ephemeral",
            "text": f"Unbekannter Stream '{stream}'. Verfügbar: {', '.join(DATASTREAM_MAP.keys())}"
        })

    try:
        start_date, end_date = parse_date_range(date_range)
    except Exception as e:
        return jsonify({
            "response_type": "ephemeral",
            "text": f"Datumsformat ungültig: {e}\nNutze: DD.MM.-DD.MM.YY (z. B. 09.11.-09.11.25)"
        })

    datastream_id = DATASTREAM_MAP[stream]
    raw_prompt = f"{user_name}: /fetch {text}"

    # ACK immediately (avoid Slack timeout)
    Thread(
        target=start_fetch_async,
        args=(stream, datastream_id, start_date, end_date, trigger_user_id, trigger_channel_id, raw_prompt),
        daemon=True
    ).start()

    return jsonify({"response_type": "ephemeral", "text": "✅ Anfrage angenommen. Ich starte den Fetch im Hintergrund."})


@app.route("/poll-jobs", methods=["GET", "POST"])
def poll_jobs():
    """
    Call every 5 minutes via external cron:
      GET https://adverity-fetch.onrender.com/poll-jobs

    NEW behavior:
    - Also notifies rows that are already done_* but NotifiedAt is empty (retry / catch-up)
    - NotifiedAt is set ONLY when Slack notification succeeded
    """
    try:
        ws = get_gsheet_worksheet()
    except Exception as e:
        return jsonify({"message": f"Sheet init error: {e}", "checked_rows": 0, "updated_rows": 0}), 500

    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        return jsonify({"message": "Keine Log-Einträge gefunden.", "checked_rows": 0, "updated_rows": 0})

    checked = 0
    updated = 0

    data_rows = rows[1:]

    for sheet_row_idx, row in enumerate(data_rows, start=2):
        if len(row) < 13:
            continue

        timestamp = (row[0] or "").strip()
        stream = (row[1] or "").strip()
        datastream_id = (row[2] or "").strip()
        start_date = (row[3] or "").strip()
        end_date = (row[4] or "").strip()
        status = (row[7] or "").strip()         # Status
        job_id = (row[9] or "").strip()         # JobId
        trigger_user_id = (row[10] or "").strip()
        trigger_channel_id = (row[11] or "").strip()
        notified_at = (row[12] or "").strip()

        # We only care if not notified yet
        if notified_at:
            continue

        # Case A: already done_* but no notification -> send now (catch-up)
        if status.startswith("done_"):
            checked += 1
            # derive final state from status string
            final_state = status.replace("done_", "").upper()
            icon = "✅" if final_state == "SUCCESS" else "❌"
            use_stream = stream or ID_TO_NAME_MAP.get(datastream_id, datastream_id) or "unknown"

            msg = (
                f"{icon} Dein Adverity-Job ist abgeschlossen.\n"
                f"• Stream: {use_stream}\n"
                f"• Zeitraum: {start_date} – {end_date}\n"
                f"• Job ID: {job_id}\n"
                f"• Status: {final_state.title()}"
            )

            ok, method, err = notify_user(trigger_channel_id, trigger_user_id, msg)
            if ok:
                ws.update_cell(sheet_row_idx, 13, _utc_now_iso())  # NotifiedAt
                updated += 1
            else:
                # keep NotifiedAt empty so we retry next cron run
                ws.update_cell(sheet_row_idx, 9, f"notify_error: {err}"[:1000])
            continue

        # Case B: running -> check Adverity state
        if status != "running" or not job_id:
            continue

        checked += 1

        try:
            state_label, raw = adverity_get_job_state(job_id)
            state_up = str(state_label).upper()
        except Exception as e:
            ws.update_cell(sheet_row_idx, 9, f"poll_error: {str(e)[:900]}")
            continue

        if state_up not in TERMINAL_STATES:
            continue

        done_status = f"done_{state_up.lower()}"

        # Update status + details (but DO NOT set NotifiedAt yet)
        try:
            ws.update_cell(sheet_row_idx, 8, done_status)
            ws.update_cell(sheet_row_idx, 9, json.dumps(raw)[:1000])
        except Exception as e:
            print(f"Sheet update failed for row {sheet_row_idx}: {e}")

        icon = "✅" if state_up == "SUCCESS" else "❌"
        use_stream = stream or ID_TO_NAME_MAP.get(datastream_id, datastream_id) or "unknown"

        msg = (
            f"{icon} Dein Adverity-Job ist abgeschlossen.\n"
            f"• Stream: {use_stream}\n"
            f"• Zeitraum: {start_date} – {end_date}\n"
            f"• Job ID: {job_id}\n"
            f"• Status: {state_label}"
        )

        ok, method, err = notify_user(trigger_channel_id, trigger_user_id, msg)
        if ok:
            ws.update_cell(sheet_row_idx, 13, _utc_now_iso())  # NotifiedAt only on success
            updated += 1
        else:
            # keep NotifiedAt empty so we retry next cron run
            ws.update_cell(sheet_row_idx, 9, f"notify_error: {err}"[:1000])

    return jsonify({"message": "Polling abgeschlossen.", "checked_rows": checked, "updated_rows": updated})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
