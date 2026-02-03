import os
import time
import json
from datetime import datetime
from threading import Thread

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- Datastream Mapping ---
DATASTREAM_MAP = {
    "meta": "674",
    "google": "678",
    "snapchat": "679",
    "tiktok": "675",
    "instafollows": "573",
}

ID_TO_NAME_MAP = {v: k for k, v in DATASTREAM_MAP.items()}

# --- Polling Intervall (5 Minuten) ---
POLL_INTERVAL_SECONDS = 5 * 60

# --- Terminal states (Adverity) ---
TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED", "DISCARDED"}


def slack_post_ephemeral(channel_id: str, user_id: str, text: str):
    """Antwort nur an den User im Channel (ephemeral)."""
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        print("SLACK_BOT_TOKEN fehlt – kann keine Slack Nachricht senden.")
        return

    url = "https://slack.com/api/chat.postEphemeral"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "channel": channel_id,
        "user": user_id,
        "text": text,
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=8)
        if resp.status_code >= 400:
            print(f"Slack Ephemeral HTTP Fehler: {resp.status_code} - {resp.text}")
            return
        body = resp.json()
        if not body.get("ok"):
            print(f"Slack Ephemeral API Fehler: {body}")
    except Exception as e:
        print(f"Slack Ephemeral Exception: {e}")


def parse_date_range(date_range: str):
    """
    Erwartet: DD.MM.-DD.MM.YY
    Beispiel: 09.11.-09.11.25
    Return: (YYYY-MM-DD, YYYY-MM-DD)
    """
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


def adverity_start_fetch(datastream_id: str, start_date: str, end_date: str):
    """
    Startet den Fetch in Adverity.
    Erwartet, dass die API eine JobId zurückliefert.
    """
    instance = os.environ.get("ADVERITY_INSTANCE")
    token = os.environ.get("ADVERITY_TOKEN")
    if not instance or not token:
        raise RuntimeError("ADVERITY_INSTANCE oder ADVERITY_TOKEN fehlt")

    url = f"https://{instance}/api/datastreams/{datastream_id}/fetch_fixed/"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"start": start_date, "end": end_date}

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    # Wir werten hier NICHT "success" als Job-fertig, sondern nur: JobId vorhanden?
    data = {}
    try:
        data = resp.json()
    except ValueError:
        pass

    job_id = ""
    jobs = data.get("jobs")
    if isinstance(jobs, list) and jobs:
        job_id = jobs[0].get("id") or ""

    if not job_id:
        job_id = data.get("job_id") or data.get("id") or ""

    if not job_id:
        # Keine JobId -> als echter Fehler behandeln, inkl. Debug-Info
        detail = data or resp.text
        raise RuntimeError(f"Keine JobId von Adverity erhalten. HTTP {resp.status_code}. Detail: {detail}")

    return job_id


def adverity_get_job_state(job_id: str):
    """Holt den Status eines Jobs aus Adverity."""
    instance = os.environ.get("ADVERITY_INSTANCE")
    token = os.environ.get("ADVERITY_TOKEN")
    if not instance or not token:
        raise RuntimeError("ADVERITY_INSTANCE oder ADVERITY_TOKEN fehlt")

    url = f"https://{instance}/api/jobs/{job_id}/"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    state_label = data.get("state_label") or data.get("status") or ""
    return state_label, data


def poll_job_until_done(job_id: str, stream_name: str, start_date: str, end_date: str, channel_id: str, user_id: str):
    """
    Pollt alle 5 Minuten bis der Job in einem terminal state ist.
    Dann ephemere Antwort an den ursprünglichen User.
    """
    while True:
        try:
            state_label, raw = adverity_get_job_state(job_id)
            state_up = str(state_label).upper()

            if state_up in TERMINAL_STATES:
                icon = "✅" if state_up == "SUCCESS" else "❌"
                msg = (
                    f"{icon} Dein Adverity-Job ist abgeschlossen.\n"
                    f"• Stream: {stream_name}\n"
                    f"• Zeitraum: {start_date} – {end_date}\n"
                    f"• Job ID: {job_id}\n"
                    f"• Status: {state_label}"
                )
                slack_post_ephemeral(channel_id, user_id, msg)
                return

        except Exception as e:
            # Pollingfehler: wir schicken nicht sofort an User, nur Log.
            print(f"Polling Fehler (Job {job_id}): {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


@app.route("/", methods=["GET"])
def health():
    return {"ok": True, "service": "adverity-fetcher"}


@app.route("/slack", methods=["POST"])
def slack_fetch():
    """
    Slack Slash Command: /fetch <stream> <DD.MM.-DD.MM.YY>
    Beispiel: /fetch instafollows 09.11.-09.11.25
    """
    text = request.form.get("text", "").strip()
    user_id = request.form.get("user_id", "")
    channel_id = request.form.get("channel_id", "")

    if not text:
        return jsonify({
            "response_type": "ephemeral",
            "text": "Bitte nutze: `/fetch <stream> <DD.MM.-DD.MM.YY>`"
        })

    parts = text.split()
    if len(parts) < 2:
        return jsonify({
            "response_type": "ephemeral",
            "text": "Zu wenig Parameter. Beispiel: `/fetch instafollows 09.11.-09.11.25`"
        })

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

    # 1) Fetch starten
    try:
        datastream_id = DATASTREAM_MAP[stream]
        job_id = adverity_start_fetch(datastream_id, start_date, end_date)
    except Exception as e:
        return jsonify({
            "response_type": "ephemeral",
            "text": f"Fehler beim Starten des Fetch: {e}"
        })

    # 2) Polling im Hintergrund starten (alle 5 Minuten)
    t = Thread(
        target=poll_job_until_done,
        args=(job_id, stream, start_date, end_date, channel_id, user_id),
        daemon=True,
    )
    t.start()

    # 3) Sofortige Antwort an Slack (damit kein Timeout)
    return jsonify({
        "response_type": "ephemeral",
        "text": (
            f"⏳ Fetch für *{stream}* ({start_date} – {end_date}) gestartet.\n"
            f"Ich prüfe den Job alle 5 Minuten und melde mich, sobald er fertig ist."
        )
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
