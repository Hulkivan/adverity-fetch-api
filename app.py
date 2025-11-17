# VOLLSTÄNDIGER, FINALER CODE FÜR DIESEN ANSATZ

import os
import json
from flask import Flask, request, jsonify, Response
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import threading

app = Flask(__name__)

# --- Konfiguration & Secrets ---
SHEET_ID = "1EZlvkKLfTBiYEbCrQIpFTDviXw3JOeSLTyYOCdPEEec"
ADVERITY_INSTANCE = os.environ.get("ADVERITY_INSTANCE")
ADVERITY_TOKEN = os.environ.get("ADVERITY_TOKEN")

def log_to_google_sheet(info: dict):
    # ... (unverändert) ...

def execute_and_poll(datastream_id, datastream_name, start, end, date_range, response_url):
    """Startet den Fetch, pollt den Status und sendet die finale Antwort."""
    
    # 1. Adverity-Job starten (ohne Callback)
    url = f"https://{ADVERITY_INSTANCE}/api/datastreams/{datastream_id}/fetch_fixed/"
    headers = {"Authorization": f"Bearer {ADVERITY_TOKEN}", "Content-Type": "application/json"}
    body = {"start": start, "end": end}
    job_id = None

    try:
        response = requests.post(url, headers=headers, json=body, timeout=30)
        response.raise_for_status()
        # Job-ID aus der Antwort extrahieren
        data = response.json()
        if "jobs" in data and data["jobs"]:
            job_id = data["jobs"][0].get("id")
        elif "id" in data:
            job_id = data.get("id")
        
        if not job_id:
            raise ValueError("Konnte Job-ID nicht aus Adverity-Antwort extrahieren.")

    except Exception as e:
        error_msg = {"response_type": "ephemeral", "text": f"❌ Fehler beim Starten des Adverity-Jobs: {e}"}
        requests.post(response_url, json=error_msg)
        return

    # 2. Polling des Job-Status
    status_url = f"https://{ADVERITY_INSTANCE}/api/jobs/{job_id}/"
    max_wait_time = 28 * 60  # 28 Minuten (Sicherheitsabstand zu 30 Min. Timeout der response_url)
    start_time = time.time()

    while time.time() - start_time < max_wait_time:
        try:
            res = requests.get(status_url, headers=headers, timeout=15).json()
            status = res.get("status", "unknown").lower()
            if status not in ["pending", "running", "scheduled"]:
                adverity_link = f"<https://{ADVERITY_INSTANCE}/jobs/{job_id}|Zu Adverity>"
                if status in ["completed", "successful", "finished"]:
                    final_text = f"✅ *Fetch erfolgreich!* (Abgeschlossen)\n📊 Stream: {datastream_name}\n📅 Zeitraum: {date_range}\n{adverity_link}"
                else:
                    final_text = f"❌ *Fetch fehlgeschlagen!*\n📊 Stream: {datastream_name}\n📉 Status: `{status}`\n{adverity_link}"
                
                final_msg = {"response_type": "in_channel", "text": final_text}
                requests.post(response_url, json=final_msg)
                return
        except Exception as e:
            print(f"Polling-Fehler für Job {job_id}: {e}")
        time.sleep(45)

    # 3. Timeout-Nachricht
    timeout_msg = {"response_type": "in_channel", "text": f"⌛️ *Fetch-Überwachung Zeitüberschreitung* für Stream *{datastream_name}*.\nDer Job `{job_id}` läuft vermutlich noch. Bitte manuell prüfen: <https://{ADVERITY_INSTANCE}/jobs/{job_id}|Zu Adverity>"}
    requests.post(response_url, json=timeout_msg)


@app.route("/slack", methods=["POST"])
def slack_command():
    # ... (Der gesamte Parsing-Code bleibt hier exakt gleich) ...
    # ANNAHME: datastream_id, datastream_name, start, end, date_range werden hier extrahiert.
    
    response_url = request.form.get('response_url')
    user_name = request.form.get('user_name', 'unknown')
    text = request.form.get('text', '')

    # Starte den gesamten Prozess im Hintergrund
    thread = threading.Thread(
        target=execute_and_poll,
        args=(datastream_id, datastream_name, start, end, date_range, response_url)
    )
    thread.start()
    
    # Logge den Startvorgang
    log_to_google_sheet({
        "datastreamId": datastream_id, "start": start, "end": end,
        "instance": ADVERITY_INSTANCE, "rawPrompt": f"{user_name}: {text}"
    })

    # Sende eine sofortige, private Bestätigung an den Nutzer
    return jsonify({
        "response_type": "ephemeral",
        "text": f"⏳ Anfrage für *{datastream_name}* ({date_range}) angenommen. Der Job wird gestartet und überwacht. Du erhältst eine öffentliche Nachricht bei Abschluss."
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
