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
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")


# --- Hilfsfunktionen ---

def log_to_google_sheet(info: dict):
    # Diese Funktion bleibt unverändert
    try:
        if not GOOGLE_CREDS_JSON: return
        log_entry = [datetime.now().isoformat(), info.get('datastreamId'), info.get('start'), info.get('end'), info.get('instance'), info.get('rawPrompt', 'n/a')]
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_json_dict = json.loads(GOOGLE_CREDS_JSON)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json_dict, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        sheet.insert_row(log_entry, index=2)
    except Exception as e:
        print(f"LOGGING-FEHLER (Google Sheets): {e}")

def execute_and_poll(datastream_id, datastream_name, start, end, date_range, response_url, user_name, text):
    """Führt alle langsamen Operationen im Hintergrund aus und loggt jeden Schritt."""
    
    print(f"THREAD-START: Für {datastream_name} durch {user_name} gestartet.")
    
    log_to_google_sheet({"datastreamId": datastream_id, "start": start, "end": end, "instance": ADVERITY_INSTANCE, "rawPrompt": f"{user_name}: {text}"})
    
    url = f"https://{ADVERITY_INSTANCE}/api/datastreams/{datastream_id}/fetch_fixed/"
    headers = {"Authorization": f"Bearer {ADVERITY_TOKEN}", "Content-Type": "application/json"}
    body = {"start": start, "end": end}
    job_id = None

    try:
        print(f"ADVERITY-START-REQUEST: Sende POST an {url}")
        response = requests.post(url, headers=headers, json=body, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        print(f"ADVERITY-START-RESPONSE: {data}") # NEU: Logge die Antwort vom Job-Start
        
        job_id = (data.get("jobs", [{}])[0].get("id") if "jobs" in data and data["jobs"] else data.get("id"))
        
        if not job_id:
            raise ValueError("Struktur der Antwort unklar, Job-ID nicht gefunden.")
            
        print(f"EXTRACTED-JOB-ID: {job_id}") # NEU: Logge die extrahierte Job-ID

    except Exception as e:
        error_text = f"❌ Fehler beim Starten des Adverity-Jobs: {e}"
        print(f"FATAL-ERROR (Job Start): {error_text}") # NEU: Logge den fatalen Fehler
        requests.post(response_url, json={"response_type": "ephemeral", "text": error_text})
        return

    status_url = f"https://{ADVERITY_INSTANCE}/api/jobs/{job_id}/"
    adverity_link = f"<https://{ADVERITY_INSTANCE}/jobs/{job_id}|Zu Adverity>"
    max_wait_time = 28 * 60
    start_time = time.time()
    
    print(f"POLLING-START: Beginne Abfrage für Job {job_id} an URL {status_url}")

    while time.time() - start_time < max_wait_time:
        try:
            res = requests.get(status_url, headers=headers, timeout=15).json()
            print(f"POLLING-RESPONSE (Job {job_id}): {res}") # NEU: Logge JEDE Polling-Antwort
            
            status = res.get("status", "unknown").lower()
            if status not in ["pending", "running", "scheduled"]:
                print(f"POLLING-END: Job {job_id} hat finalen Status '{status}'. Sende Slack-Nachricht.")
                if status in ["completed", "successful", "finished"]:
                    final_text = f"✅ *Fetch erfolgreich!* (Abgeschlossen)\n📊 Stream: {datastream_name}\n📅 Zeitraum: {date_range}\n{adverity_link}"
                else:
                    final_text = f"❌ *Fetch fehlgeschlagen!*\n📊 Stream: {datastream_name}\n📉 Status: `{status}`\n{adverity_link}"
                requests.post(response_url, json={"response_type": "in_channel", "text": final_text})
                return
        except Exception as e:
            print(f"POLLING-FEHLER (Job {job_id}): {e}")
        time.sleep(45)

    print(f"POLLING-TIMEOUT: Job {job_id} hat die maximale Wartezeit überschritten.")
    timeout_text = f"⌛️ *Fetch-Überwachung Zeitüberschreitung* für Stream *{datastream_name}*.\nDer Job `{job_id}` läuft vermutlich noch. Bitte manuell prüfen: {adverity_link}"
    requests.post(response_url, json={"response_type": "in_channel", "text": timeout_text})

# --- Flask Routen (unverändert) ---
@app.route("/", methods=["GET"])
def index():
    return {"message": "Adverity Fetch API (v3.4: Super-Debug)"}

@app.route("/slack", methods=["POST"])
def slack_command():
    text, user_name, response_url = request.form.get('text', ''), request.form.get('user_name', 'unknown'), request.form.get('response_url')
    
    parts = text.strip().split()
    if len(parts) < 2: return jsonify({"response_type": "ephemeral", "text": "Format: /fetch name DD.MM.-DD.MM.YY"})
    datastream_name, date_range = parts[0], parts[1]
    
    DATASTREAM_MAP = {"meta": "674", "google": "678", "snapchat": "679", "tiktok": "675", "instafollows": "573"}
    datastream_id = DATASTREAM_MAP.get(datastream_name.lower())
    if not datastream_id: return jsonify({"response_type": "ephemeral", "text": f"Datastream '{datastream_name}' nicht gefunden."})
    
    try:
        date_parts = date_range.split('-'); start_str, end_str = date_parts[0].strip(), date_parts[1].strip()
        start_day, start_month = start_str.rstrip('.').split('.')
        end_parts = end_str.rstrip('.').split('.'); end_day, end_month = end_parts[0], end_parts[1]
        end_year = end_parts[2] if len(end_parts) > 2 else None
        year = f"20{end_year}" if end_year and len(end_year) == 2 else (end_year or str(datetime.now().year))
        start, end = f"{year}-{start_month.zfill(2)}-{start_day.zfill(2)}", f"{year}-{end_month.zfill(2)}-{end_day.zfill(2)}"
    except Exception as e: return jsonify({"response_type": "ephemeral", "text": f"Datumsformat ungültig: {e}"})

    if not all([ADVERITY_INSTANCE, ADVERITY_TOKEN]):
        return jsonify({"response_type": "ephemeral", "text": "Server-Konfigurationsfehler."})

    threading.Thread(
        target=execute_and_poll,
        args=(datastream_id, datastream_name, start, end, date_range, response_url, user_name, text)
    ).start()

    return jsonify({"response_type": "ephemeral", "text": f"⏳ Anfrage für *{datastream_name}* ({date_range}) angenommen. Job wird gestartet und überwacht..."})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
