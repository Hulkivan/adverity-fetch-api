import os
import json
from flask import Flask, request, jsonify
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import time
import threading

app = Flask(__name__)

# --- Konfiguration & Secrets ---
SHEET_ID = "1EZlvkKLfTBiYEbCrQIpFTDviXw3JOeSLTyYOCdPEEec"
ADVERITY_INSTANCE = os.environ.get("ADVERITY_INSTANCE")
ADVERITY_TOKEN = os.environ.get("ADVERITY_TOKEN")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")

# --- Client Initialisierung ---
slack_client = WebClient(token=SLACK_BOT_TOKEN)

# --- Hilfsfunktionen ---

def log_to_google_sheet(info: dict):
    """Loggt einen Eintrag in das definierte Google Sheet."""
    try:
        if not GOOGLE_CREDS_JSON:
            print("LOGGING-INFO: GOOGLE_CREDS_JSON nicht gefunden, Logging wird übersprungen.")
            return
        log_entry = [
            datetime.now().isoformat(), info.get('datastreamId'), info.get('start'),
            info.get('end'), info.get('instance'), info.get('rawPrompt', 'n/a')
        ]
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_json_dict = json.loads(GOOGLE_CREDS_JSON)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json_dict, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        sheet.insert_row(log_entry, index=2)
    except Exception as e:
        print(f"LOGGING-FEHLER (Google Sheets): {e}")

def send_dm(user_id, message):
    """Sendet eine Direktnachricht an einen Benutzer."""
    try:
        response = slack_client.conversations_open(users=user_id)
        channel_id = response["channel"]["id"]
        slack_client.chat_postMessage(channel=channel_id, text=message)
    except SlackApiError as e:
        print(f"Fehler beim Senden der DM an {user_id}: {e.response['error']}")

def execute_and_poll(user_id, datastream_id, datastream_name, start, end, date_range, user_name, text):
    """Startet den Fetch, pollt den Status und sendet Debug-DMs an den Nutzer."""
    
    send_dm(user_id, f"⚙️ DEBUG: Hintergrundprozess für *{datastream_name}* gestartet. Beginne Adverity-Job-Start.")
    
    # Adverity-Job starten
    url = f"https://{ADVERITY_INSTANCE}/api/datastreams/{datastream_id}/fetch_fixed/"
    headers = {"Authorization": f"Bearer {ADVERITY_TOKEN}", "Content-Type": "application/json"}
    body = {"start": start, "end": end}
    job_id = None

    try:
        log_to_google_sheet({
            "datastreamId": datastream_id, "start": start, "end": end,
            "instance": ADVERITY_INSTANCE, "rawPrompt": f"{user_name}: {text}"
        })
        response = requests.post(url, headers=headers, json=body, timeout=30)
        response.raise_for_status()
        data = response.json()
        job_id = (data.get("jobs", [{}])[0].get("id") if "jobs" in data and data["jobs"] else data.get("id"))
        if not job_id: raise ValueError(f"Konnte Job-ID nicht aus Adverity-Antwort extrahieren. Antwort war: {data}")
    except Exception as e:
        send_dm(user_id, f"❌ DEBUG: Fehler beim Starten des Adverity-Jobs: {e}")
        return

    send_dm(user_id, f"⚙️ DEBUG: Adverity-Job `{job_id}` erfolgreich gestartet. Beginne Status-Abfrage (Polling)...")
    
    # Polling des Job-Status
    status_url = f"https://{ADVERITY_INSTANCE}/api/jobs/{job_id}/"
    adverity_link = f"<https://{ADVERITY_INSTANCE}/jobs/{job_id}|Zu Adverity>"
    max_wait_time = 28 * 60
    start_time = time.time()
    poll_count = 0

    while time.time() - start_time < max_wait_time:
        try:
            res = requests.get(status_url, headers=headers, timeout=15).json()
            status = res.get("status_display", res.get("status", "unknown")).lower()
            
            poll_count += 1
            if poll_count == 1: # Nur beim ersten Poll eine Status-DM senden
                send_dm(user_id, f"⚙️ DEBUG (Poll #{poll_count}): Erster Status für Job `{job_id}` ist `{status}`.")

            if status not in ["pending", "running", "scheduled", "in warteschlange", "wird ausgeführt"]:
                if status in ["completed", "successful", "finished", "erfolgreich", "abgeschlossen"]:
                    final_text = f"✅ Dein Fetch für *{datastream_name}* ist erfolgreich abgeschlossen!\n📅 Zeitraum: {date_range}\n{adverity_link}"
                else:
                    final_text = f"❌ Dein Fetch für *{datastream_name}* ist fehlgeschlagen!\n📉 Status: `{status}`\n{adverity_link}"
                send_dm(user_id, final_text)
                return
        except Exception as e:
            send_dm(user_id, f"❌ DEBUG: Ein Fehler ist während des Pollings aufgetreten: {e}")
            return # Thread bei Fehler beenden
        time.sleep(60)

    timeout_text = f"⌛️ Die Überwachung deines Fetches für *{datastream_name}* hat die Zeit überschritten.\nDer Job `{job_id}` läuft vermutlich noch. Bitte manuell prüfen: {adverity_link}"
    send_dm(user_id, timeout_text)

# --- Flask Routen ---
@app.route("/", methods=["GET"])
def index():
    return {"message": "Adverity Fetch API (v5.2: Final-Debug)"}

@app.route("/slack", methods=["POST"])
def slack_command():
    user_id, user_name, text = request.form.get('user_id'), request.form.get('user_name', 'unknown'), request.form.get('text', '')
    
    parts = text.strip().split()
    if len(parts) < 2: return jsonify({"response_type": "ephemeral", "text": "Format: /fetch name DD.MM.-DD.MM.YY"})
    datastream_name, date_range = parts[0], parts[1]
    
    DATASTREAM_MAP = {"meta": "674", "google": "678", "snapchat": "679", "tiktok": "675", "instafollows": "573"}
    datastream_id = DATASTREAM_MAP.get(datastream_name.lower())
    if not datastream_id: return jsonify({"response_type": "ephemeral", "text": f"Datastream '{datastream_name}' nicht gefunden."})
    
    try:
        date_parts=date_range.split('-'); start_str, end_str = date_parts[0].strip(), date_parts[1].strip()
        start_day, start_month = start_str.rstrip('.').split('.')
        end_parts=end_str.rstrip('.').split('.'); end_day, end_month = end_parts[0], end_parts[1]
        end_year = end_parts[2] if len(end_parts) > 2 else None
        year = f"20{end_year}" if end_year and len(end_year) == 2 else (end_year or str(datetime.now().year))
        start, end = f"{year}-{start_month.zfill(2)}-{start_day.zfill(2)}", f"{year}-{end_month.zfill(2)}-{end_day.zfill(2)}"
    except Exception as e: return jsonify({"response_type": "ephemeral", "text": f"Datumsformat ungültig: {e}"})

    if not all([ADVERITY_INSTANCE, ADVERITY_TOKEN, SLACK_BOT_TOKEN]):
        return jsonify({"response_type": "ephemeral", "text": "Server-Konfigurationsfehler: Wichtige Umgebungsvariablen fehlen."})

    threading.Thread(target=execute_and_poll, args=(user_id, datastream_id, datastream_name, start, end, date_range, user_name, text)).start()
    
    return jsonify({"response_type": "ephemeral", "text": f"⏳ Anfrage für *{datastream_name}* ({date_range}) angenommen. Ich schicke dir jetzt eine Direktnachricht zur Bestätigung..."})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
