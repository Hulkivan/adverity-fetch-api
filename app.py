import os
import json
from flask import Flask, request, jsonify
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import threading

app = Flask(__name__)

# --- Konfiguration & Secrets ---
SHEET_ID = "1EZlvkKLfTBiYEbCrQIpFTDviXw3JOeSLTyYOCdPEEec"
ADVERITY_INSTANCE = os.environ.get("ADVERITY_INSTANCE")
ADVERITY_TOKEN = os.environ.get("ADVERITY_TOKEN")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")


# --- Hilfsfunktionen ---

def execute_in_background(response_url, log_data, fetch_url, fetch_headers, fetch_body, final_message_template):
    """Führt die langsamen Aktionen (Logging, Fetch) im Hintergrund aus und sendet die finale Nachricht."""
    try:
        # 1. Google Sheet Logging (langsam)
        try:
            # Erneutes Holen der Credentials innerhalb des Threads ist sicherer
            creds_str = os.environ.get("GOOGLE_CREDS_JSON")
            if creds_str:
                scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
                creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_str), scope)
                client = gspread.authorize(creds)
                client.open_by_key(SHEET_ID).sheet1.insert_row([
                    datetime.now().isoformat(), log_data.get('datastreamId'), log_data.get('start'),
                    log_data.get('end'), log_data.get('instance'), log_data.get('rawPrompt', 'n/a')
                ], index=2)
        except Exception as log_e:
            print(f"HINTERGRUND-LOGGING-FEHLER: {log_e}")

        # 2. Adverity Fetch (langsam)
        response = requests.post(fetch_url, headers=fetch_headers, json=fetch_body, timeout=30)
        response.raise_for_status()
        result = response.json()
        
        # Job-ID extrahieren
        job_id = (result.get("jobs", [{}])[0].get("id") if "jobs" in result and result.get("jobs") else result.get("id", "unbekannt"))
        
        # 3. Finale Nachricht an Slack senden
        final_text = final_message_template.format(job_id=job_id)
        requests.post(response_url, json={"response_type": "in_channel", "text": final_text})

    except Exception as e:
        # Bei einem Fehler eine private Nachricht an den Nutzer senden
        error_text = f"❌ Ein Fehler ist im Hintergrund aufgetreten: {e}"
        requests.post(response_url, json={"response_type": "ephemeral", "text": error_text})


# --- Flask Routen ---

@app.route("/", methods=["GET"])
def index():
    return {"message": "Adverity Fetch API (v9.0: Stable & Simple)"}

@app.route("/slack", methods=["POST"])
def slack_command():
    """Nimmt den Befehl an, startet den Hintergrundprozess und antwortet SOFORT."""
    
    # --- Parsing (schnell) ---
    text = request.form.get('text', '')
    user_name = request.form.get('user_name', 'unknown')
    response_url = request.form.get('response_url') # Wichtig für die Antwort aus dem Thread
    
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

    if not all([ADVERITY_INSTANCE, ADVERITY_TOKEN]):
        return jsonify({"response_type": "ephemeral", "text": "Server-Konfigurationsfehler."})
        
    # --- Daten für den Hintergrundprozess vorbereiten ---
    log_data = {
        "datastreamId": datastream_id, "start": start, "end": end,
        "instance": ADVERITY_INSTANCE, "rawPrompt": f"{user_name}: {text}"
    }
    fetch_url = f"https://{ADVERITY_INSTANCE}/api/datastreams/{datastream_id}/fetch_fixed/"
    fetch_headers = {"Authorization": f"Bearer {ADVERITY_TOKEN}", "Content-Type": "application/json"}
    fetch_body = {"start": start, "end": end}
    
    adverity_link = f"<https://{ADVERITY_INSTANCE}/jobs/{{job_id}}|Zu Adverity>"
    final_message_template = f"✅ *Fetch erfolgreich gestartet!*\n📊 Stream: {datastream_name}\n📅 Zeitraum: {date_range}\n🔗 Job-ID: `{{job_id}}`\n\nDer Job läuft nun im Hintergrund. Überprüfe den Status hier: {adverity_link}"

    # --- Hintergrundprozess starten (schnell) ---
    threading.Thread(target=execute_in_background, args=(
        response_url, log_data, fetch_url, fetch_headers, fetch_body, final_message_template
    )).start()

    # --- Sofortige Antwort an Slack senden (schnell) ---
    return jsonify({"response_type": "ephemeral", "text": "⏳ Anfrage angenommen. Starte den Fetch und das Logging im Hintergrund..."})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

