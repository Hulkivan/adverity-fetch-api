import os
import json
from flask import Flask, request, jsonify
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# --- Konfiguration & Secrets ---
SHEET_ID = "1EZlvkKLfTBiYEbCrQIpFTDviXw3JOeSLTyYOCdPEEec"
ADVERITY_INSTANCE = os.environ.get("ADVERITY_INSTANCE")
ADVERITY_TOKEN = os.environ.get("ADVERITY_TOKEN")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")


# --- Hilfsfunktionen ---
def log_to_google_sheet(info: dict):
    """Loggt einen Eintrag in das definierte Google Sheet."""
    # HINWEIS: Diese Funktion wird nun im Hintergrund ausgeführt und könnte das 3s-Limit überschreiten.
    # Für maximale Stabilität könnte man sie in einen Thread auslagern oder bei Problemen entfernen.
    try:
        if not GOOGLE_CREDS_JSON: return
        log_entry = [datetime.now().isoformat(), info.get('datastreamId'), info.get('start'), info.get('end'), info.get('instance'), info.get('rawPrompt', 'n/a')]
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_CREDS_JSON), scope)
        client = gspread.authorize(creds)
        client.open_by_key(SHEET_ID).sheet1.insert_row(log_entry, index=2)
    except Exception as e:
        print(f"LOGGING-FEHLER (Google Sheets): {e}")


# --- Flask Routen ---
@app.route("/", methods=["GET"])
def index():
    return {"message": "Adverity Fetch API (v6.0: Robust & Simple)"}

@app.route("/slack", methods=["POST"])
def slack_command():
    """Startet den Job und antwortet sofort mit der Job-ID."""
    
    # --- Parsing ---
    text = request.form.get('text', '')
    user_name = request.form.get('user_name', 'unknown')
    
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
    except Exception as e:
        return jsonify({"response_type": "ephemeral", "text": f"Datumsformat ungültig: {e}"})

    if not all([ADVERITY_INSTANCE, ADVERITY_TOKEN]):
        return jsonify({"response_type": "ephemeral", "text": "Server-Konfigurationsfehler."})
        
    # --- Adverity-Job starten ---
    url = f"https://{ADVERITY_INSTANCE}/api/datastreams/{datastream_id}/fetch_fixed/"
    headers = {"Authorization": f"Bearer {ADVERITY_TOKEN}", "Content-Type": "application/json"}
    body = {"start": start, "end": end}
    
    try:
        # Führe das Logging aus (auf die Gefahr hin, dass es das 3s-Limit sprengt)
        log_to_google_sheet({
            "datastreamId": datastream_id, "start": start, "end": end,
            "instance": ADVERITY_INSTANCE, "rawPrompt": f"{user_name}: {text}"
        })
        
        # Starte den Job
        response = requests.post(url, headers=headers, json=body, timeout=10)
        response.raise_for_status()
        data = response.json()
        job_id = (data.get("jobs", [{}])[0].get("id") if "jobs" in data and data["jobs"] else data.get("id"))
        if not job_id: raise ValueError("Konnte Job-ID nicht aus Antwort extrahieren.")

        # --- Sofortige und finale Antwort an Slack ---
        adverity_link = f"<https://{ADVERITY_INSTANCE}/jobs/{job_id}|Zu Adverity>"
        final_text = f"✅ *Fetch erfolgreich gestartet!*\n📊 Stream: {datastream_name}\n📅 Zeitraum: {date_range}\n🔗 Job-ID: `{job_id}`\n\nDer Job läuft nun im Hintergrund. Überprüfe den Status hier: {adverity_link}"
        return jsonify({"response_type": "in_channel", "text": final_text})

    except Exception as e:
        # Fange alle Fehler ab und gib eine verständliche Rückmeldung
        return jsonify({"response_type": "ephemeral", "text": f"❌ Ein Fehler ist aufgetreten: {e}"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
