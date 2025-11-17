# app.py (angepasst mit Webhook-Logik)
import time
import threading
import os
import json
from flask import Flask, request, jsonify, Response # NEU: Response importiert
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from slack_sdk import WebClient # NEU: slack_sdk wird für direkte API-Calls benötigt

app = Flask(__name__)

# --- Konfiguration & Secrets (aus Ihrem Originalcode und ergänzt) ---
SHEET_ID = "1EZlvkKLfTBiYEbCrQIpFTDviXw3JOeSLTyYOCdPEEec"
# NEU: Diese Umgebungsvariablen werden für die Webhook-Lösung benötigt
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
APP_BASE_URL = os.environ.get("APP_BASE_URL") # Die öffentliche URL Ihrer App

# NEU: Slack WebClient Instanz erstellen
if not SLACK_BOT_TOKEN:
    print("WARNUNG: SLACK_BOT_TOKEN ist nicht gesetzt. Thread-Antworten werden fehlschlagen.")
slack_client = WebClient(token=SLACK_BOT_TOKEN)


# --- Bestehende Funktionen (unverändert) ---
def log_to_google_sheet(info: dict):
    # Diese Funktion bleibt exakt wie in Ihrem Original
    log_entry = [
        datetime.now().isoformat(),
        info.get('datastreamId'),
        info.get('start'),
        info.get('end'),
        info.get('instance'),
        info.get('rawPrompt', 'n/a')
    ]
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_json = json.loads(os.environ["GOOGLE_CREDS_JSON"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID)
    worksheet = sheet.sheet1
    worksheet.insert_row(log_entry, index=2)

@app.route("/", methods=["GET"])
def index():
    # Unverändert
    return {"message": "Adverity Fetch API with Google Sheets Logging 🎉 (Webhook-Version)"}

@app.route("/start-fetch", methods=["POST"])
def start_fetch():
    # Diese Route bleibt für andere Zwecke unverändert
    data = request.get_json()
    # ... (restlicher Code der Funktion bleibt gleich)
    try:
        log_to_google_sheet(data)
    except Exception as log_error:
        print(f"Log-Fehler: {log_error}")
    instance = data.get("instance")
    token = data.get("token")
    auth_type = data.get("authType", "Bearer")
    datastream_id = data.get("datastreamId")
    start = data.get("start")
    end = data.get("end")
    if not all([instance, token, datastream_id, start, end]):
        return jsonify({"error": "Fehlende Parameter"}), 400
    url = f"https://{instance}/api/datastreams/{datastream_id}/fetch_fixed/"
    headers = {"Authorization": f"{auth_type} {token}", "Content-Type": "application/json"}
    body = {"start": start, "end": end}
    try:
        response = requests.post(url, headers=headers, json=body, timeout=30)
        response.raise_for_status()
        return response.json(), 200
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e), "details": e.response.text if e.response else None}), 500


# --- /slack Route (stark angepasst) ---
@app.route("/slack", methods=["POST"])
def slack_command():
    """
    Empfängt Slack Slash Commands, postet eine Start-Nachricht
    und startet den Adverity-Job mit einer Callback-URL.
    """
    # Parsing des Inputs bleibt gleich
    text = request.form.get('text', '')
    user_name = request.form.get('user_name', 'unknown')
    channel_id = request.form.get('channel_id') # NEU: Wird benötigt, um zu wissen, wo wir posten sollen

    # ... (Ihr gesamter Code zum Parsen von `text` wird hier eingefügt) ...
    # ANNAHME: Der folgende Code-Block aus Ihrer Datei ist hier vorhanden und funktioniert.
    # Er extrahiert: datastream_name, datastream_id, start, end, date_range
    parts = text.strip().split()
    if len(parts) < 2: return jsonify({"response_type": "ephemeral", "text": "..."})
    datastream_name = parts[0]
    date_range = parts[1]
    DATASTREAM_MAP = {"meta": "674", "google": "678", "snapchat": "679", "tiktok": "675", "instafollows": "573"}
    datastream_id = DATASTREAM_MAP.get(datastream_name.lower())
    if not datastream_id: return jsonify({"response_type": "ephemeral", "text": f"Datastream '{datastream_name}' nicht gefunden..."})
    try:
        date_parts = date_range.split('-'); start_str = date_parts[0].strip(); end_str = date_parts[1].strip()
        start_day, start_month = start_str.rstrip('.').split('.')
        end_parts = end_str.rstrip('.').split('.'); end_day = end_parts[0]; end_month = end_parts[1]; end_year = end_parts[2] if len(end_parts) > 2 else None
        year = f"20{end_year}" if end_year and len(end_year) == 2 else end_year or str(datetime.now().year)
        start = f"{year}-{start_month.zfill(2)}-{start_day.zfill(2)}"; end = f"{year}-{end_month.zfill(2)}-{end_day.zfill(2)}"
    except Exception as e: return jsonify({"response_type": "ephemeral", "text": f"Datumsformat ungültig: {e}"})

    # Credentials aus Umgebungsvariablen holen (unverändert)
    instance = os.environ.get("ADVERITY_INSTANCE")
    token = os.environ.get("ADVERITY_TOKEN")
    if not instance or not token:
        return jsonify({"response_type": "ephemeral", "text": "❌ Server-Konfigurationsfehler (Credentials fehlen)"})

    # GEÄNDERT: Kein Threading mehr. Wir posten direkt eine Nachricht.
    try:
        initial_response = slack_client.chat_postMessage(
            channel=channel_id,
            text=f"⏳ Starte Fetch für *{datastream_name}* ({date_range})... _Du bekommst ein Update in diesem Thread._"
        )
        parent_message_ts = initial_response["ts"]
    except Exception as e:
        return jsonify({"response_type": "ephemeral", "text": f"❌ Slack-Fehler (Token ungültig?): {e}"})

    # NEU: Callback-URL erstellen, die Slack-Infos enthält
    if not APP_BASE_URL:
        return jsonify({"response_type": "ephemeral", "text": "❌ Server-Konfigurationsfehler (APP_BASE_URL fehlt)"})
    callback_url = f"{APP_BASE_URL}/adverity-webhook?channel={channel_id}&thread_ts={parent_message_ts}"

    # Adverity-Job mit Callback starten
    url = f"https://{instance}/api/datastreams/{datastream_id}/fetch_fixed/"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"start": start, "end": end, "callback": callback_url} # NEU: "callback" im Body

    log_data = {"datastreamId": datastream_id, "start": start, "end": end, "instance": instance, "rawPrompt": f"{user_name}: {text}"}

    try:
        log_to_google_sheet(log_data)
        response = requests.post(url, headers=headers, json=body, timeout=30)
        response.raise_for_status()
    except Exception as e:
        # Wenn der Start fehlschlägt, informieren wir den Nutzer im Thread
        error_text = f"❌ Fehler beim Starten des Adverity-Jobs: {str(e)}"
        slack_client.chat_postMessage(channel=channel_id, text=error_text, thread_ts=parent_message_ts)

    # GEÄNDERT: Wir senden eine leere 200-Antwort, da die Start-Nachricht bereits gepostet wurde.
    return Response(status=200)


# NEUE HILFSFUNKTION: Diese Funktion läuft im Hintergrund und fragt den Job-Status ab
def poll_adverity_status(job_id, channel_id, thread_ts):
    """
    Fragt den Status eines Adverity-Jobs so lange ab, bis er abgeschlossen ist,
    und postet dann das finale Ergebnis nach Slack.
    """
    instance = os.environ.get("ADVERITY_INSTANCE")
    token = os.environ.get("ADVERITY_TOKEN")
    status_url = f"https://{instance}/api/jobs/{job_id}/"
    headers = {"Authorization": f"Bearer {token}"}
    
    # Maximal 30 Minuten warten, um Endlosschleifen zu vermeiden
    max_wait_time = 30 * 60 
    start_time = time.time()

    while time.time() - start_time < max_wait_time:
        try:
            response = requests.get(status_url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                job_status = data.get("status", "unknown").lower()
                
                # Prüfen, ob der Job fertig ist (erfolgreich oder nicht)
                if job_status not in ["pending", "running", "scheduled"]:
                    datastream_name = data.get('datastream_name', 'unbekannt') # Annahme
                    adverity_link = f"<https://{instance}/jobs/{job_id}|Zu Adverity>"
                    
                    if job_status in ["completed", "successful", "finished"]:
                        final_text = f"✅ *Fetch erfolgreich!* (Abgeschlossen)\n📊 Stream: {datastream_name}\n🔗 Job-ID: `{job_id}`\n{adverity_link}"
                    else:
                        final_text = f"❌ *Fetch fehlgeschlagen!*\n📊 Stream: {datastream_name}\n🔗 Job-ID: `{job_id}`\n📉 Status: `{job_status}`"
                    
                    # Finale Nachricht posten und den Thread beenden
                    slack_client.chat_postMessage(channel=channel_id, text=final_text, thread_ts=thread_ts)
                    return # Wichtig: Schleife und Thread beenden

        except Exception as e:
            print(f"Fehler beim Pollen von Job {job_id}: {e}")
            # Optional: Eine Fehlermeldung nach Slack posten, dass das Polling gescheitert ist
            
        # Wenn nicht fertig, 45 Sekunden warten bis zur nächsten Abfrage
        time.sleep(45)

    # Fallback, wenn die maximale Wartezeit überschritten wurde
    timeout_text = f"⌛️ *Fetch-Überwachung Zeitüberschreitung*\nDer Job `{job_id}` läuft noch oder konnte nicht verifiziert werden. Bitte manuell in Adverity prüfen."
    slack_client.chat_postMessage(channel=channel_id, text=timeout_text, thread_ts=thread_ts)


# GEÄNDERTER WEBHOOK-EMPFÄNGER
@app.route("/adverity-webhook", methods=["POST"])
def handle_adverity_webhook():
    """
    Wird von Adverity SOFORT nach Job-Erstellung aufgerufen.
    Extrahiert die Job-ID und startet einen Hintergrund-Polling-Prozess.
    """
    channel_id = request.args.get("channel")
    thread_ts = request.args.get("thread_ts")

    if not channel_id or not thread_ts:
        return "Fehlende URL-Parameter", 400

    data = request.json
    # Annahme: Der Callback von Adverity enthält eine Liste von Jobs
    jobs = data.get("jobs", [])
    
    if not jobs:
        # Manchmal ist die Job-ID direkt im Root-Objekt
        job_id = data.get("id")
        if not job_id:
            print("Webhook-Fehler: Keine Job-ID im Payload gefunden.")
            return "Keine Job-ID gefunden", 400
    else:
        # Den ersten Job aus der Liste nehmen
        job_id = jobs[0].get("id")

    if not job_id:
        print("Webhook-Fehler: Job-Objekt enthält keine ID.")
        return "Job-Objekt enthält keine ID", 400

    # Einen Hintergrund-Thread starten, um den Job-Status zu pollen.
    # Dies blockiert die Antwort an Adverity nicht.
    polling_thread = threading.Thread(
        target=poll_adverity_status,
        args=(job_id, channel_id, thread_ts)
    )
    polling_thread.start()

    # SOFORT eine 200er-Antwort an Adverity senden, um zu bestätigen,
    # dass wir den Webhook erhalten haben.
    return Response(status=200)


# Unverändert
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

