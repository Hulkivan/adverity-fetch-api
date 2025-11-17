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
# Wir lesen die Variablen hier, aber der Thread wird sie zur Sicherheit erneut prüfen
SHEET_ID = "1EZlvkKLfTBiYEbCrQIpFTDviXw3JOeSLTyYOCdPEEec"
ADVERITY_INSTANCE = os.environ.get("ADVERITY_INSTANCE")
ADVERITY_TOKEN = os.environ.get("ADVERITY_TOKEN")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")


# --- Hilfsfunktionen ---

def log_to_google_sheet(info: dict):
    # ... (Diese Funktion bleibt unverändert) ...

def execute_and_poll(datastream_id, datastream_name, start, end, date_range, response_url, user_name, text):
    """Führt alle langsamen Operationen im Hintergrund aus und loggt JEDEN Schritt."""
    
    # --- ALLERERSTER SCHRITT: VARIABLEN PRÜFEN ---
    print("--- BACKGROUND THREAD STARTED ---")
    
    # Lese die Variablen INNERHALB des Threads erneut, um Caching-Probleme auszuschließen
    instance_check = os.environ.get("ADVERITY_INSTANCE")
    token_check = os.environ.get("ADVERITY_TOKEN")
    google_creds_check = os.environ.get("GOOGLE_CREDS_JSON")
    
    # Schreibe den Status jeder Variable in die Logs
    print(f"THREAD-CHECK: ADVERITY_INSTANCE is present: {bool(instance_check)}")
    print(f"THREAD-CHECK: ADVERITY_TOKEN is present: {bool(token_check)}")
    print(f"THREAD-CHECK: GOOGLE_CREDS_JSON is present: {bool(google_creds_check)}")

    if not all([instance_check, token_check]):
        print("FATAL-ERROR: Thread bricht ab, da Adverity-Credentials fehlen.")
        requests.post(response_url, json={"response_type": "ephemeral", "text": "❌ Konnte Adverity-Credentials im Hintergrundprozess nicht laden. Bitte Admin prüfen."})
        return
        
    # Ab hier sollte der Rest des Codes laufen
    log_to_google_sheet({
        "datastreamId": datastream_id, "start": start, "end": end,
        "instance": instance_check, "rawPrompt": f"{user_name}: {text}"
    })
    
    # ... (Der Rest der Funktion bleibt exakt wie im vorigen Code) ...
    url = f"https://{instance_check}/api/datastreams/{datastream_id}/fetch_fixed/"
    headers = {"Authorization": f"Bearer {token_check}", "Content-Type": "application/json"}
    body = {"start": start, "end": end}
    # ... etc. ...


# --- Flask Routen (unverändert) ---
@app.route("/", methods=["GET"])
def index():
    return {"message": "Adverity Fetch API (v3.5: Final-Debug)"}

@app.route("/slack", methods=["POST"])
def slack_command():
    # ... (Diese Route bleibt exakt wie im vorigen Code) ...
    text, user_name, response_url = request.form.get('text', ''), request.form.get('user_name', 'unknown'), request.form.get('response_url')
    # ... etc. ...
    threading.Thread(target=execute_and_poll, args=(...)).start()
    return jsonify({"response_type": "ephemeral", "text": "..."})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

