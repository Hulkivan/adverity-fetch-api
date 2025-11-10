import threading
import os
import json
from flask import Flask, request, jsonify
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# Google Sheets Setup
SHEET_ID = "1EZlvkKLfTBiYEbCrQIpFTDviXw3JOeSLTyYOCdPEEec"

def log_to_google_sheet(info: dict):
    # Eintrag vorbereiten
    log_entry = [
        datetime.now().isoformat(),
        info.get('datastreamId'),
        info.get('start'),
        info.get('end'),
        info.get('instance'),
        info.get('rawPrompt', 'n/a')
    ]

    # Google Sheets API via Umgebungsvariable laden
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_json = json.loads(os.environ["GOOGLE_CREDS_JSON"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)

    # Einfügen in Google Sheet
    sheet = client.open_by_key(SHEET_ID)
    worksheet = sheet.sheet1
    worksheet.insert_row(log_entry, index=2)

@app.route("/", methods=["GET"])
def index():
    return {"message": "Adverity Fetch API with Google Sheets Logging 🎉"}

@app.route("/start-fetch", methods=["POST"])
def start_fetch():
    data = request.get_json()

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
    headers = {
        "Authorization": f"{auth_type} {token}",
        "Content-Type": "application/json"
    }
    body = {"start": start, "end": end}

    try:
        response = requests.post(url, headers=headers, json=body, timeout=30)
        response.raise_for_status()
        return response.json(), 200
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e), "details": e.response.text if e.response else None}), 500

import threading

@app.route("/slack", methods=["POST"])
def slack_command():
    """
    Empfängt Slack Slash Commands wie: /fetch meta 01.06.-02.06.25
    """
    # Slack sendet Form-Data, nicht JSON
    text = request.form.get('text', '')
    user_name = request.form.get('user_name', 'unknown')
    response_url = request.form.get('response_url')  # Für delayed response
    
    if not text:
        return jsonify({
            "response_type": "ephemeral",
            "text": "❌ Bitte Format nutzen: `/fetch datastream-name DD.MM.-DD.MM.YY`"
        })
    
    # Text parsen
    parts = text.strip().split()
    
    if len(parts) < 2:
        return jsonify({
            "response_type": "ephemeral",
            "text": "❌ Zu wenig Infos. Beispiel: `/fetch meta 01.06.-02.06.25`"
        })
    
    datastream_name = parts[0]
    date_range = parts[1]
    
    # Datastream-Mapping
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
        return jsonify({
            "response_type": "ephemeral",
            "text": f"❌ Datastream '{datastream_name}' nicht gefunden.\nVerfügbar: {available}"
        })
    
    # Datums-Parsing
    try:
        date_parts = date_range.split('-')
        if len(date_parts) != 2:
            raise ValueError("Ungültiges Format")
        
        start_str = date_parts[0].strip()
        end_str = date_parts[1].strip()
        
        start_day, start_month = start_str.rstrip('.').split('.')
        
        end_parts = end_str.rstrip('.').split('.')
        end_day = end_parts[0]
        end_month = end_parts[1]
        end_year = end_parts[2] if len(end_parts) > 2 else None
        
        if end_year:
            year = f"20{end_year}" if len(end_year) == 2 else end_year
        else:
            year = str(datetime.now().year)
        
        start = f"{year}-{start_month.zfill(2)}-{start_day.zfill(2)}"
        end = f"{year}-{end_month.zfill(2)}-{end_day.zfill(2)}"
        
    except Exception as parse_error:
        return jsonify({
            "response_type": "ephemeral",
            "text": f"❌ Datumsformat ungültig: {str(parse_error)}\nNutze: DD.MM.-DD.MM.YY"
        })
    
    # Credentials
    instance = os.environ.get("ADVERITY_INSTANCE")
    token = os.environ.get("ADVERITY_TOKEN")
    
    if not instance or not token:
        return jsonify({
            "response_type": "ephemeral",
            "text": "❌ Server-Konfigurationsfehler (Credentials fehlen)"
        })
    
    # Background-Job starten
    def run_fetch():
        """Läuft im Hintergrund und sendet Ergebnis zurück an Slack"""
        log_data = {
            "datastreamId": datastream_id,
            "start": start,
            "end": end,
            "instance": instance,
            "rawPrompt": f"{user_name}: {text}"
        }
        
        url = f"https://{instance}/api/datastreams/{datastream_id}/fetch_fixed/"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        body = {"start": start, "end": end}
        
        try:
            # Loggen
            try:
                log_to_google_sheet(log_data)
            except Exception as log_error:
                print(f"Sheet-Logging fehlgeschlagen: {log_error}")
            
            # Fetch ausführen
            response = requests.post(url, headers=headers, json=body, timeout=120)
            
            if response.status_code in [200, 201, 202]:
                try:
                    result = response.json()
                    job_id = result.get('id', result.get('job_id', 'gestartet'))
                except:
                    job_id = "gestartet"
                
                success_msg = {
                    "response_type": "in_channel",
                    "text": f"✅ *Fetch erfolgreich!*\n📊 Stream: {datastream_name}\n📅 Zeitraum: {date_range} ({start} bis {end})\n🔗 Job-ID: `{job_id}`\n\n<https://{instance}/datastreams/{datastream_id}|Zu Adverity>"
                }
                requests.post(response_url, json=success_msg, timeout=5)
            else:
                error_msg = {
                    "response_type": "ephemeral",
                    "text": f"❌ Adverity-Fehler (HTTP {response.status_code})"
                }
                requests.post(response_url, json=error_msg, timeout=5)
                
        except requests.exceptions.Timeout:
            timeout_msg = {
                "response_type": "in_channel",
                "text": f"⏳ *Fetch gestartet (langsame Response)*\n📊 Stream: {datastream_name}\n📅 {date_range}\n\nCheck Adverity für Status.\n<https://{instance}/app/datastreams/{datastream_id}|Zu Adverity>"
            }
            requests.post(response_url, json=timeout_msg, timeout=5)
            
        except Exception as e:
            error_msg = {
                "response_type": "ephemeral",
                "text": f"❌ Fehler: {str(e)}"
            }
            requests.post(response_url, json=error_msg, timeout=5)
    
    # Thread starten (nicht blockierend)
    thread = threading.Thread(target=run_fetch)
    thread.start()
    
    # SOFORT antworten (innerhalb 3 Sekunden für Slack)
    return jsonify({
        "response_type": "ephemeral",
        "text": f"⏳ Starte Fetch für *{datastream_name}* ({date_range})...\n_Du bekommst gleich eine Update-Nachricht_"
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
