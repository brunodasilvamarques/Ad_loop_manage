from flask import Flask, request, redirect, jsonify, send_file, Response, render_template
from datetime import datetime, timedelta
import threading
import json
import os
import time
from collections import defaultdict
from threading import Timer
import pytz
import io
import csv
import requests
import msal
import base64

app = Flask(__name__)

# ========== Microsoft Graph Email Setup ==========
TENANT_ID = "ce3cbfd0-f41e-440c-a359-65cdc219ff9c"
CLIENT_ID = "673e7dd3-45ba-4bb6-a364-799147e7e9fc"
CLIENT_SECRET = "0lV8Q~_xqQ8wIkuLjKMwPFr4wtX.YycseJkYpcOo"  # Replace with your real client secret
SENDER_EMAIL = "b.marques@fcceinnovations.com"
RECIPIENT_EMAIL = "b.marques@fcceinnovations.com"

@app.template_filter('to_datetime')
def to_datetime_filter(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return datetime.min

# ======== CONFIG ========
DATA_FILE = "data/kiosks_data.json"
MAPPINGS_FILE = "data/kiosk_mappings.json"
HEARTBEAT_TIMEOUT = 300  # 5 minutes
ADMIN_USERNAME = "ChangeBoxAdmin"
ADMIN_PASSWORD = "Admin@@55"
USER_USERNAME = "ChangeBoxUser"
USER_PASSWORD = "UserFRM@@59"

# ======== GLOBALS ========
kiosks = {}
kiosk_mappings = {}  # AnyDesk ID -> Friendly Name and Country

# ======== UTILS ========
def load_kiosks():
    global kiosks
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            kiosks = json.load(f)


def save_kiosks():
    def convert(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj
    with open(DATA_FILE, 'w') as f:
        json.dump(kiosks, f, indent=4, default=convert)


def load_mappings():
    global kiosk_mappings
    if os.path.exists(MAPPINGS_FILE):
        with open(MAPPINGS_FILE, 'r') as f:
            kiosk_mappings = json.load(f)


def save_mappings():
    with open(MAPPINGS_FILE, 'w') as f:
        json.dump(kiosk_mappings, f, indent=4)


# ======== ROUTES ========

@app.route("/api/heartbeat", methods=["POST"])
def heartbeat():
    data = request.get_json()
    anydesk_id = data.get("anydesk_id")
    if not anydesk_id:
        return jsonify({"error": "Missing anydesk_id"}), 400

    now = datetime.utcnow()
    if anydesk_id not in kiosks:
        kiosks[anydesk_id] = {
            "first_seen": now.isoformat(),
            "last_seen": now.isoformat(),
            "uptime_history": [],
            "uptime_percent": 0,
            "video_stats": [],
            "location": "",
            "currency": "",
            "software_version": data.get("software_version", "unknown"),
            "configured": False
        }

    kiosk = kiosks[anydesk_id]
    kiosk["last_seen"] = now.isoformat()
    kiosk["software_version"] = data.get("software_version", kiosk.get("software_version", "unknown"))

    videos = []
    for v in data.get("videos", []):
        videos.append({
            "filename": v.get("filename"),
            "play_count": v.get("play_count", 0),
            "total_play_duration": v.get("total_play_duration", 0),
            "first_play": v.get("first_play"),
            "last_play": v.get("last_play")
        })
    kiosk["videos"] = videos
    kiosk["heartbeat_interval"] = 60  # assume 1-minute intervals for tracking

    # Ensure history exists
    if "uptime_history" not in kiosk:
        kiosk["uptime_history"] = []

    # Append actual timestamp
    if "actual_heartbeat_times" not in kiosk:
        kiosk["actual_heartbeat_times"] = []
    kiosk["actual_heartbeat_times"].append(now.isoformat())

    # Remove entries older than 1 day (to keep actual heartbeat memory lightweight)
    cutoff_actuals = now - timedelta(days=1)
    kiosk["actual_heartbeat_times"] = [
        t for t in kiosk["actual_heartbeat_times"]
        if datetime.fromisoformat(t) > cutoff_actuals
    ]

    # Determine if it's time to record an uptime point (every 2 mins)
    new_uptime_entry_added = False
    history = kiosk["uptime_history"]
    interval = 120  # 2 minutes
    now_ts = now.timestamp()

    if not history:
        last_record_time = now - timedelta(seconds=interval)
    else:
        last_record_time = datetime.fromisoformat(history[-1]["timestamp"])

    if (now - last_record_time).total_seconds() >= interval:
        # Match if any actual heartbeat within 90 seconds of expected
        matched = any(
            abs((now - datetime.fromisoformat(at)).total_seconds()) <= 90
            for at in kiosk["actual_heartbeat_times"]
        )
        kiosk["is_active"] = matched
        history.append({
            "timestamp": now.isoformat(),
            "status": "ok" if matched else "missed"
        })
        new_uptime_entry_added = True

    # Trim history to only last 24 hours
    cutoff = now - timedelta(hours=24)
    kiosk["uptime_history"] = [
        entry for entry in history
        if datetime.fromisoformat(entry["timestamp"]) > cutoff
    ]

    # Only update percentage if new entry added
    if new_uptime_entry_added:
        total = len(kiosk["uptime_history"])
        ok_count = sum(1 for entry in kiosk["uptime_history"] if entry["status"] == "ok")
        percent = (ok_count / total) * 100 if total else 0
        kiosk["uptime_percent"] = round(min(percent, 100), 2)

    save_kiosks()
    return jsonify({"status": "heartbeat received"})

def require_auth(func):
    def wrapper(*args, **kwargs):
        auth = request.authorization
        if not auth:
            return Response("Unauthorized", 401, {"WWW-Authenticate": "Basic realm='Login Required'"})

        request.is_admin = False

        if auth.username == ADMIN_USERNAME and auth.password == ADMIN_PASSWORD:
            request.is_admin = True
        elif auth.username == USER_USERNAME and auth.password == USER_PASSWORD:
            request.is_admin = False
        else:
            return Response("Unauthorized", 401, {"WWW-Authenticate": "Basic realm='Login Required'"})

        return func(*args, **kwargs)
    wrapper.__name__ = func.__name__
    return wrapper


@app.route('/')
@require_auth
def dashboard():
    now = datetime.utcnow()

    # Group kiosks into Configured and Not Configured
    configured = defaultdict(list)
    unconfigured = []

    for anydesk_id, info in kiosks.items():
        mapping = kiosk_mappings.get(anydesk_id)
        if mapping:
            configured[mapping['country']].append((anydesk_id, mapping['kiosk_name'], info))
        else:
            unconfigured.append((anydesk_id, info))  
            
    # Sort kiosks per country, alphabetically by name; not running at top
    for country in configured:
        configured[country].sort(key=lambda x: (
            0 if (now - to_datetime_filter(x[2].get('last_seen'))).total_seconds() > HEARTBEAT_TIMEOUT else 1,
            x[1].lower()
        ))

    return render_template('dashboard.html',
                           configured=configured,
                           unconfigured=unconfigured,
                           now=now,
                           heartbeat_timeout=HEARTBEAT_TIMEOUT,
                           is_admin=request.is_admin)
                           
@app.route('/uptime_debug/<anydesk_id>')
@require_auth
def uptime_debug(anydesk_id):
    if anydesk_id in kiosks:
        return jsonify({
            "uptime_percent": kiosks[anydesk_id].get("uptime_percent"),
            "uptime_history": kiosks[anydesk_id].get("uptime_history"),
            "actual_heartbeat_times": kiosks[anydesk_id].get("actual_heartbeat_times")
        })
    return jsonify({"error": "Kiosk not found"}), 404


@app.route('/configure', methods=['POST'])
@require_auth
def configure_kiosk():
    anydesk_id = request.form.get('anydesk_id')
    kiosk_name = request.form.get('kiosk_name')
    country = request.form.get('country')

    if anydesk_id and kiosk_name and country:
        kiosk_mappings[anydesk_id] = {"kiosk_name": kiosk_name, "country": country}
        save_mappings()

    return redirect('/')


@app.route('/reset_video/<anydesk_id>/<video_name>', methods=['POST'])
@require_auth
def reset_video(anydesk_id, video_name):
    if anydesk_id in kiosks:
        videos = kiosks[anydesk_id].get('videos', [])
        for video in videos:
            if video['filename'] == video_name:
                video['play_count'] = 0
                video['total_play_duration'] = 0
                video['first_play'] = None
                video['last_play'] = None
        save_kiosks()
    return redirect('/')


@app.route('/delete_video/<anydesk_id>/<video_name>', methods=['POST'])
@require_auth
def delete_video(anydesk_id, video_name):
    if anydesk_id in kiosks:
        videos = kiosks[anydesk_id].get('videos', [])
        kiosks[anydesk_id]['videos'] = [v for v in videos if v['filename'] != video_name]
        save_kiosks()
    return redirect('/')


@app.route('/delete_kiosk/<anydesk_id>', methods=['POST'])
@require_auth
def delete_pi(anydesk_id):
    if anydesk_id in kiosks:
        kiosks.pop(anydesk_id)
        save_kiosks()
    if anydesk_id in kiosk_mappings:
        kiosk_mappings.pop(anydesk_id)
        save_mappings()
    return redirect('/')
    
def update_all_kiosk_uptime():
    now = datetime.utcnow()
    interval = 120  # 2 minutes

    for kiosk in kiosks.values():
        # Ensure uptime_history and heartbeat log exist
        if "uptime_history" not in kiosk:
            kiosk["uptime_history"] = []
        if "actual_heartbeat_times" not in kiosk:
            kiosk["actual_heartbeat_times"] = []

        history = kiosk["uptime_history"]

        # Get last record time
        if not history:
            last_record_time = now - timedelta(seconds=interval)
        else:
            last_record_time = datetime.fromisoformat(history[-1]["timestamp"])

        # If enough time has passed
        if (now - last_record_time).total_seconds() >= interval:
            # Check if any heartbeat matches in last 90 sec
            matched = any(
                abs((now - datetime.fromisoformat(at)).total_seconds()) <= 90
                for at in kiosk["actual_heartbeat_times"]
            )
            kiosk["is_active"] = matched
            history.append({
                "timestamp": now.isoformat(),
                "status": "ok" if matched else "missed"
            })

            # Trim to 24 hrs
            cutoff = now - timedelta(hours=24)
            kiosk["uptime_history"] = [
                entry for entry in history if datetime.fromisoformat(entry["timestamp"]) > cutoff
            ]

            # Recalculate uptime %
            total = len(kiosk["uptime_history"])
            ok_count = sum(1 for entry in kiosk["uptime_history"] if entry["status"] == "ok")
            percent = (ok_count / total) * 100 if total else 0
            kiosk["uptime_percent"] = round(min(percent, 100), 2)

    save_kiosks()


@app.route('/download_csv')
@require_auth
def download_csv():
    from io import StringIO
    import csv

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Country", "Kiosk Name", "Video Name", "Play Count", "Total Duration (s)", "First Play", "Last Play"])

    for anydesk_id, info in kiosks.items():
        mapping = kiosk_mappings.get(anydesk_id, {"kiosk_name": anydesk_id, "country": "Unknown"})
        for video in info.get('videos', []):
            duration_seconds = int(video.get('total_play_duration', 0))

            writer.writerow([
                mapping['country'],
                mapping['kiosk_name'],
                video['filename'],
                video.get('play_count', 0),
                duration_seconds,
                video.get('first_play', 'None'),
                video.get('last_play', 'None')
            ])
    output.seek(0)
    return Response(output, mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=ChangeBox_Video_Stats.csv"})

def get_access_token():
    try:
        app = msal.ConfidentialClientApplication(
            CLIENT_ID,
            authority=f"https://login.microsoftonline.com/{TENANT_ID}",
            client_credential=CLIENT_SECRET
        )
        token_result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        if "access_token" in token_result:
            return token_result["access_token"]
        else:
            print(f"❌ Failed to get token: {token_result}")
            return None
    except Exception as e:
        print(f"❌ Exception getting token: {e}")
        return None
                   
def send_weekly_csv_email():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Country", "Kiosk Name", "Video Name", "Play Count", "Total Duration (s)", "First Play", "Last Play"])
    for anydesk_id, info in kiosks.items():
        mapping = kiosk_mappings.get(anydesk_id, {"kiosk_name": anydesk_id, "country": "Unknown"})
        for video in info.get('videos', []):
            writer.writerow([
                mapping['country'],
                mapping['kiosk_name'],
                video['filename'],
                video.get('play_count', 0),
                int(video.get('total_play_duration', 0)),
                video.get('first_play', 'None'),
                video.get('last_play', 'None')
            ])
    output.seek(0)
    csv_bytes = output.getvalue().encode("utf-8")

    token = get_access_token()
    if not token:
        print("❌ Cannot send email, no access token.")
        return

    email_data = {
        "message": {
            "subject": "📊 Weekly ChangeBox Advertisement Video_Looper CSV Report",
            "body": {
                "contentType": "Text",
                "content": "Attached is the weekly ChangeBox CSV report."
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": RECIPIENT_EMAIL
                    }
                }
            ],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": "ChangeBox_Video_Stats.csv",
                    "contentType": "text/csv",
                    "contentBytes": base64.b64encode(csv_bytes).decode("utf-8")
                }
            ]
        },
        "saveToSentItems": "true"
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(
        f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail",
        headers=headers,
        json=email_data
    )

    if response.status_code == 202:
        print("✅ Email sent successfully via Microsoft Graph.")
    else:
        print(f"❌ Failed to send email: {response.status_code} - {response.text}")

def schedule_email():
    from datetime import datetime, timedelta
    import pytz

    # Run every Monday at 9:00am UK time
    timezone = pytz.timezone("Europe/London")
    now = datetime.now(timezone)
    next_monday = now + timedelta((7 - now.weekday()) % 7)
    run_time = timezone.localize(datetime.combine(next_monday.date(), datetime.strptime("09:00", "%H:%M").time()))
    delay = (run_time - now).total_seconds()

    Timer(delay, email_task_wrapper).start()

def email_task_wrapper():
    send_weekly_csv_email()
    # Removed the recursive call to avoid infinite timers

schedule_email()

def schedule_uptime_checks():
    update_all_kiosk_uptime()
    Timer(120, schedule_uptime_checks).start()

schedule_uptime_checks()

if __name__ == '__main__':
    load_kiosks()
    load_mappings()
    schedule_email()  # Only runs when launched directly, not on import
    schedule_uptime_checks()
    app.run(host='0.0.0.0', port=5000)
