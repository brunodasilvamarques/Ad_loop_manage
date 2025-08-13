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
from zoneinfo import ZoneInfo  # for BST/GMT-aware formatting

app = Flask(__name__)
os.makedirs("data", exist_ok=True)

# ========== Microsoft Graph Email Setup ==========
TENANT_ID = "ce3cbfd0-f41e-440c-a359-65cdc219ff9c"
CLIENT_ID = "673e7dd3-45ba-4bb6-a364-799147e7e9fc"
CLIENT_SECRET = "wJS8Q~Nbfs1cxhmuB1pQLk4cFB~l0X_KiYFBxbfE"  # Replace with your real client secret
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
    # ✅ If the client sent kiosk_name, auto-map it so no manual step is needed
    kiosk_name = data.get("kiosk_name")
    country = data.get("country")  # optional

    if kiosk_name:
        existing = kiosk_mappings.get(anydesk_id, {})
        new_country = country or existing.get('country') or "Unknown"
        if (not existing) or (existing.get('kiosk_name') != kiosk_name) or (existing.get('country') != new_country):
            kiosk_mappings[anydesk_id] = {"kiosk_name": kiosk_name, "country": new_country}
            save_mappings()

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
            "configured": False,
            "offline_alert_sent": False
        }

    kiosk = kiosks[anydesk_id]
    kiosk["last_seen"] = now.isoformat()
    kiosk["offline_alert_sent"] = False
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
        # Attach weekly uptime from master_data (does not slow down much; files are small)
        try:
            weekly = compute_weekly_uptime_from_master(anydesk_id)
            info["weekly_uptime_percent"] = weekly["weekly_percent"]
        except Exception:
            info["weekly_uptime_percent"] = None

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
    
@app.route('/uptime_week/<anydesk_id>')
@require_auth
def uptime_week(anydesk_id):
    return jsonify(compute_weekly_uptime_from_master(anydesk_id))

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
    
def _find_master_files_for_kiosk(anydesk_id: str) -> list[str]:
    """Return list of candidate master_data json paths for this kiosk (current+prev year)."""
    paths = []
    try:
        # We try to match by kiosk_name first (if known), else AnyDesk prefix in filename
        mapping = kiosk_mappings.get(anydesk_id, {})
        name = (mapping.get("kiosk_name") or "").strip()
        now = datetime.utcnow()
        years = {now.year, now.year - 1}
        for y in years:
            pattern = f"master_data_{y}.json"
            for fname in os.listdir("data"):
                if fname.endswith(pattern):
                    full = os.path.join("data", fname)
                    try:
                        with open(full, "r") as f:
                            j = json.load(f)
                        if name and (j.get("kiosk_name") or "").strip() == name:
                            paths.append(full)
                        elif not name and (j.get("device_id") == anydesk_id or fname.startswith(anydesk_id + "_")):
                            paths.append(full)
                    except Exception:
                        continue
    except Exception:
        pass
    return sorted(set(paths))
    
def compute_weekly_uptime_from_master(anydesk_id: str):
    """
    Returns: {
      "weekly_percent": 0..100,
      "days": { "YYYY-MM-DD": {"00:00":0/1, ... "23:00":0/1} }  # only last 7 days
    }
    Missing hours are treated as 0.
    """
    files = _find_master_files_for_kiosk(anydesk_id)
    if not files:
        return {"weekly_percent": 0.0, "days": {}}

    # Merge by date across any found files (handles year crossover)
    by_date = {}
    for path in files:
        try:
            with open(path, "r") as f:
                j = json.load(f)
            for key, val in j.items():
                if key.count("-") == 2 and isinstance(val, dict):  # looks like a date block
                    by_date.setdefault(key, {})
                    uh = val.get("uptime_hours", {})
                    # Normalize HH:00 keys as strings
                    if isinstance(uh, dict):
                        by_date[key].update({k: 1 if int(v) == 1 else 0 for k, v in uh.items()})
        except Exception:
            continue

    # Consider last 7 calendar days
    today = datetime.utcnow().date()
    window = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
    days_out = {}
    total_hours = 0
    up_hours = 0

    for d in reversed(window):  # oldest->newest order
        hours_map = {}
        for h in range(24):
            key = f"{h:02d}:00"
            v = int(by_date.get(d, {}).get(key, 0))
            hours_map[key] = v
            up_hours += v
            total_hours += 1
        days_out[d] = hours_map

    percent = (up_hours / total_hours) * 100 if total_hours else 0.0
    return {"weekly_percent": round(percent, 2), "days": days_out}
    
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
    
def format_london(dt_aware_or_iso: str | datetime) -> str:
    """
    Return '12 Aug 2025 21:33:31 BST' in Europe/London.
    Accepts ISO string or datetime (naive = UTC).
    """
    try:
        if isinstance(dt_aware_or_iso, str):
            dt = datetime.fromisoformat(dt_aware_or_iso)
        else:
            dt = dt_aware_or_iso
        # treat naive as UTC, then convert to London
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(ZoneInfo("Europe/London")).strftime("%d %b %Y %H:%M:%S %Z")
    except Exception:
        return str(dt_aware_or_iso)
    
def check_offline_alerts():
    now = datetime.utcnow()
    for anydesk_id, info in kiosks.items():
        try:
            last = datetime.fromisoformat(info.get("last_seen"))
        except Exception:
            continue

        delta = (now - last).total_seconds()
        was_sent = info.get("offline_alert_sent", False)

        if delta > HEARTBEAT_TIMEOUT and not was_sent:
            # Resolve friendly name + country (or fallback)
            mapping = kiosk_mappings.get(anydesk_id, {"kiosk_name": anydesk_id, "country": "Unknown"})
            name = mapping.get("kiosk_name", anydesk_id)
            country = mapping.get("country", "Unknown")

            subject = f"⚠️ VLS Offline | Kiosk: {country} - {name}"
            body = (
                "The Video Looper Software for the kiosk detailed below has gone offline.\n\n"
                f"Country: {country}\n"
                f"Kiosk ID: {anydesk_id}\n"
                f"Kiosk Name: {name}\n"
                f"Last heartbeat (UK local time): {format_london(info.get('last_seen'))}\n"
                f"Offline threshold: {HEARTBEAT_TIMEOUT}s "
                f"({HEARTBEAT_TIMEOUT // 60} minute{'s' if HEARTBEAT_TIMEOUT // 60 != 1 else ''})\n"
            )
            send_text_email(subject, body, [RECIPIENT_EMAIL])
            info["offline_alert_sent"] = True


        # Optional recovery email (uncomment to use)
        # if delta <= HEARTBEAT_TIMEOUT and was_sent:
        #     mapping = kiosk_mappings.get(anydesk_id, {"kiosk_name": anydesk_id, "country": "Unknown"})
        #     name = mapping.get("kiosk_name", anydesk_id)
        #     country = mapping.get("country", "Unknown")
        #     subject = f"✅ Kiosk Back Online: {country} - {name}"
        #     body = f"Kiosk {country} - {name} is back online at {now.isoformat()} UTC."
        #     send_text_email(subject, body, [RECIPIENT_EMAIL])
        #     info["offline_alert_sent"] = False

    save_kiosks()

@app.route('/download_csv')
@require_auth
def download_csv():
    from io import StringIO
    import csv
    import threading

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Country", "Kiosk Name", "Video Name",
        "Play Count", "Total Duration (s)", "First Play", "Last Play"
    ])

    for anydesk_id, info in kiosks.items():
        mapping = kiosk_mappings.get(
            anydesk_id, {"kiosk_name": anydesk_id, "country": "Unknown"}
        )
        for video in info.get('videos', []):
            duration_seconds = int(video.get('total_play_duration', 0))
            writer.writerow([
                mapping['country'],
                mapping['kiosk_name'],
                video.get('filename'),
                video.get('play_count', 0),
                duration_seconds,
                video.get('first_play', 'None'),
                video.get('last_play', 'None'),
            ])

    # Get CSV as bytes for both email and HTTP response
    csv_text = output.getvalue()
    csv_bytes = csv_text.encode("utf-8")

    # 📧 Fire-and-forget email to Bruno when the CSV is downloaded
    try:
        # Uses the helper you added earlier: send_csv_email(subject, csv_bytes, recipients)
        # and the constant RECIPIENT_EMAIL = "b.marques@fcceinnovations.com"
        threading.Thread(
            target=send_csv_email,
            args=("ChangeBox Kiosk - Advertisement Video Playback Data", csv_bytes, [RECIPIENT_EMAIL]),
            daemon=True,
        ).start()
    except Exception as e:
        print(f"⚠️ Failed to queue CSV email on download: {e}")

    output.seek(0)
    return Response(
        output,
        mimetype="text/csv",
        headers={'Content-Disposition': 'attachment; filename="ChangeBox_Advertisement Video Playback_Combined Data.csv"'}
    )                   

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
        
def send_text_email(subject, body, recipients):
    token = get_access_token()
    if not token:
        print("❌ Cannot send alert email (no token).")
        return

    email_data = {
        "message": {
            "subject": subject,
            "body": {"contentType": "Text", "content": body},
            "toRecipients": [{"emailAddress": {"address": r}} for r in recipients],
        },
        "saveToSentItems": "true",
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.post(
        f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail",
        headers=headers, json=email_data
    )
    if resp.status_code == 202:
        print("✅ Offline alert email sent.")
    else:
        print(f"❌ Failed to send offline alert: {resp.status_code} - {resp.text}")
        
def send_csv_email(subject, csv_bytes, recipients):
    token = get_access_token()
    if not token:
        print("❌ Cannot send email, no access token.")
        return

    email_data = {
        "message": {
            "subject": subject,
            "body": {"contentType": "Text", "content": "Attached is the CSV export."},
            "toRecipients": [{"emailAddress": {"address": r}} for r in recipients],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": "ChangeBox_Advertisement Video Playback_Combined Data.csv",
                    "contentType": "text/csv",
                    "contentBytes": base64.b64encode(csv_bytes).decode("utf-8")
                }
            ],
        },
        "saveToSentItems": "true",
    }

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.post(
        f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail",
        headers=headers,
        json=email_data,
    )
    if resp.status_code == 202:
        print("✅ CSV email sent (download trigger).")
    else:
        print(f"❌ Failed to send download CSV email: {resp.status_code} - {resp.text}")
        
@app.route('/upload_json', methods=['POST'])
def upload_json():
    uploaded_file = request.files.get('file')
    if not uploaded_file:
        return "❌ No file uploaded", 400

    raw = uploaded_file.read()
    if not raw or not raw.strip():
        return "❌ Empty upload", 400

    try:
        json_payload = json.loads(raw)
    except Exception as e:
        print(f"❌ JSON decode failed for {uploaded_file.filename}: {e}")
        return "❌ Invalid JSON format", 400

    # Save as sent filename under data/
    filename = os.path.basename(uploaded_file.filename)
    save_path = os.path.join("data", filename)
    try:
        with open(save_path, "w") as f:
            json.dump(json_payload, f, indent=2)
        print(f"✅ Uploaded master_data file: {save_path}")
        return f"✅ Uploaded {uploaded_file.filename}", 200
    except Exception as e:
        print(f"❌ Failed to save uploaded JSON: {e}")
        return "❌ Server error saving file", 500
                   
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
            "subject": "📊 Weekly ChangeBox Advertisement Video Playback CSV Report",
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
                    "name": "ChangeBox_Advertisement Video Playback_Combined Data.csv",
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
    """
    Schedule the next send for Monday 09:00 Europe/London, DST-aware.
    If it's already past Monday 09:00 this week, schedule next Monday.
    """
    tz = pytz.timezone("Europe/London")
    now = datetime.now(tz)

    # Candidate = today at 09:00 (Europe/London)
    today_0900 = tz.localize(datetime.strptime(now.strftime("%Y-%m-%d") + " 09:00", "%Y-%m-%d %H:%M"))

    # Compute days ahead to next Monday
    days_ahead = (0 - now.weekday()) % 7  # Monday=0
    # If we're on Monday but already past 09:00, push to next week
    if days_ahead == 0 and now >= today_0900:
        days_ahead = 7

    target_date = (now + timedelta(days=days_ahead)).date()
    run_time = tz.localize(datetime.combine(target_date, datetime.strptime("09:00", "%H:%M").time()))
    delay = (run_time - now).total_seconds()

    # Safety: if delay slipped negative/zero (clock change), jump a week
    if delay <= 0:
        delay += 7 * 24 * 60 * 60

    print(f"⏰ Weekly CSV scheduled for {run_time.isoformat()} (in {int(delay)}s)")
    Timer(delay, email_task_wrapper).start()


def email_task_wrapper():
    """Send the weekly CSV, then schedule the next one."""
    send_weekly_csv_email()
    # Immediately schedule the next Monday 09:00 after sending
    schedule_email()

def schedule_uptime_checks():
    update_all_kiosk_uptime()
    check_offline_alerts()   # ✅ send alert if any kiosk is offline past threshold
    Timer(120, schedule_uptime_checks).start()
    
# ---- Start background jobs once, for both Gunicorn and python run ----
_background_jobs_started = False
def start_background_jobs_once():
    global _background_jobs_started
    if _background_jobs_started:
        return
    print("🔧 Starting background jobs: weekly email + offline monitor")
    # ensure data is loaded before jobs run
    load_kiosks()
    load_mappings()
    schedule_email()
    schedule_uptime_checks()
    _background_jobs_started = True

@app.before_request
def _kick_jobs():
    # Flask 3.x safe: this runs on first request; guard prevents duplicates
    start_background_jobs_once()

if __name__ == '__main__':
    start_background_jobs_once()
    app.run(host='0.0.0.0', port=5000)
