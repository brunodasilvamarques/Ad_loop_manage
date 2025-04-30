from flask import Flask, request, redirect, jsonify, send_file, Response, render_template
from datetime import datetime
import threading
import json
import os
import time
from collections import defaultdict

app = Flask(__name__)

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

@app.route('/api/heartbeat', methods=['POST'])
def heartbeat():
    data = request.get_json()
    anydesk_id = data.get("anydesk_id")
    now = datetime.utcnow()
    software_version = data.get("software_version", "Unknown")

    def safe_datetime(val):
        return val.isoformat() if isinstance(val, datetime) else val

    if not anydesk_id:
        return jsonify({"error": "Missing anydesk_id"}), 400

    videos = []
    for v in data.get("videos", []):
        videos.append({
            "filename": v.get("filename"),
            "play_count": v.get("play_count", 0),
            "total_play_duration": v.get("total_play_duration", 0),
            "first_play": safe_datetime(v.get("first_play")),
            "last_play": safe_datetime(v.get("last_play"))
        })

    if anydesk_id not in kiosks:
        kiosks[anydesk_id] = {
            "first_seen": now,
            "uptime_seconds": 0,
            "software_version": software_version,
            "videos": videos,
            "last_seen": now
        }
    else:
        last_seen_raw = kiosks[anydesk_id].get("last_seen")
        last_seen = datetime.fromisoformat(last_seen_raw) if isinstance(last_seen_raw, str) else last_seen_raw
        delta = now - last_seen
        buffer_seconds = 300
        expected = delta.total_seconds()

        if expected < 3600:
            kiosks[anydesk_id]["expected_seconds"] = kiosks[anydesk_id].get("expected_seconds", 0) + expected
            if delta.total_seconds() <= buffer_seconds:
                kiosks[anydesk_id]["uptime_seconds"] = kiosks[anydesk_id].get("uptime_seconds", 0) + expected

        kiosks[anydesk_id]["last_seen"] = now
        kiosks[anydesk_id]["software_version"] = software_version
        kiosks[anydesk_id]["videos"] = videos

    save_kiosks()
    return jsonify({"status": "ok"}), 200

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
            
        info['uptime_percent'] = 100
        if info.get('expected_seconds', 0) > 0:
            uptime_ratio = info.get('uptime_seconds', 0) / info['expected_seconds']
            info['uptime_percent'] = round(uptime_ratio * 100, 2)

    return render_template('dashboard.html',
                           configured=configured,
                           unconfigured=unconfigured,
                           now=now,
                           heartbeat_timeout=HEARTBEAT_TIMEOUT,
                           is_admin=request.is_admin)


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


@app.route('/delete_pi/<anydesk_id>', methods=['POST'])
@require_auth
def delete_pi(anydesk_id):
    if anydesk_id in kiosks:
        kiosks.pop(anydesk_id)
        save_kiosks()
    if anydesk_id in kiosk_mappings:
        kiosk_mappings.pop(anydesk_id)
        save_mappings()
    return redirect('/')


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
            writer.writerow([
                mapping['country'],
                mapping['kiosk_name'],
                video['filename'],
                video.get('play_count', 0),
                video.get('total_play_duration', 0),
                video.get('first_play', 'None'),
                video.get('last_play', 'None')
            ])

    output.seek(0)
    return Response(output, mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=ChangeBox_Video_Stats.csv"})


if __name__ == '__main__':
    load_kiosks()
    load_mappings()
    app.run(host='0.0.0.0', port=5000)