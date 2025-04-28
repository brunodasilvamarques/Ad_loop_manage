from flask import Flask, request, redirect, jsonify, send_file, Response, render_template
from datetime import datetime
import threading
import json
import os
import time
from collections import defaultdict

app = Flask(__name__)

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
    with open(DATA_FILE, 'w') as f:
        json.dump(kiosks, f, indent=4)


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
    pi_id = data.get("pi_id")
    videos = data.get("videos", [])
    software_version = data.get("software_version", "Unknown")

    if not pi_id:
        return jsonify({"error": "Missing pi_id"}), 400

    kiosks[pi_id] = {
        "last_seen": datetime.utcnow().isoformat(),
        "software_version": software_version,
        "videos": videos
    }
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

    for pi_id, info in kiosks.items():
        mapping = kiosk_mappings.get(pi_id)
        if mapping:
            configured[mapping['country']].append((pi_id, mapping['kiosk_name'], info))
        else:
            unconfigured.append((pi_id, info))

    return render_template('dashboard.html',
                           configured=configured,
                           unconfigured=unconfigured,
                           now=now,
                           heartbeat_timeout=HEARTBEAT_TIMEOUT,
                           is_admin=request.is_admin)


@app.route('/configure', methods=['POST'])
@require_auth
def configure_kiosk():
    pi_id = request.form.get('pi_id')
    kiosk_name = request.form.get('kiosk_name')
    country = request.form.get('country')

    if pi_id and kiosk_name and country:
        kiosk_mappings[pi_id] = {"kiosk_name": kiosk_name, "country": country}
        save_mappings()

    return redirect('/')


@app.route('/reset_video/<pi_id>/<video_name>', methods=['POST'])
@require_auth
def reset_video(pi_id, video_name):
    if pi_id in kiosks:
        videos = kiosks[pi_id].get('videos', [])
        for video in videos:
            if video['filename'] == video_name:
                video['play_count'] = 0
                video['total_play_duration'] = 0
                video['first_play'] = None
                video['last_play'] = None
        save_kiosks()
    return redirect('/')


@app.route('/delete_video/<pi_id>/<video_name>', methods=['POST'])
@require_auth
def delete_video(pi_id, video_name):
    if pi_id in kiosks:
        videos = kiosks[pi_id].get('videos', [])
        kiosks[pi_id]['videos'] = [v for v in videos if v['filename'] != video_name]
        save_kiosks()
    return redirect('/')


@app.route('/delete_pi/<pi_id>', methods=['POST'])
@require_auth
def delete_pi(pi_id):
    if pi_id in kiosks:
        kiosks.pop(pi_id)
        save_kiosks()
    if pi_id in kiosk_mappings:
        kiosk_mappings.pop(pi_id)
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

    for pi_id, info in kiosks.items():
        mapping = kiosk_mappings.get(pi_id, {"kiosk_name": pi_id, "country": "Unknown"})
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