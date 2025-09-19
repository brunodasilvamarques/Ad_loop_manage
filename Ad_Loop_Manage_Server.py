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
import socket # NEW: for single-process leader lock

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
_jobs_mutex = threading.Lock()   # NEW: thread-safe “start once” guard
_leader_sock = None             # NEW: holds the leader’s port lock

master_summary_cache = {}
kiosks = {}
kiosk_mappings = {}  # AnyDesk ID -> Friendly Name and Country
# ISO → friendly country label used in dashboard/CSVs
ISO_TO_NAME = {
    "GBP": "United Kingdom",
    "USD": "United States of America",
    "EUR": "Europe",
}

# ---- normalization helpers (kept, with safer rules) --------------------------
COUNTRY_ALIASES = {
    # UK
    "uk": "GBP", "gb": "GBP", "gbr": "GBP", "united kingdom": "GBP", "england": "GBP", "gbp": "GBP",
    # USA
    "us": "USD", "usa": "USD", "united states": "USD", "united states of america": "USD", "usd": "USD",
    # Europe / Eurozone
    "eu": "EUR", "europe": "EUR", "eurozone": "EUR", "eur": "EUR",
}
def _norm_text(s):
    if s is None:
        return None
    s = str(s).strip()
    # FIX: treat empty/placeholder values as None and collapse whitespace
    if s == "" or s.lower() in {"none", "n/a", "na", "-"}:
        return None
    return " ".join(s.split())

def _canon_country(v):
    if v is None:
        return None
    key = _norm_text(v)
    if key is None:
        return None
    # prefer ISO code we understand
    return COUNTRY_ALIASES.get(key.casefold(), key.upper())

def _canonical_identity(name, country, code, address):
    """Return normalized tuple used for compare/store + fingerprint string."""
    nm = _norm_text(name)
    cc = _canon_country(country)
    cd = _norm_text(code)
    if cd is not None:
        cd = cd.upper()  # FIX: ensure kiosk codes compare case-insensitively
    ad = _norm_text(address)
    fp = f"{nm}|{cc}|{cd}|{ad}"
    return nm, cc, cd, ad, fp
# -----------------------------------------------------------------------------

# FIX: derive a stable, unique device id when client sends a placeholder
_PLACEHOLDER_IDS = {"raspberrypi", "pi", "localhost", "127.0.0.1", "unknown", "device", "default", "changeme"}
def _normalize_device_id(anydesk_id, kiosk_code=None):
    aid = (str(anydesk_id).strip() if anydesk_id is not None else "")
    kc = _norm_text(kiosk_code)
    if aid == "" or aid.lower() in _PLACEHOLDER_IDS:
        if kc:
            return f"kc::{kc.upper()}"
        # fallthrough: keep placeholder (still unique per process), but we tried
    return aid
    
def _is_real_anydesk(aid: str) -> bool:
    """
    Return True only for genuine AnyDesk IDs (not placeholders and not kc:: aliases).
    """
    if not aid:
        return False
    s = str(aid).strip()
    if s.lower() in _PLACEHOLDER_IDS:
        return False
    return not s.startswith("kc::")

def _safe_load_json(path: str, default):
    """Load JSON or return default if file is missing/empty/corrupt."""
    try:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Failed to load JSON '{path}': {e}. Using default.")
        # Try to preserve the bad file once so we stop crashing on every request.
        try:
            bad_path = path + ".bad"
            if os.path.exists(path) and not os.path.exists(bad_path):
                os.rename(path, bad_path)
                print(f"↪️ Renamed corrupt file to {bad_path}")
        except Exception:
            pass
        return default

# ======== UTILS ========
def load_kiosks():
    global kiosks
    kiosks = _safe_load_json(DATA_FILE, {})

def load_mappings():
    global kiosk_mappings
    kiosk_mappings = _safe_load_json(MAPPINGS_FILE, {})

def save_kiosks():
    def convert(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(kiosks, f, indent=4, default=convert)

def save_mappings():
    with open(MAPPINGS_FILE, 'w', encoding='utf-8') as f:
        json.dump(kiosk_mappings, f, indent=4)

# ======== ROUTES ========

@app.route("/api/heartbeat", methods=["POST"])
def heartbeat():
    data = request.get_json()
    # FIX: protect against placeholder anydesk_id; derive from kiosk_code if needed
    anydesk_id_raw = data.get("anydesk_id")
    anydesk_id = _normalize_device_id(anydesk_id_raw, data.get("kiosk_code"))
    if not anydesk_id:
        return jsonify({"error": "Missing anydesk_id and kiosk_code"}), 400

    now = datetime.utcnow()
    # If the client sent kiosk_name/country/code/address, auto-map it
    kiosk_name = data.get("kiosk_name")
    country    = data.get("country")    # optional
    kiosk_code = data.get("kiosk_code") # optional
    kiosk_address = data.get("kiosk_address")  # optional

    if kiosk_name or kiosk_code or country or kiosk_address:
        existing   = kiosk_mappings.get(anydesk_id, {})
        prev_name  = existing.get('kiosk_name')
        prev_ctry  = existing.get('country')
        prev_code  = existing.get('kiosk_code')
        prev_addr  = existing.get('address')

        # ---- normalize both sides before comparing
        pn, pc, pcode, paddr, prev_fp = _canonical_identity(prev_name, prev_ctry, prev_code, prev_addr)
        nn, nc, ncode, naddr, new_fp  = _canonical_identity(
            kiosk_name if kiosk_name is not None else prev_name,
            country if country is not None else prev_ctry,
            kiosk_code if kiosk_code is not None else prev_code,
            kiosk_address if kiosk_address is not None else prev_addr
        )

        changed = (existing == {} or prev_fp != new_fp)
        if changed:
            kiosk_mappings[anydesk_id] = {
                "kiosk_name": nn,
                "country":    nc,
                "kiosk_code": ncode,
                "address":    naddr
            }
            save_mappings()

            # Only email if there *was* an existing identity and fingerprint changed,
            # and we didn't just send the exact same change already.
            if existing and prev_fp != new_fp:
                kiosks.setdefault(anydesk_id, {})
                kiosks[anydesk_id]["identity_changed_at"]    = now.isoformat()
                kiosks[anydesk_id]["suppress_offline_until"] = (now + timedelta(minutes=10)).isoformat()

                # de-dupe using last stored fingerprint
                last_fp = kiosks[anydesk_id].get("identity_fingerprint")
                kiosks[anydesk_id]["identity_fingerprint"] = new_fp
                save_kiosks()

                if last_fp != new_fp:
                    # Map ISO -> friendly labels for email
                    old_country_label = ISO_TO_NAME.get(pc, (pc or "None"))
                    new_country_label = ISO_TO_NAME.get(nc, nc)

                    subject = f"🆕 VLS Identity Updated | {new_country_label} - {nn}"
                    body = (
                        "The kiosk identity was updated.\n\n"
                        f"Kiosk ID: {anydesk_id}\n"
                        f"Old Name: {pn or 'None'} → New Name: {nn}\n"
                        f"Old Country: {old_country_label} → New Country: {new_country_label}\n"
                        f"Old Code: {pcode or 'None'} → New Code: {ncode or 'None'}\n"
                        f"Old Address: {paddr or 'None'} → New Address: {naddr or 'None'}\n"
                        f"Change time (UK local): {format_london(now)}\n"
                        f"Last heartbeat (UK local): {format_london(kiosks[anydesk_id].get('last_seen', now.isoformat()))}\n"
                    )
                    send_text_email(subject, body, [RECIPIENT_EMAIL])

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

    for anydesk_id in pick_unique_kiosk_ids():
        info = kiosks[anydesk_id]

        # Attach weekly uptime...
        try:
            weekly = compute_weekly_uptime_from_master(anydesk_id)
            info["weekly_uptime_percent"] = weekly["weekly_percent"]
        except Exception:
            info["weekly_uptime_percent"] = None

        ms = master_summary_cache.get(anydesk_id)
        if not ms:
            ms = summarize_videos_from_master(anydesk_id)

        # Build a set of filenames currently in the device's live playlist
        current_active = {
            (v.get("filename") or "")
            for v in kiosks.get(anydesk_id, {}).get("videos", [])
            if v.get("filename") and v.get("active", True)
        }

        # Build list and mark whether each video is active (in the current playlist)
        video_list = []
        for fn, rec in ms.items():
            item = {
                "filename": fn,
                "play_count": rec["play_count"],
                "total_play_duration": rec["total_duration"],
                "first_play": (rec["first_play"].replace(" ", "T") if rec.get("first_play") else None),
                "last_play":  (rec["last_play"].replace(" ", "T")  if rec.get("last_play")  else None),
                "active": fn in current_active,   # <-- NEW
            }
            video_list.append(item)

        # Order: active first (alphabetical), then inactive (alphabetical)
        info["videos"] = sorted(
            video_list,
            key=lambda x: (0 if x["active"] else 1, x["filename"].lower())
        )

        mapping = kiosk_mappings.get(anydesk_id)
        if mapping:
            country_code  = mapping.get('country', 'Unknown')
            country_label = ISO_TO_NAME.get(country_code, country_code)
            configured[country_label].append((anydesk_id, mapping['kiosk_name'], info))
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
        nn, nc, _, _, _ = _canonical_identity(kiosk_name, country, None, None)
        kiosk_mappings[anydesk_id] = {"kiosk_name": nn, "country": nc}
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
    
def summarize_videos_from_master(anydesk_id: str) -> dict:
    """
    Return { filename: {play_count, total_duration, first_play, last_play}, ... }
    by merging all 'video_summary' blocks across the kiosk's uploaded master_data files.
    """
    files = _find_master_files_for_kiosk(anydesk_id)
    out = {}
    for path in files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                j = json.load(f)
            for date_key, day in j.items():
                if not (isinstance(day, dict) and "video_summary" in day):
                    continue
                for fname, rec in day["video_summary"].items():
                    agg = out.setdefault(fname, {"play_count": 0, "total_duration": 0, "first_play": None, "last_play": None})
                    agg["play_count"] += int(rec.get("play_count", 0))
                    agg["total_duration"] += int(rec.get("total_duration", 0))
                    # first_play = earliest non-null
                    fp = rec.get("first_play")
                    if fp and (agg["first_play"] is None or fp < agg["first_play"]):
                        agg["first_play"] = fp
                    # last_play = latest non-null
                    lp = rec.get("last_play")
                    if lp and (agg["last_play"] is None or lp > agg["last_play"]):
                        agg["last_play"] = lp
        except Exception:
            continue
    return out
    
def compute_weekly_uptime_from_master(anydesk_id: str):
    """
    Weekly uptime over the last 7 calendar days in the kiosk's *local* time.
    Counts only ELAPSED hours for *today* (so future hours don't drag the % down).
    """
    files = _find_master_files_for_kiosk(anydesk_id)
    if not files:
        return {"weekly_percent": 0.0, "days": {}}

    # 1) Merge uptime_hours from master_data (same as before)
    by_date = {}
    for path in files:
        try:
            with open(path, "r") as f:
                j = json.load(f)
            for key, val in j.items():
                if key.count("-") == 2 and isinstance(val, dict):
                    by_date.setdefault(key, {})
                    uh = val.get("uptime_hours", {})
                    if isinstance(uh, dict):
                        by_date[key].update({k: 1 if int(v) == 1 else 0 for k, v in uh.items()})
        except Exception:
            continue

    # 2) Choose a local timezone (GBP -> Europe/London; else fall back to UTC)
    tzname = "UTC"
    try:
        iso = kiosk_mappings.get(anydesk_id, {}).get("country")
        if iso == "GBP":
            tzname = "Europe/London"
    except Exception:
        pass
    now_local = datetime.now(ZoneInfo(tzname))
    today_local = now_local.date()
    today_str   = today_local.strftime("%Y-%m-%d")

    # Last 7 local calendar days (oldest → newest)
    window = [(today_local - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(6, -1, -1)]

    days_out = {}
    counted_hours = 0
    up_hours = 0

    for d in window:
        hours_map = {}
        # Only count elapsed hours for *today*; all 24 for prior days.
        last_included_hour = 23 if d != today_str else (now_local.hour - 1)

        for h in range(24):
            key = f"{h:02d}:00"
            v = int(by_date.get(d, {}).get(key, 0))
            hours_map[key] = v

            if h <= last_included_hour:
                counted_hours += 1
                up_hours += v

        days_out[d] = hours_map

    percent = (up_hours / counted_hours) * 100 if counted_hours else 0.0
    return {"weekly_percent": round(percent, 2), "days": days_out}
    
def compute_all_uptime_from_master(anydesk_id: str) -> dict[str, dict[str, int]]:
    """
    Return every day we can find in uploaded master_data files for this kiosk:
      { "YYYY-MM-DD": {"00:00":0/1, ..., "23:00":0/1}, ... }
    No 7-day cap.
    """
    files = _find_master_files_for_kiosk(anydesk_id)
    all_days: dict[str, dict[str, int]] = {}
    for path in files:
        try:
            with open(path, "r") as f:
                j = json.load(f)
            for key, val in j.items():
                if key.count("-") == 2 and isinstance(val, dict):
                    hours = val.get("uptime_hours", {})
                    if isinstance(hours, dict):
                        d = all_days.setdefault(key, {})
                        for h, v in hours.items():
                            d[h] = 1 if int(v) == 1 else 0
        except Exception:
            continue
    # return days sorted by date ascending
    return {k: all_days[k] for k in sorted(all_days.keys())}
    
def pick_unique_kiosk_ids() -> list[str]:
    """
    Return one AnyDesk ID per kiosk.
    Prefer kiosk_code as the identity key; if missing, fall back to (country, kiosk_name).
    Keep the entry with the latest last_seen.
    """
    from datetime import datetime

    chosen: dict[str, str] = {}  # identity_key -> anydesk_id
    for aid, info in kiosks.items():
        m = kiosk_mappings.get(aid, {})
        identity_key = (
            m.get("kiosk_code")
            or f"{m.get('country','Unknown')}::{m.get('kiosk_name', aid)}"
        )

        prev_id = chosen.get(identity_key)
        if not prev_id:
            chosen[identity_key] = aid
            continue

        # keep the more recent one
        prev_seen = to_datetime_filter(kiosks.get(prev_id, {}).get("last_seen"))
        cur_seen  = to_datetime_filter(info.get("last_seen"))
        if cur_seen > prev_seen:
            chosen[identity_key] = aid

    return list(chosen.values())
    
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
    # only check one representative AnyDesk ID per kiosk identity (prevents dupes)
    for anydesk_id in pick_unique_kiosk_ids():
        info = kiosks.get(anydesk_id, {})

        # ⛔ Grace window after identity change
        suppress_until = info.get("suppress_offline_until")
        if suppress_until:
            try:
                if now < datetime.fromisoformat(suppress_until):
                    continue  # skip offline alerts during the grace period
            except Exception:
                pass

        try:
            last = datetime.fromisoformat(info.get("last_seen"))
        except Exception:
            continue

        delta = (now - last).total_seconds()
        was_sent = info.get("offline_alert_sent", False)

        if delta > HEARTBEAT_TIMEOUT and not was_sent:

            # Resolve friendly name + country (or fallback)
            mapping = kiosk_mappings.get(anydesk_id, {"kiosk_name": anydesk_id, "country": "Unknown", "kiosk_code": None})
            name    = mapping.get("kiosk_name", anydesk_id)
            iso     = mapping.get("country", "Unknown")
            country = ISO_TO_NAME.get(iso, iso)          # map GBP/USD/EUR to friendly name
            kcode   = mapping.get("kiosk_code") or "N/A"

            subject = f"⚠️ VLS Offline | Kiosk: {country} - {name}"

            # Only include Kiosk ID if it's a genuine AnyDesk ID (not kc:: alias or placeholder)
            aid_line = f"Kiosk ID: {anydesk_id}\n" if _is_real_anydesk(anydesk_id) else ""

            body = (
                "The Video Looper Software for the kiosk detailed below has gone offline.\n\n"
                f"Country: {country}\n"
                f"{aid_line}"
                f"Kiosk Code: {kcode}\n"
                f"Kiosk Name: {name}\n"
                f"Last heartbeat (UK local time): {format_london(info.get('last_seen'))}\n"
                f"Offline threshold: {HEARTBEAT_TIMEOUT}s "
                f"({HEARTBEAT_TIMEOUT // 60} minute{'s' if HEARTBEAT_TIMEOUT // 60 != 1 else ''})\n"
            )
            send_text_email(subject, body, [RECIPIENT_EMAIL])
            info["offline_alert_sent"] = True

    save_kiosks()

@app.route('/download_csv')
@require_auth
def download_csv():
    from io import StringIO, BytesIO
    import csv, threading, zipfile

    # ===== 1) Build Playback CSV (same as before) =====
    pb_out = StringIO()
    pb_writer = csv.writer(pb_out)
    pb_writer.writerow([
        "Country", "Kiosk Code", "Kiosk Name", "Address", "Video Name",
        "Play Count", "Total Duration (s)", "First Play", "Last Play"
    ])

    for anydesk_id in pick_unique_kiosk_ids():
        info    = kiosks[anydesk_id]
        mapping = kiosk_mappings.get(anydesk_id, {"kiosk_name": anydesk_id, "country": "Unknown"})
        summary = master_summary_cache.get(anydesk_id) or summarize_videos_from_master(anydesk_id)

        country_code  = mapping.get('country', 'Unknown')
        country_label = ISO_TO_NAME.get(country_code, country_code)
        kiosk_code    = mapping.get('kiosk_code') or "N/A"
        kiosk_name    = mapping.get('kiosk_name', anydesk_id)
        address       = mapping.get('address', "") or ""

        for filename, rec in (summary or {}).items():
            pb_writer.writerow([
                country_label,
                kiosk_code,
                kiosk_name,
                address,
                filename,
                int(rec.get('play_count', 0)),
                int(rec.get('total_duration', 0)),
                rec.get('first_play', 'None'),
                rec.get('last_play', 'None'),
            ])

    playback_text  = pb_out.getvalue()
    playback_bytes = playback_text.encode("utf-8")

    # ===== 2) Build Uptime CSV (Country, Kiosk Name, Kiosk Code, Date, Up Hours, 00:00..23:00) =====
    up_out = StringIO()
    up_writer = csv.writer(up_out)
    up_writer.writerow(["Country", "Kiosk Code", "Kiosk Name", "Address", "Date", "Up Hours"] + [f"{h:02d}:00" for h in range(24)])

    for anydesk_id in pick_unique_kiosk_ids():
        info    = kiosks[anydesk_id]
        mapping = kiosk_mappings.get(anydesk_id, {"kiosk_name": anydesk_id, "country": "Unknown", "kiosk_code": None})
        all_days = compute_all_uptime_from_master(anydesk_id)

        country_code  = mapping.get("country", "Unknown")
        country_label = ISO_TO_NAME.get(country_code, country_code)
        kiosk_name    = mapping.get("kiosk_name", anydesk_id)
        kiosk_code    = mapping.get("kiosk_code") or "N/A"
        address       = mapping.get("address","") or ""

        for date_key, hours_map in all_days.items():
            flags = [int(hours_map.get(f"{h:02d}:00", 0)) for h in range(24)]
            up_writer.writerow([country_label, kiosk_code, kiosk_name, address, date_key, sum(flags), *flags])

    uptime_text  = up_out.getvalue()
    uptime_bytes = uptime_text.encode("utf-8")

    # ===== 3) Email both CSVs as attachments =====
    try:
        threading.Thread(
            target=send_csv_email,
            args=("📊 Weekly ChangeBox Advertisement Video Playback CSV Report", playback_bytes, [RECIPIENT_EMAIL], uptime_bytes),
            daemon=True,
        ).start()
    except Exception as e:
        print(f"⚠️ Failed to queue CSV email on download: {e}")

    # ===== 4) Return both CSVs to the browser as a single ZIP =====
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("ChangeBox_Advertisement Video Playback_Combined Data.csv", playback_text)
        zf.writestr("ChangeBox_VideoLooper_Uptime_By_Day.csv", uptime_text)

    zip_buf.seek(0)
    return Response(
        zip_buf.getvalue(),
        mimetype="application/zip",
        headers={'Content-Disposition': 'attachment; filename="ChangeBox_VideoLooper_Reports.zip"'}
    )  

@app.route('/download_uptime_csv')
@require_auth
def download_uptime_csv():
    from io import StringIO
    import csv

    output = StringIO()
    writer = csv.writer(output)
    # Match the ZIP/email: Country, Kiosk Name, Address, Kiosk Code, Date, Up Hours, 00:00..23:00
    headers = ["Country", "Kiosk Code", "Kiosk Name", "Address", "Date", "Up Hours"] + [f"{h:02d}:00" for h in range(24)]
    writer.writerow(headers)

    for anydesk_id in pick_unique_kiosk_ids():
        mapping  = kiosk_mappings.get(anydesk_id, {"kiosk_name": anydesk_id, "country": "Unknown", "kiosk_code": None})
        all_days = compute_all_uptime_from_master(anydesk_id)

        country_code  = mapping.get("country", "Unknown")
        country_label = ISO_TO_NAME.get(country_code, country_code)
        kiosk_name    = mapping.get("kiosk_name", anydesk_id)
        kiosk_code    = mapping.get("kiosk_code") or "N/A"
        address       = mapping.get("address", "") or ""

        for date_key, hours_map in all_days.items():
            flags = [int(hours_map.get(f"{h:02d}:00", 0)) for h in range(24)]
            writer.writerow([country_label, kiosk_code, kiosk_name, address, date_key, sum(flags), *flags])

    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={'Content-Disposition': 'attachment; filename="ChangeBox_VideoLooper_Uptime_By_Day.csv"'}
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
        print("✅ Offline/Identity alert email sent.")
    else:
        print(f"❌ Failed to send alert: {resp.status_code} - {resp.text}")
        
def send_csv_email(subject, csv_bytes, recipients, uptime_csv_bytes=None):
    token = get_access_token()
    if not token:
        print("❌ Cannot send email, no access token.")
        return

    attachments = [
        {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": "ChangeBox_Advertisement Video Playback_Combined Data.csv",
            "contentType": "text/csv",
            "contentBytes": base64.b64encode(csv_bytes).decode("utf-8")
        }
    ]
    if uptime_csv_bytes:
        attachments.append({
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": "ChangeBox_VideoLooper_Uptime_By_Day.csv",
            "contentType": "text/csv",
            "contentBytes": base64.b64encode(uptime_csv_bytes).decode("utf-8")
        })

    email_data = {
        "message": {
            "subject": subject,
            "body": {"contentType": "Text", "content": "Attached is the combined CSV from all kiosks"},
            "toRecipients": [{"emailAddress": {"address": r}} for r in recipients],
            "attachments": attachments,
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
        print("✅ CSV email (both) sent.")
    else:
        print(f"❌ Failed to send CSV email: {resp.status_code} - {resp.text}")
        
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
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(json_payload, f, ensure_ascii=False, indent=2)
        print(f"✅ Uploaded master_data file: {save_path}")

        # Ensure this kiosk exists in memory (so dashboard/CSV show it even without heartbeat)
        try:
            from datetime import datetime
            device_id_raw = json_payload.get("device_id")
            # FIX: derive device id from kiosk_code if device_id is placeholder
            device_id = _normalize_device_id(device_id_raw, json_payload.get("kiosk_code"))
            sw_ver    = json_payload.get("software_version", "unknown")
            kname     = json_payload.get("kiosk_name")
            kcountry  = json_payload.get("country", "Unknown")
            kcode     = json_payload.get("kiosk_code")
            kaddr     = json_payload.get("address")
            now_iso   = datetime.utcnow().isoformat()

            if device_id:
                # Auto-map name/country/code if present — and detect changes
                existing   = kiosk_mappings.get(device_id, {})
                prev_name  = existing.get("kiosk_name")
                prev_ctry  = existing.get("country")
                prev_code  = existing.get("kiosk_code")
                prev_addr  = existing.get("address")

                pn, pc, pcode, paddr, prev_fp = _canonical_identity(prev_name, prev_ctry, prev_code, prev_addr)
                nn, nc, ncode, naddr, new_fp  = _canonical_identity(
                    kname if kname is not None else prev_name,
                    kcountry if kcountry is not None else prev_ctry,
                    kcode if kcode is not None else prev_code,
                    kaddr if kaddr is not None else prev_addr
                )

                changed = (existing == {} or prev_fp != new_fp)
                if changed:
                    kiosk_mappings[device_id] = {
                        "kiosk_name": nn,
                        "country":    nc,
                        "kiosk_code": ncode,
                        "address":    naddr
                    }
                    save_mappings()

                    if existing and prev_fp != new_fp:
                        kiosks.setdefault(device_id, {})
                        kiosks[device_id]["identity_changed_at"]    = now_iso
                        kiosks[device_id]["suppress_offline_until"] = (datetime.utcnow() + timedelta(minutes=10)).isoformat()

                        last_fp = kiosks[device_id].get("identity_fingerprint")
                        kiosks[device_id]["identity_fingerprint"] = new_fp
                        save_kiosks()

                        if last_fp != new_fp:
                            old_country_label = ISO_TO_NAME.get(pc, (pc or "None"))
                            new_country_label = ISO_TO_NAME.get(nc, nc)

                            subject = f"🆕 VLS Identity Updated | {new_country_label} - {nn}"
                            body = (
                                "The kiosk identity was updated (via upload).\n\n"
                                f"Kiosk ID: {device_id}\n"
                                f"Old Name: {pn or 'None'} → New Name: {nn}\n"
                                f"Old Country: {old_country_label} → New Country: {new_country_label}\n"
                                f"Old Code: {pcode or 'None'} → New Code: {ncode or 'None'}\n"
                                f"Old Address: {paddr or 'None'} → New Address: {naddr or 'None'}\n"
                                f"Change time (UK local): {format_london(now_iso)}\n"
                            )
                            send_text_email(subject, body, [RECIPIENT_EMAIL])

                # Create/refresh kiosk entry
                k = kiosks.get(device_id) or {
                    "first_seen": now_iso, "uptime_history": [], "uptime_percent": 0,
                    "video_stats": [], "configured": False, "offline_alert_sent": False
                }
                k["last_seen"] = now_iso
                k["software_version"] = sw_ver
                kiosks[device_id] = k
                save_kiosks()

                # Refresh this kiosk's summary cache now
                master_summary_cache[device_id] = summarize_videos_from_master(device_id)
        except Exception as e:
            print(f"⚠️ Failed to upsert kiosk on upload: {e}")

        return f"✅ Uploaded {uploaded_file.filename}", 200

    except Exception as e:
        print(f"❌ Failed to save uploaded JSON: {e}")
        return "❌ Server error saving file", 500
                   
def send_weekly_csv_email():
    # ===== Playback CSV =====
    pb_out = io.StringIO()
    pb_writer = csv.writer(pb_out)
    pb_writer.writerow(["Country", "Kiosk Code", "Kiosk Name", "Address", "Video Name",
                        "Play Count", "Total Play Duration (s)", "First Play", "Last Play"])

    for anydesk_id in pick_unique_kiosk_ids():
        info    = kiosks[anydesk_id]
        mapping = kiosk_mappings.get(anydesk_id, {"kiosk_name": anydesk_id, "country": "Unknown"})
        summary = master_summary_cache.get(anydesk_id) or summarize_videos_from_master(anydesk_id)

        country_code  = mapping.get('country', 'Unknown')
        country_label = ISO_TO_NAME.get(country_code, country_code)
        kiosk_code    = mapping.get('kiosk_code') or "N/A"
        kiosk_name    = mapping.get('kiosk_name', anydesk_id)
        address       = mapping.get('address', "") or ""

        for filename, rec in (summary or {}).items():
            pb_writer.writerow([
                country_label,
                kiosk_code,
                kiosk_name,
                address,
                filename,
                int(rec.get('play_count', 0)),
                int(rec.get('total_duration', 0)),
                rec.get('first_play', 'None'),
                rec.get('last_play', 'None')
            ])
    playback_bytes = pb_out.getvalue().encode("utf-8")

    # ===== Uptime CSV =====
    up_out = io.StringIO()
    up_writer = csv.writer(up_out)
    up_writer.writerow(["Country", "Kiosk Code", "Kiosk Name", "Address", "Date", "Up Hours"] + [f"{h:02d}:00" for h in range(24)])
    for anydesk_id in pick_unique_kiosk_ids():
        info    = kiosks[anydesk_id]
        mapping = kiosk_mappings.get(anydesk_id, {"kiosk_name": anydesk_id, "country": "Unknown", "kiosk_code": None})
        all_days = compute_all_uptime_from_master(anydesk_id)

        country_code  = mapping.get("country", "Unknown")
        country_label = ISO_TO_NAME.get(country_code, country_code)
        kiosk_name    = mapping.get("kiosk_name", anydesk_id)
        kiosk_code    = mapping.get("kiosk_code") or "N/A"
        address       = mapping.get("address","") or ""

        for date_key, hours_map in all_days.items():
            flags = [int(hours_map.get(f"{h:02d}:00", 0)) for h in range(24)]
            up_writer.writerow([country_label, kiosk_code, kiosk_name, address, date_key, sum(flags), *flags])
    uptime_bytes = up_out.getvalue().encode("utf-8")

    token = get_access_token()
    if not token:
        print("❌ Cannot send email, no access token.")
        return

    attachments = [
        {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": "ChangeBox_Advertisement Video Playback_Combined Data.csv",
            "contentType": "text/csv",
            "contentBytes": base64.b64encode(playback_bytes).decode("utf-8")
        },
        {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": "ChangeBox_VideoLooper_Uptime_By_Day.csv",
            "contentType": "text/csv",
            "contentBytes": base64.b64encode(uptime_bytes).decode("utf-8")
        }
    ]

    email_data = {
        "message": {
            "subject": "📊 Weekly ChangeBox Advertisement Video Playback CSV Report",
            "body": {"contentType": "Text", "content": "Attached is the combined CSV from all kiosks"},
            "toRecipients": [{"emailAddress": {"address": RECIPIENT_EMAIL}}],
            "attachments": attachments
        },
        "saveToSentItems": "true"
    }

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(
        f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail",
        headers=headers,
        json=email_data
    )
    if response.status_code == 202:
        print("✅ Weekly email (both CSVs) sent.")
    else:
        print(f"❌ Failed to send weekly email: {response.status_code} - {response.text}")

def schedule_email():
    """
    Schedule the next send for Tuesday 09:30 Europe/London, DST-aware.
    If it's already past Tuesday 09:30 this week, schedule next Tuesday.
    """
    tz = pytz.timezone("Europe/London")
    now = datetime.now(tz)

    # Today at 09:30 in London time
    today_0930 = now.replace(hour=9, minute=30, second=0, microsecond=0)

    # Tuesday is 1 (Mon=0, Tue=1, ... Sun=6)
    days_ahead = (1 - now.weekday()) % 7
    if days_ahead == 0 and now >= today_0930:
        # It's already past 09:30 on Tuesday → schedule next week
        days_ahead = 7

    run_time = today_0930 + timedelta(days=days_ahead)
    delay = (run_time - now).total_seconds()
    if delay <= 0:
        delay += 7 * 24 * 60 * 60

    print(f"⏰ Weekly CSV scheduled for {run_time.isoformat()} (in {int(delay)}s)")
    Timer(delay, email_task_wrapper).start()


def email_task_wrapper():
    """Send the weekly CSV, then schedule the next one."""
    send_weekly_csv_email()
    # Immediately schedule the next Tuesday 09:30 after sending
    schedule_email()

def schedule_uptime_checks():
    update_all_kiosk_uptime()
    check_offline_alerts()   # send alert if any kiosk is offline past threshold
    Timer(120, schedule_uptime_checks).start()
    
# ---- Start background jobs once, for both Gunicorn and python run ----
def refresh_master_summary_cache():
    global master_summary_cache
    cache = {}
    for anydesk_id in pick_unique_kiosk_ids():
        try:
            cache[anydesk_id] = summarize_videos_from_master(anydesk_id)
        except Exception:
            cache[anydesk_id] = {}
    master_summary_cache = cache
    Timer(3600, refresh_master_summary_cache).start()
    
def _i_am_leader() -> bool:
    """
    Elect a single scheduler leader by binding a localhost TCP port.
    Only one process can hold this lock; others skip starting timers.
    """
    global _leader_sock
    if _leader_sock:
        return True
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 47621))  # fixed, unused port for lock
        s.listen(1)
        _leader_sock = s
        print("👑 This process is the scheduler leader.")
        return True
    except OSError:
        print("🔁 Another process already holds the scheduler leader lock; skipping timers here.")
        return False
    
_background_jobs_started = False
def start_background_jobs_once():
    global _background_jobs_started
    # Thread-safe “once” guard
    with _jobs_mutex:
        if _background_jobs_started:
            return
        _background_jobs_started = True

    print("🔧 Starting background jobs: weekly email + offline monitor")
    # ensure data is loaded before jobs run
    load_kiosks()
    load_mappings()

    # Only the elected leader runs timers (prevents duplicate emails/alerts)
    if _i_am_leader():
        schedule_email()             # weekly Tuesday 09:30 (Europe/London)
        schedule_uptime_checks()     # 2-min offline monitor
        refresh_master_summary_cache()  # hourly cache refresh
    else:
        # Non-leaders do not start timers, but can still serve requests.
        pass

@app.before_request
def _kick_jobs():
    # Flask 3.x safe: this runs on first request; guard prevents duplicates
    start_background_jobs_once()

if __name__ == '__main__':
    start_background_jobs_once()
    app.run(host='0.0.0.0', port=5000)
