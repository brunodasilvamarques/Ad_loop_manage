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

# Heartbeat is now sent once per hour, on the hour
HEARTBEAT_INTERVAL_SECONDS = 3600          # 1 hour
HEARTBEAT_TIMEOUT = 7200                   # 2 hours before kiosk is treated as offline

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
            "video_stats": [],
            "videos": [],
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
            "last_play": v.get("last_play"),
            "active": bool(v.get("active", True))
        })

    kiosk["videos"] = videos
    kiosk["heartbeat_interval"] = HEARTBEAT_INTERVAL_SECONDS
    kiosk["is_active"] = True

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

        ms = master_summary_cache.get(anydesk_id)
        if not ms:
            ms = summarize_videos_from_master(anydesk_id)

        heartbeat_videos = kiosks.get(anydesk_id, {}).get("videos", []) or []

        heartbeat_names = sorted({
            (v.get("filename") or "").strip()
            for v in heartbeat_videos
            if (v.get("filename") or "").strip()
        })

        current_active = {
            (v.get("filename") or "").strip()
            for v in heartbeat_videos
            if (v.get("filename") or "").strip() and v.get("active", True)
        }

        all_names = sorted(set(ms.keys()) | set(heartbeat_names), key=lambda x: x.lower())

        video_list = []
        for fn in all_names:
            rec = ms.get(fn, {})
            item = {
                "filename": fn,
                "play_count": int(rec.get("play_count", 0) or 0),
                "total_play_duration": int(rec.get("total_duration", 0) or 0),
                "first_play": (rec.get("first_play").replace(" ", "T") if rec.get("first_play") else None),
                "last_play":  (rec.get("last_play").replace(" ", "T") if rec.get("last_play") else None),
                "active": fn in current_active,
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
    
def _iter_daily_sections_from_payload(payload):
    """
    Supports both formats:

    1) Old yearly format:
       {
           "device_id": "...",
           "2026-03-16": {
               "uptime_hours": {...},
               "video_summary": {...}
           }
       }

    2) New flat daily format from latest video_looper:
       {
           "device_id": "...",
           "kiosk_name": "...",
           "date": "2026-03-16",
           "uptime_hours": {...},
           "video_summary": {...}
       }

    Returns a list of tuples:
        [(date_key, day_dict), ...]
    """
    out = []

    if not isinstance(payload, dict):
        return out

    # New flat daily format
    if isinstance(payload.get("uptime_hours"), dict) or isinstance(payload.get("video_summary"), dict):
        date_key = str(payload.get("date") or "").strip()
        if date_key:
            out.append((
                date_key,
                {
                    "uptime_hours": payload.get("uptime_hours") or {},
                    "video_summary": payload.get("video_summary") or {}
                }
            ))

    # Old yearly nested format
    for key, val in payload.items():
        if key.count("-") == 2 and isinstance(val, dict):
            out.append((key, val))

    return out

    
def _find_master_files_for_kiosk(anydesk_id: str) -> list[str]:
    """
    Return candidate data files for this kiosk.

    Supports:
      - old yearly master_data_YYYY.json files
      - new flat daily video_looper JSON files
    """
    paths = []
    try:
        mapping = kiosk_mappings.get(anydesk_id, {})
        name = (mapping.get("kiosk_name") or "").strip()
        code = (mapping.get("kiosk_code") or "").strip().upper()

        for fname in os.listdir("data"):
            if not fname.lower().endswith(".json"):
                continue
            if fname in {os.path.basename(DATA_FILE), os.path.basename(MAPPINGS_FILE)}:
                continue

            full = os.path.join("data", fname)

            try:
                with open(full, "r", encoding="utf-8") as f:
                    j = json.load(f)
            except Exception:
                continue

            if not isinstance(j, dict):
                continue

            # Accept either the old yearly file or the new flat daily file
            is_supported_shape = fname.startswith("master_data_") or bool(_iter_daily_sections_from_payload(j))
            if not is_supported_shape:
                continue

            j_name = (j.get("kiosk_name") or "").strip()
            j_code = (j.get("kiosk_code") or "").strip().upper()
            j_device = str(j.get("device_id") or "").strip()

            if code and j_code == code:
                paths.append(full)
            elif name and j_name == name:
                paths.append(full)
            elif j_device == anydesk_id or fname.startswith(anydesk_id + "_"):
                paths.append(full)

    except Exception:
        pass

    return sorted(set(paths))

    
def summarize_videos_from_master(anydesk_id: str) -> dict:
    """
    Return { filename: {play_count, total_duration, first_play, last_play}, ... }
    by merging all 'video_summary' blocks across all supported uploaded data files
    for this kiosk.
    """
    files = _find_master_files_for_kiosk(anydesk_id)
    out = {}

    for path in files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                j = json.load(f)

            for _, day in _iter_daily_sections_from_payload(j):
                video_summary = day.get("video_summary") or {}
                if not isinstance(video_summary, dict):
                    continue

                for fname, rec in video_summary.items():
                    agg = out.setdefault(
                        fname,
                        {"play_count": 0, "total_duration": 0, "first_play": None, "last_play": None}
                    )
                    agg["play_count"] += int(rec.get("play_count", 0))
                    agg["total_duration"] += int(rec.get("total_duration", 0))

                    fp = rec.get("first_play")
                    if fp and (agg["first_play"] is None or fp < agg["first_play"]):
                        agg["first_play"] = fp

                    lp = rec.get("last_play")
                    if lp and (agg["last_play"] is None or lp > agg["last_play"]):
                        agg["last_play"] = lp

        except Exception:
            continue

    return out

    
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
        print(f"✅ Uploaded video looper data file: {save_path}")

        # Ensure this kiosk exists in memory (so dashboard/CSV show it even without heartbeat)
        try:
            from datetime import datetime
            device_id_raw = json_payload.get("device_id")
            # FIX: derive device id from kiosk_code if device_id is placeholder
            device_id = _normalize_device_id(device_id_raw, json_payload.get("kiosk_code"))
            sw_ver    = json_payload.get("software_version", "unknown")
            kname     = json_payload.get("kiosk_name")
            kcountry  = json_payload.get("country") or json_payload.get("currency_iso") or "Unknown"
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
                    "first_seen": now_iso,
                    "video_stats": [],
                    "videos": [],
                    "configured": False,
                    "offline_alert_sent": False
                }
                k["last_seen"] = now_iso
                k["software_version"] = sw_ver

                available_media = json_payload.get("available_media") or []
                if isinstance(available_media, list):
                    k["videos"] = [
                        {
                            "filename": str(name).strip(),
                            "play_count": 0,
                            "total_play_duration": 0,
                            "first_play": None,
                            "last_play": None,
                            "active": True
                        }
                        for name in available_media
                        if str(name).strip()
                    ]

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

def schedule_uptime_checks():
    check_offline_alerts()   # send alert if any kiosk is offline past threshold
    Timer(300, schedule_uptime_checks).start()
    
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
