#!/usr/bin/env python3
from threading import Lock
import io
import os
import subprocess
import logging
import sys
import time
from logging.handlers import TimedRotatingFileHandler
import threading
import json
import requests
import socket
# 'schedule' is optional; if missing we skip hourly jobs instead of crashing.
try:
    import schedule  # type: ignore
except Exception:
    schedule = None

from datetime import datetime, timedelta

# ========== SOFTWARE VERSION ==========
SOFTWARE_VERSION = "v2.6.0"  # Change manually when you release updates

# Constants
MASTER_JSON_LOCK = Lock()
MOUNT_PATH = '/media/pi'
SUPPORTED_FORMATS = ('.mp4', '.avi', '.mkv', '.mov', '.flv')
LOG_FILE = '/home/pi/logs/video_looper.log'
BLACK_SCREEN_IMAGE = '/home/pi/black_screen_with_text.png'
BLACK_IMAGE_RESOLUTION = (1920, 1080)
TEXT_COLOR = "white"
TEXT_SIZE_STATIC = 30  # Adjusted text size
TEXT_POSITION_Y = 1030  # Moved text closer to the bottom
FONT_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

# Global variables
usb_mount_point = None
feh_message_displayed = None  # Tracks the currently displayed message

HEARTBEAT_INTERVAL = 60  # Seconds

# === JSON + URL bootstrap ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
ARCHIVE_DIR = os.path.join(DATA_DIR, "archived")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

URL_FILE = os.path.join(BASE_DIR, "serverURL.txt")
DEFAULT_URL = "https://changebox-videolooper-dashboard.onrender.com/"  # with trailing slash

# Ensure serverURL.txt exists
if not os.path.exists(URL_FILE):
    with open(URL_FILE, "w") as f:
        f.write(DEFAULT_URL)

with open(URL_FILE, "r") as f:
    base_url = f.read().strip().rstrip('/')  # normalize

# Endpoints
HEARTBEAT_URL = f"{base_url}/api/heartbeat"
UPLOAD_URL    = f"{base_url}/upload_json"

# === Bootstrap config files (created on first run) ===
EMAIL_RECIPIENTS_FILE = os.path.join(BASE_DIR, "email_recipients.txt")
if not os.path.exists(EMAIL_RECIPIENTS_FILE):
    with open(EMAIL_RECIPIENTS_FILE, "w") as f:
        f.write(
            "# Add additional email recipients below, one per line.\n"
            "# Example:\n"
            "# someone@example.com\n"
        )

EMAIL_TOKEN_FILE = os.path.join(BASE_DIR, "email_token.txt")
if not os.path.exists(EMAIL_TOKEN_FILE):
    with open(EMAIL_TOKEN_FILE, "w") as f:
        f.write("wJS8Q~Nbfs1cxhmuB1pQLk4cFB~l0X_KiYFBxbfE")

VIDEO_STATS_FILE = '/home/pi/video_stats.json'

def setup_logging():
    """
    Log to /home/pi/logs/video_looper.log.YYYY-MM-DD (today included),
    keep only the last 7 daily files, and mirror to console.
    NOTE: no explicit encoding is used when opening the log file.
    """
    from glob import glob

    base_dir = os.path.dirname(LOG_FILE)
    os.makedirs(base_dir, exist_ok=True)

    logger = logging.getLogger("VideoLooper")
    logger.setLevel(logging.DEBUG)

    class DailyFileHandler(logging.Handler):
        def __init__(self, base_dir: str, prefix: str, keep_days: int = 7):
            super().__init__()
            self.base_dir = base_dir
            self.prefix = prefix          # e.g. "video_looper.log"
            self.keep_days = keep_days
            self.current_date = None
            self.stream = None
            self._open_for_today(first_open=True)

        def _dated_name(self, date_str: str) -> str:
            # -> "/home/pi/logs/video_looper.log.2025-08-13"
            return os.path.join(self.base_dir, f"{self.prefix}.{date_str}")

        def _base_name(self) -> str:
            # -> "/home/pi/logs/video_looper.log"
            return os.path.join(self.base_dir, self.prefix)

        def _migrate_base_to_dated_if_needed(self, dated_path: str):
            """
            If an old un-suffixed log exists from a previous handler, move it
            to today's dated filename so you only see the dated variant.
            """
            base_path = self._base_name()
            try:
                if os.path.exists(base_path) and not os.path.exists(dated_path):
                    os.replace(base_path, dated_path)
            except Exception:
                pass

        def _open_for_today(self, first_open: bool = False):
            date_str = datetime.now().strftime("%Y-%m-%d")
            if self.current_date != date_str:
                # Roll file handle if date changed
                if self.stream:
                    try:
                        self.stream.close()
                    except Exception:
                        pass

                self.current_date = date_str
                fname = self._dated_name(date_str)

                if first_open:
                    # migrate old base file (no suffix) into today's dated file
                    self._migrate_base_to_dated_if_needed(fname)

                # open in append mode with default system encoding
                self.stream = open(fname, "a")

                self._cleanup_old()

        def _cleanup_old(self):
            # Keep only the last N daily files: video_looper.log.YYYY-MM-DD
            pattern = os.path.join(self.base_dir, f"{self.prefix}.*")
            files = sorted(glob(pattern))
            while len(files) > self.keep_days:
                old = files.pop(0)
                try:
                    os.remove(old)
                except Exception:
                    pass

        def emit(self, record):
            try:
                self._open_for_today()
                msg = self.format(record)
                self.stream.write(msg + "\n")
                self.stream.flush()
            except Exception:
                self.handleError(record)

        def close(self):
            try:
                if self.stream:
                    self.stream.close()
            finally:
                super().close()

    # File handler: today’s file has the date in the name
    file_handler = DailyFileHandler(base_dir, os.path.basename(LOG_FILE), keep_days=7)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    logger.addHandler(console_handler)

    logger.info("\n--- New Session ---\n")
    return logger

# Initialize logger FIRST so other functions can safely log
logger = setup_logging()

# === Kiosk identity (optional, from kiosk.json in the same folder) ===
def load_kiosk_identity():
    try:
        cfg_path = os.path.join(BASE_DIR, "kiosk.json")
        if os.path.exists(cfg_path):
            with open(cfg_path, "r") as f:
                cfg = json.load(f)
            # Prefer Name; fall back to Code if Name missing
            kiosk_name = cfg.get("KioskName") or cfg.get("KioskCode")
            # Country is optional; try DispenseIso then Country
            kiosk_country = cfg.get("DispenseIso") or cfg.get("Country")
            return kiosk_name, kiosk_country
    except Exception as e:
        logger.warning(f"Could not read kiosk.json: {e}")
    return None, None

KIOSK_NAME, KIOSK_COUNTRY = load_kiosk_identity()

# Track file mtime so we only re-read when kiosk.json changes
KIOSK_IDENTITY_MTIME = (
    os.path.getmtime(os.path.join(BASE_DIR, "kiosk.json"))
    if os.path.exists(os.path.join(BASE_DIR, "kiosk.json")) else None
)

def maybe_reload_kiosk_identity():
    """Reload kiosk.json if it appeared or changed; also patch master_data metadata."""
    global KIOSK_NAME, KIOSK_COUNTRY, KIOSK_IDENTITY_MTIME
    path = os.path.join(BASE_DIR, "kiosk.json")
    try:
        if not os.path.exists(path):
            return
        mtime = os.path.getmtime(path)
        if KIOSK_IDENTITY_MTIME is not None and mtime <= KIOSK_IDENTITY_MTIME:
            return  # unchanged

        with open(path, "r") as f:
            cfg = json.load(f)

        KIOSK_NAME = cfg.get("KioskName") or cfg.get("KioskCode")
        KIOSK_COUNTRY = cfg.get("DispenseIso") or cfg.get("Country")
        KIOSK_IDENTITY_MTIME = mtime
        logger.info(f"🔄 kiosk.json reloaded: name={KIOSK_NAME}, country={KIOSK_COUNTRY}")

        # Ensure existing master_data picks up identity if it was null
        ensure_master_json()
        sync_master_metadata()

    except Exception as e:
        logger.warning(f"⚠️ kiosk.json reload failed: {e}")

# Extract AnyDesk ID from installed config file or CLI
def get_anydesk_id():
    try:
        output = subprocess.check_output(["anydesk", "--get-id"]).decode().strip()
        return output
    except Exception as e:
        # Safe to log now because logger is initialized above
        logger.error(f"Failed to get AnyDesk ID: {e}")
        return socket.gethostname()  # Fallback to hostname

DEVICE_ID = get_anydesk_id()

def send_heartbeat():
    """Send heartbeat and playback stats to the dashboard server."""
    while True:
        maybe_reload_kiosk_identity()
        payload = {
            "anydesk_id": DEVICE_ID,
            "software_version": SOFTWARE_VERSION,
            "videos": []
        }
        # ✅ If kiosk.json exists, include it so the server can auto-map
        if KIOSK_NAME:
            payload["kiosk_name"] = KIOSK_NAME
        if KIOSK_COUNTRY:
            payload["country"] = KIOSK_COUNTRY
        
        for filename, data in video_stats.items():
            payload["videos"].append({
                "filename": filename,
                "play_count": data["play_count"],
                "total_play_duration": data["play_duration"],
                "first_play": datetime.strptime(data["first_play"], "%Y-%m-%d %H:%M:%S").isoformat() if data["first_play"] else None,
                "last_play": datetime.strptime(data["last_play"], "%Y-%m-%d %H:%M:%S").isoformat() if data["last_play"] else None,
            })
        
        try:
            response = requests.post(HEARTBEAT_URL, json=payload, timeout=10)
            logger.info(f"Heartbeat sent: {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to send heartbeat: {e}")
        
        time.sleep(HEARTBEAT_INTERVAL)
        
def load_video_stats():
    global video_stats
    if os.path.exists(VIDEO_STATS_FILE):
        try:
            with open(VIDEO_STATS_FILE, 'r') as f:
                video_stats = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load video stats: {e}")
            video_stats = {}
    else:
        video_stats = {}

def save_video_stats():
    try:
        with open(VIDEO_STATS_FILE, 'w') as f:
            json.dump(video_stats, f)
    except Exception as e:
        logger.error(f"Failed to save video stats: {e}")
        
# === Master JSON (yearly) helpers ===
def get_today_str():
    return datetime.now().strftime("%Y-%m-%d")

def get_yearly_master_json_path():
    year = datetime.now().strftime("%Y")
    return os.path.join(DATA_DIR, f"master_data_{year}.json")

def ensure_master_json():
    path = get_yearly_master_json_path()
    with MASTER_JSON_LOCK:
        if not os.path.exists(path):
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({
                    "device_id": DEVICE_ID,
                    "software_version": SOFTWARE_VERSION,
                    "kiosk_name": KIOSK_NAME or None,
                    "country": KIOSK_COUNTRY or None
                }, f, indent=4)
            os.replace(tmp, path)
            
def sync_master_metadata():
    """Ensure master_data root metadata matches current kiosk identity if it was null/empty."""
    path = get_yearly_master_json_path()
    if not os.path.exists(path):
        return
    try:
        with MASTER_JSON_LOCK:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            changed = False
            # Only patch when current file still has null/empty values and we now have real values
            if (not data.get("kiosk_name")) and KIOSK_NAME:
                data["kiosk_name"] = KIOSK_NAME
                changed = True
            if (not data.get("country")) and KIOSK_COUNTRY:
                data["country"] = KIOSK_COUNTRY
                changed = True
            if changed:
                tmp = path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
                os.replace(tmp, path)
                logger.info("📝 master_data metadata patched (kiosk name/country).")
    except Exception as e:
        logger.warning(f"⚠️ sync_master_metadata failed: {e}")

def update_master_data(date_str, section, payload):
    path = get_yearly_master_json_path()
    ensure_master_json()
    with MASTER_JSON_LOCK:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Ensure today's container exists (now with video_summary too)
        if date_str not in data:
            data[date_str] = {"video_plays": [], "uptime_hours": {}, "video_summary": {}}

        if section == "video_play":
            # keep the original human-readable play list
            plays = data[date_str].setdefault("video_plays", [])
            plays.append(f"{len(plays) + 1} - {payload['time']} - {payload['filename']}")

            # NEW: maintain a per-video daily summary
            summary = data[date_str].setdefault("video_summary", {})
            s = summary.setdefault(payload["filename"], {
                "play_count": 0,
                "total_duration": 0,
                "first_play": None,
                "last_play": None
            })
            s["play_count"] += 1
            s["total_duration"] += int(payload.get("duration", 0))
            # Only set first_play if not already set
            if payload.get("first_play") and not s["first_play"]:
                s["first_play"] = payload["first_play"]
            # Always update last_play to the most recent
            if payload.get("last_play"):
                s["last_play"] = payload["last_play"]

        elif section == "uptime":
            uh = data[date_str].setdefault("uptime_hours", {})
            uh[payload["hour"]] = int(payload.get("up", 1))

        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        os.replace(tmp, path)
        
# === Hourly push to server ===
def push_todays_jsons():
    maybe_reload_kiosk_identity()
    logger.info("⏰ Hourly push triggered via scheduler")
    current_file = get_yearly_master_json_path()
    try:
        # ✅ Make sure the master file exists even if no plays happened yet
        ensure_master_json()

        id_prefix = KIOSK_NAME or DEVICE_ID
        upload_name = f"{id_prefix}_{os.path.basename(current_file)}"
        # Read the file fully under the lock, then upload the bytes
        with MASTER_JSON_LOCK:
            with open(current_file, "rb") as f:
                data_bytes = f.read()

        r = requests.post(
            UPLOAD_URL,
            files={"file": (upload_name, data_bytes, "application/json")},
            timeout=10
        )
        if r.status_code == 200:
            logger.info(f"✅ Pushed {upload_name} successfully")
        else:
            logger.error(f"❌ Push failed for {upload_name}: {r.status_code} | {r.text}")
    except Exception as e:
        logger.error(f"❌ Error pushing master data: {e}")
        
def record_hourly_uptime():
    """Mark this hour as 'up' in the master_data JSON (1 means process was running at the hour mark)."""
    maybe_reload_kiosk_identity()
    try:
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        hour_key = now.strftime("%H:00")
        update_master_data(date_str, "uptime", {"hour": hour_key, "up": 1})
        logger.info(f"🟢 Uptime marked for {date_str} {hour_key}")
    except Exception as e:
        logger.error(f"❌ Failed to record hourly uptime: {e}")

def run_scheduler_loop():
    if schedule is None:
        logger.warning("⏳ 'schedule' module not installed; hourly jobs are disabled.")
        return
    while True:
        schedule.run_pending()
        time.sleep(30)
        
# === Midnight archive + retention ===
ARCHIVE_RETENTION_DAYS = 3  # set to 3 for testing if you want

def archive_yesterday_data():
    y = datetime.now() - timedelta(days=1)
    yearly_file = os.path.join(DATA_DIR, f"master_data_{y.strftime('%Y')}.json")
    if not os.path.exists(yearly_file):
        logger.info(f"⚠️ No yearly file for {y.year}, skipping archive.")
        return
    archive_name = os.path.join(ARCHIVE_DIR, f"master_data_{y.strftime('%d-%m-%Y')}.json")
    try:
        import shutil
        shutil.copy2(yearly_file, archive_name)
        logger.info(f"📦 Archived {os.path.basename(yearly_file)} → {os.path.basename(archive_name)}")
    except Exception as e:
        logger.error(f"❌ Archive failed: {e}")

def delete_old_files(base_path, days_old):
    cutoff = time.time() - days_old * 86400
    deleted = 0
    for root, _, files in os.walk(base_path):
        for f in files:
            p = os.path.join(root, f)
            try:
                if os.path.isfile(p) and os.path.getmtime(p) < cutoff:
                    os.remove(p); deleted += 1
            except Exception as e:
                logger.error(f"⚠️ Failed to delete {p}: {e}")
    if deleted:
        logger.info(f"🧹 Deleted {deleted} files older than {days_old} days from {base_path}")

def run_daily_cleanup():
    delete_old_files(ARCHIVE_DIR, ARCHIVE_RETENTION_DAYS)

def start_daily_cleanup_scheduler():
    def loop():
        # Run once at startup
        run_daily_cleanup()
        logger.info("🗓️ Daily scheduler armed. Waiting for midnight...")
        while True:
            now = datetime.now()
            next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            time.sleep((next_midnight - now).total_seconds())

            logger.info("🕛 Midnight — archive & cleanup.")
            try:
                archive_yesterday_data()
            except Exception as e:
                logger.error(f"❌ Archive job failed: {e}")
            try:
                run_daily_cleanup()
            except Exception as e:
                logger.error(f"❌ Cleanup failed: {e}")
    threading.Thread(target=loop, daemon=True).start()

def create_black_image_with_text(message) -> bool:
    """Create a black screen with a message. Returns True on success."""
    try:
        from PIL import Image, ImageDraw, ImageFont  # lazy import; won't crash if Pillow missing
    except Exception as e:
        logger.warning(f"Pillow not available ({e}); skipping black-screen image generation.")
        return False

    try:
        logger.info(f"Creating black screen image with text: {message}")
        img = Image.new('RGB', BLACK_IMAGE_RESOLUTION, color='black')
        draw = ImageDraw.Draw(img)
        try:
            font_static = ImageFont.truetype(FONT_PATH, TEXT_SIZE_STATIC)
        except IOError:
            logger.warning("Font not found. Falling back to default font.")
            font_static = ImageFont.load_default()
        text_width_static, _ = draw.textsize(message, font=font_static)
        draw.text(((BLACK_IMAGE_RESOLUTION[0] - text_width_static) // 2, TEXT_POSITION_Y),
                  message, font=font_static, fill=TEXT_COLOR)
        img.save(BLACK_SCREEN_IMAGE)
        return True
    except Exception as e:
        logger.error(f"Failed to create black image: {e}")
        return False

def is_feh_running():
    """Check if the 'feh' process is running."""
    try:
        output = subprocess.check_output(["pgrep", "feh"])
        return bool(output.strip())
    except subprocess.CalledProcessError:
        return False

def display_black_screen(message):
    """Ensure seamless message transitions without gaps."""
    global feh_message_displayed

    if feh_message_displayed == message and is_feh_running():
        logger.info(f"The message '{message}' is already displayed. Skipping unnecessary update.")
        return  # Avoid unnecessary updates to feh

    # First, generate the new image BEFORE starting feh
    if not create_black_image_with_text(message):
        logger.warning("Black-screen image not available; skipping feh launch.")
        return

    # Now, replace the current feh instance without a gap
    logger.info(f"Displaying black screen with message: {message}")
    try:
        subprocess.Popen(["feh", "--fullscreen", "--hide-pointer", "--auto-zoom", BLACK_SCREEN_IMAGE])
    except Exception as e:
        logger.error(f"Failed to start feh: {e}")
        return

    feh_message_displayed = message  # Track the current message

def stop_black_screen():
    """Stop feh only if it's running, but ensure smooth transitions."""
    global feh_message_displayed
    if is_feh_running():
        logger.info("Stopping black screen.")
        subprocess.call(["pkill", "feh"])
        time.sleep(0.2)  # Very short delay to ensure process stops smoothly
    feh_message_displayed = None  # Reset message tracker

def scan_usb_directory():
    """Scan the USB directory for video files."""
    global usb_mount_point
    logger.info(f"Scanning USB directory: {usb_mount_point}")
    if usb_mount_point:
        video_files = []
        for root, _, files in os.walk(usb_mount_point):
            for file in sorted(files):
                if file.endswith(SUPPORTED_FORMATS):
                    video_files.append(os.path.join(root, file))
        logger.info(f"Detected video files: {video_files}")
        return video_files
    logger.info("No USB mount point available.")
    return []

def check_usb():
    """Check if a USB device is connected and update the screen immediately."""
    global usb_mount_point
    logger.info("Checking for USB device...")

    for mount_point in os.listdir(MOUNT_PATH):  # Scan for mounted USBs
        full_path = os.path.join(MOUNT_PATH, mount_point)
        if os.path.ismount(full_path):
            if usb_mount_point != full_path:  # If USB is newly detected
                logger.info(f"USB detected at {full_path}")
                usb_mount_point = full_path
                display_black_screen("CHANGEBOX MEDIA IS PLAYING...")  # Immediately update the screen
            return

    # No USB detected
    if usb_mount_point is not None:  # Only update if USB was previously detected
        usb_mount_point = None
        logger.info("No USB detected.")
        display_black_screen("PLEASE INSERT CHANGEBOX MEDIA")  # Instantly update the screen

def play_videos():
    """Play all videos from the USB directory in a loop with minimal delay."""
    global usb_mount_point

    while True:
        check_usb()  # Ensure USB is still connected

        if not usb_mount_point:
            display_black_screen("PLEASE INSERT CHANGEBOX MEDIA")
            time.sleep(2)  # Reduced delay
            continue

        video_files = scan_usb_directory()
        if not video_files:
            display_black_screen("NO MEDIA FILES FOUND")
            time.sleep(2)  # Reduced delay
            continue

        # Kill any previous OMXPlayer instances before starting fresh playback
        subprocess.call(["pkill", "-9", "omxplayer"])
        time.sleep(0.5)  # Reduced from 1 second to 0.5 seconds

        # Ensure "CHANGEBOX MEDIA IS PLAYING..." is displayed before playing the first video
        if not is_feh_running():
            display_black_screen("CHANGEBOX MEDIA IS PLAYING...")

        # Play all videos in sequence with minimal gap
        for file in video_files:
            logger.info(f"Playing video: {file}")
            
            filename = os.path.basename(file)

            # Update stats before playing
            if filename not in video_stats:
                video_stats[filename] = {
                    "play_count": 0,
                    "play_duration": 0,
                    "first_play": None,
                    "last_play": None
                }

            video_stats[filename]["play_count"] += 1
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')

            # ✅ Also log to master_data for today's date
            now_str = datetime.now().strftime("%H:%M:%S")

            if not video_stats[filename]["first_play"]:
                video_stats[filename]["first_play"] = current_time

            video_stats[filename]["last_play"] = current_time

            # Get video duration using ffprobe
            video_duration = get_video_duration(file)  # ✅ First get real duration
            if video_duration == 0:
                logger.warning(f"Skipping {file}, unable to determine duration.")
                continue

            video_stats[filename]["play_duration"] += video_duration  # then add to running total
            save_video_stats()

            # NEW: also write a daily summary entry to master_data for accuracy on the server
            update_master_data(
                get_today_str(),
                "video_play",
                {
                    "filename": filename,
                    "time": now_str,
                    "duration": int(video_duration),
                    "first_play": video_stats[filename]["first_play"],
                    "last_play": video_stats[filename]["last_play"],
                }
            )

            command = ["omxplayer", "--no-osd", "--aspect-mode", "stretch", file]
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            start_time = time.time()  # Track when the video starts playing

            # Monitor USB changes during playback and ensure feh is restored before video ends
            while process.poll() is None:  # While the video is playing
                elapsed_time = time.time() - start_time
                time_remaining = video_duration - elapsed_time

                # 2 seconds before video ends, check if feh is still running
                if time_remaining <= 3 and not is_feh_running():
                    display_black_screen("CHANGEBOX MEDIA IS PLAYING...")

                check_usb()
                if not usb_mount_point:  # If USB was removed, break early
                    process.terminate()
                    subprocess.call(["pkill", "-9", "omxplayer"])  # Ensure all OMXPlayer instances are closed
                    display_black_screen("PLEASE INSERT CHANGEBOX MEDIA")
                    break
                time.sleep(0.5)  # Reduced check interval from 1s to 0.5s

        # Ensure OMXPlayer is fully stopped before restarting playback
        subprocess.call(["pkill", "-9", "omxplayer"])
        time.sleep(0.3)  # Further reduced delay

        # If USB is still present after playback, restart loop
        check_usb()
        if usb_mount_point:
            if not is_feh_running():  # Check if feh is running before displaying message
                display_black_screen("CHANGEBOX MEDIA IS PLAYING...")
        else:
            display_black_screen("PLEASE INSERT CHANGEBOX MEDIA")

        logger.info("Playback cycle completed. Restarting playlist...")

def get_video_duration(file_path):
    """Retrieve the duration of a video file in seconds using ffprobe."""
    try:
        output = subprocess.check_output(
            ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", file_path],
            stderr=subprocess.STDOUT
        )
        return float(output.strip())
    except Exception as e:
        logger.error(f"Failed to get video duration for {file_path}: {e}")
        return 0
        
def hourly_fallback_loop():
    """
    Fallback scheduler when 'schedule' is not installed:
    wake up exactly on each top-of-hour and do uptime+push.
    """
    while True:
        now = datetime.now()
        # seconds until next :00:00
        next_top = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        time.sleep((next_top - now).total_seconds())
        try:
            record_hourly_uptime()
        except Exception as e:
            logger.error(f"Fallback scheduler: uptime mark failed: {e}")
        try:
            push_todays_jsons()
        except Exception as e:
            logger.error(f"Fallback scheduler: push failed: {e}")

if __name__ == '__main__':
    load_video_stats()
    
    threading.Thread(target=send_heartbeat, daemon=True).start()  # Start heartbeat in background
    
    # 🔁 Every hour on the hour: mark uptime and push the JSON
    if schedule:
        schedule.every().hour.at(":00").do(record_hourly_uptime)
        schedule.every().hour.at(":00").do(push_todays_jsons)
        threading.Thread(target=run_scheduler_loop, daemon=True).start()
    else:
        logger.warning("⏳ 'schedule' module not installed; using fallback hourly loop.")
        threading.Thread(target=hourly_fallback_loop, daemon=True).start()
        
    # Ensure the yearly file exists and pick up kiosk identity if it was null
    ensure_master_json()
    sync_master_metadata()

    logger.info("🚀 Initial master_data push on startup…")
    # Mark the current hour as up on boot as well
    record_hourly_uptime()
    push_todays_jsons()
    
    # ✅ Start daily archive + cleanup
    start_daily_cleanup_scheduler()
    
    check_usb()
    if usb_mount_point:
        display_black_screen("CHANGEBOX MEDIA IS PLAYING...")
    else:
        display_black_screen("PLEASE INSERT CHANGEBOX MEDIA")
    
    play_videos()
