# Ad Video Dashboard

A Flask-based dashboard to monitor Raspberry Pi kiosks playing videos, tracking play counts, durations, and timestamps.

---

## Features
- Secure login (Admin and Read-Only roles)
- Live heartbeat and status (Running / Not Running)
- Rename kiosk and assign country manually
- Per-video Reset and Delete actions
- Per-Pi Delete action
- Export filtered or full data to CSV
- Clean dark UI with ChangeBox branding

---

## Folder Structure
```
project-root/
├── server.py
├── requirements.txt
├── render.yaml
├── templates/
│   └── dashboard.html
├── static/
│   ├── styles.css
│   └── changebox_logo.png
├── data/
│   ├── kiosks_data.json (auto-created)
│   └── kiosk_mappings.json (auto-created)
├── .env.template
├── README.md
```

---

## Setup Guide

### 1. Create a New GitHub Repo
- Create a **new public or private GitHub repo** (e.g., `pi-video-dashboard`)

### 2. Push Project to GitHub
```bash
cd your-project-directory

git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/pi-video-dashboard.git
git push -u origin main
```

### 3. Deploy to Render
- Log in to [Render.com](https://render.com)
- Click **"New Web Service"**
- Connect your GitHub repo
- Render will detect `render.yaml` automatically
- Set **Environment Variables**:
  - `ADMIN_USERNAME`
  - `ADMIN_PASSWORD`
  - `USER_USERNAME`
  - `USER_PASSWORD`
- Click **Deploy**

### 4. Raspberry Pi Heartbeat Configuration
- Set Pi to send heartbeat POST requests every 60 seconds to:
```
https://your-dashboard-url.onrender.com/api/heartbeat
```

---

## Notes
- If a Pi is offline for more than 5 minutes, it shows "Not Running".
- Unconfigured kiosks are shown at the top until renamed and assigned to a country.
- CSV exports include kiosk name, country, video stats, first/last play times.

---

Enjoy your new ChangeBox Kiosk Video Dashboard! 🚀
