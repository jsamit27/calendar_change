from __future__ import annotations
import os, json, uuid, datetime, re
from typing import Dict, Any
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]

SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")
SA_DELEGATE = os.getenv("SA_DELEGATE", "")  # only for Workspace DWD; else leave empty
# Either set CALENDAR_IDS="cal1,cal2,cal3" OR CALENDAR_ID="single"
CALENDAR_IDS = [c.strip() for c in os.getenv("CALENDAR_IDS", "").split(",") if c.strip()]
if not CALENDAR_IDS:
    single = os.getenv("CALENDAR_ID", "").strip()
    CALENDAR_IDS = [single] if single else []
WEBHOOK_ADDRESS = os.environ.get("WEBHOOK_ADDRESS")  # https://<public>/calendar/push

CHANNELS_FILE = "channels.json"        # channel_id -> {calendar_id, resource_id, expiration}
STATE_DIR = "calendar_states"          # state per calendar (syncToken + etags)

app = FastAPI()

@app.get("/")
async def root():
    return {"ok": True, "msg": "SA Calendar push listener (multi-calendar) running"}

# ---------- helpers ----------
def get_service():
    if SA_DELEGATE:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES, subject=SA_DELEGATE
        )
    else:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
    return build("calendar", "v3", credentials=creds)

def ensure_dirs():
    os.makedirs(STATE_DIR, exist_ok=True)

def safe_name(s: str) -> str:
    # turn an email/calendar id into a safe filename
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)

def state_path(calendar_id: str) -> str:
    return os.path.join(STATE_DIR, f"state_{safe_name(calendar_id)}.json")

def load_json(path: str, default):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, obj):
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)

def load_channels() -> Dict[str, Any]:
    return load_json(CHANNELS_FILE, {})

def save_channels(ch: Dict[str, Any]):
    save_json(CHANNELS_FILE, ch)

def describe(ev: dict) -> str:
    start = ev["start"].get("dateTime", ev["start"].get("date"))
    end   = ev["end"].get("dateTime", ev["end"].get("date"))
    return f"{ev.get('summary','(no title)')} [{start} → {end}]"

def ensure_baseline(service, calendar_id: str, state: dict):
    if state.get("syncToken"):
        return
    now = datetime.datetime.utcnow().isoformat() + "Z"
    events_map = {}
    page = None
    while True:
        res = service.events().list(
            calendarId=calendar_id,
            timeMin=now,
            singleEvents=True,
            showDeleted=True,
            orderBy="startTime",
            maxResults=2500,
            pageToken=page,
        ).execute()
        for ev in res.get("items", []):
            if ev.get("status") != "cancelled":
                events_map[ev["id"]] = ev.get("etag")
        page = res.get("nextPageToken")
        if not page:
            state["events"] = events_map
            state["syncToken"] = res.get("nextSyncToken")
            return  # caller persists

# ---------- routes ----------
@app.post("/init_watch")
async def init_watch():
    if not WEBHOOK_ADDRESS:
        return JSONResponse({"ok": False, "error": "Set WEBHOOK_ADDRESS env var"}, status_code=400)
    if not CALENDAR_IDS:
        return JSONResponse({"ok": False, "error": "Set CALENDAR_ID or CALENDAR_IDS env var"}, status_code=400)

    ensure_dirs()
    service = get_service()
    channels = load_channels()

    started = []
    for cal_id in CALENDAR_IDS:
        # 1) ensure baseline & state file per calendar
        spath = state_path(cal_id)
        state = load_json(spath, {"events": {}, "syncToken": None})
        ensure_baseline(service, cal_id, state)
        save_json(spath, state)

        # 2) start a watch channel for this calendar
        channel_id = str(uuid.uuid4())
        body = {
            "id": channel_id,
            "type": "web_hook",
            "address": WEBHOOK_ADDRESS,  # must end with /calendar/push
        }
        watch = service.events().watch(calendarId=cal_id, body=body).execute()

        info = {
            "calendar_id": cal_id,
            "resource_id": watch["resourceId"],
            "expiration": watch.get("expiration"),
        }
        channels[channel_id] = info
        started.append({"channel_id": channel_id, **info})

    save_channels(channels)
    return {"ok": True, "started": started}

@app.post("/calendar/push")
async def calendar_push(request: Request):
    import requests  # local import is fine for small scripts

    # Identify which channel (and thus which calendar) this push is for
    channel_id = request.headers.get("X-Goog-Channel-ID")
    resource_state = request.headers.get("X-Goog-Resource-State")
    channels = load_channels()
    meta = channels.get(channel_id)

    if not meta:
        # Unknown channel; might be expired or not ours
        return Response(status_code=200)

    calendar_id = meta["calendar_id"]
    service = get_service()

    # Load state for this calendar
    spath = state_path(calendar_id)
    state = load_json(spath, {"events": {}, "syncToken": None})
    if not state.get("syncToken"):
        ensure_baseline(service, calendar_id, state)
        save_json(spath, state)

    try:
        page = None
        total = 0
        while True:
            res = service.events().list(
                calendarId=calendar_id,
                syncToken=state["syncToken"],
                showDeleted=True,
                maxResults=2500,
                pageToken=page,
            ).execute()

            items = res.get("items", [])
            total += len(items)
            for ev in items:
                ev_id = ev["id"]
                if ev.get("status") == "cancelled":
                    if ev_id in state["events"]:
                        print(f"[{calendar_id}] 🔴 DELETED:", ev_id)
                        state["events"].pop(ev_id, None)
                        # --- call your external endpoint here ---
                        payload = {
                            "calendar_id": calendar_id,
                            "redis_url": "redis://default:675d88cdab9e49858ccd124a7e643914@fly-efficient-ai-1.upstash.io:6379"
                        }
                        headers = {
                            "Content-Type": "application/json",
                            "x-api-key": "https://cron-jobs-for-everyone.fly.dev/popuate-redis"
                        }
                        try:
                            r = requests.post("https://cron-jobs-for-everyone.fly.dev/populate-redis",
                                              json=payload, headers=headers, timeout=10)
                            print(f"POST → {r.status_code} | {r.text[:100]}")
                        except Exception as e:
                            print(f"POST failed: {e}")

                else:
                    prev = state["events"].get(ev_id)
                    if prev is None:
                        print(f"[{calendar_id}] 🟢 CREATED:", describe(ev))
                    elif prev != ev.get("etag"):
                        print(f"[{calendar_id}] 🟡 UPDATED:", describe(ev))
                    state["events"][ev_id] = ev.get("etag")

                    # --- same POST request for created/updated ---
                    payload = {
                        "calendar_id": calendar_id,
                        "redis_url": "redis://default:675d88cdab9e49858ccd124a7e643914@fly-efficient-ai-1.upstash.io:6379"
                    }
                    headers = {
                        "Content-Type": "application/json",
                        "x-api-key": "https://cron-jobs-for-everyone.fly.dev/popuate-redis"
                    }
                    try:
                        r = requests.post("https://cron-jobs-for-everyone.fly.dev/populate-redis",
                                          json=payload, headers=headers, timeout=10)
                        print(f"POST → {r.status_code} | {r.text[:100]}")
                    except Exception as e:
                        print(f"POST failed: {e}")

            page = res.get("nextPageToken")
            if not page:
                # persist new syncToken
                state["syncToken"] = res.get("nextSyncToken", state["syncToken"])
                save_json(spath, state)
                break

        if total == 0:
            print(f"[{calendar_id}] push '{resource_state}' with no diffs")

    except HttpError as e:
        if getattr(e, "resp", None) and e.resp.status == 410:
            print(f"[{calendar_id}] Sync token expired; reseeding baseline...")
            state["syncToken"] = None
            save_json(spath, state)
        else:
            print(f"[{calendar_id}] HTTP error in push handler:", e)

    return Response(status_code=200)


@app.post("/stop_watch")
async def stop_watch():
    """Stops all active channels we know about."""
    channels = load_channels()
    if not channels:
        return {"ok": False, "error": "no active channels"}

    service = get_service()
    stopped = []
    for ch_id, meta in list(channels.items()):
        try:
            service.channels().stop(body={
                "id": ch_id,
                "resourceId": meta["resource_id"],
            }).execute()
        except Exception as e:
            print("stop error:", e)
        stopped.append({"channel_id": ch_id, **meta})
        channels.pop(ch_id, None)

    save_channels(channels)
    return {"ok": True, "stopped": stopped}
