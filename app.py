import os
import time
import json
import logging
import random
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TZ = os.getenv("TZ", "Asia/Ho_Chi_Minh")
CONFIG_URL = os.getenv("CONFIG_URL")
PARSE_MODE = os.getenv("PARSE_MODE", "")  # "HTML" recommended
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
SELF_CHECK = os.getenv("SELF_CHECK", "1") == "1"
ENABLE_COMMANDS = os.getenv("ENABLE_COMMANDS", "1") == "1"

if not TOKEN or not CHAT_ID:
    raise RuntimeError("Missing env vars: TOKEN and CHAT_ID are required.")

API_BASE = f"https://api.telegram.org/bot{TOKEN}"

DEFAULT_CONFIG = {
    "timezone": "Asia/Ho_Chi_Minh",
    "refresh_seconds": 120,

    # jitter in seconds before sending on each slot (random within range)
    "jitter_seconds_min": 0,
    "jitter_seconds_max": 0,

    # images can be string or list of strings (file_id or https url)
    "images": {"left": "", "right": ""},
    "slot_image": {"11:00": "left", "15:00": "right", "22:00": "left"},

    # weekly schedule: mon..sun -> "11:00"/"15:00"/"22:00" -> [messages...]
    "weekly": {}
}

_cache = {"loaded_at": 0, "data": DEFAULT_CONFIG}

STATE_FILE = "state.json"
_state = {"rr_index": 0, "last_day": ""}


def load_state():
    global _state
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            _state = json.load(f)
    except Exception:
        _state = {"rr_index": 0, "last_day": ""}


def save_state():
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(_state, f, ensure_ascii=False)
    except Exception as e:
        logging.warning("Failed to save state: %s", e)


def fetch_config() -> dict:
    global _cache
    now = time.time()
    data = _cache["data"]
    refresh = int(data.get("refresh_seconds", DEFAULT_CONFIG["refresh_seconds"]))

    if not CONFIG_URL:
        return data

    if now - _cache["loaded_at"] < refresh:
        return data

    try:
        r = requests.get(CONFIG_URL, timeout=15)
        r.raise_for_status()
        cfg = r.json() or {}

        merged = DEFAULT_CONFIG.copy()
        merged.update(cfg)

        # deep merge keys we care about
        merged["images"] = {**DEFAULT_CONFIG["images"], **(cfg.get("images") or {})}
        merged["slot_image"] = {**DEFAULT_CONFIG["slot_image"], **(cfg.get("slot_image") or {})}
        merged["weekly"] = cfg.get("weekly") or {}

        _cache = {"loaded_at": now, "data": merged}
        logging.info("Config refreshed from CONFIG_URL")
        return merged
    except Exception as e:
        logging.warning("Config refresh failed, using cached config. err=%s", e)
        return data


def _post(url: str, payload: dict) -> bool:
    if DRY_RUN:
        logging.info("[DRY_RUN] POST %s keys=%s", url, list(payload.keys()))
        return True

    for attempt in range(1, 5):
        try:
            resp = requests.post(url, data=payload, timeout=20)
            if resp.status_code == 200:
                return True
            logging.warning("HTTP %s: %s", resp.status_code, resp.text[:400])
        except Exception as e:
            logging.warning("Request error attempt=%s err=%s", attempt, e)
        time.sleep(2 * attempt)
    return False


def send_text(text: str, chat_id: str | None = None) -> bool:
    url = f"{API_BASE}/sendMessage"
    payload = {
        "chat_id": chat_id or CHAT_ID,
        "text": text,
        "disable_web_page_preview": False,
    }
    if PARSE_MODE:
        payload["parse_mode"] = PARSE_MODE
    return _post(url, payload)


def send_photo(photo_ref: str, chat_id: str | None = None) -> bool:
    if not photo_ref:
        logging.info("No photo configured, skip send_photo.")
        return True
    url = f"{API_BASE}/sendPhoto"
    payload = {"chat_id": chat_id or CHAT_ID, "photo": photo_ref}
    return _post(url, payload)


def _weekday_key(dt: datetime) -> str:
    # dt.weekday(): Mon=0..Sun=6
    keys = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    return keys[dt.weekday()]


def _normalize_list(x):
    # images.left/right can be string or list
    if x is None:
        return []
    if isinstance(x, list):
        return [str(i) for i in x if str(i).strip()]
    s = str(x).strip()
    return [s] if s else []


def pick_weekly_message(cfg: dict, slot_hhmm: str) -> str:
    tzinfo = ZoneInfo(cfg.get("timezone", TZ))
    now = datetime.now(tzinfo)
    day_key = _weekday_key(now)

    day_obj = (cfg.get("weekly") or {}).get(day_key) or {}
    candidates = day_obj.get(slot_hhmm) or []

    if not candidates:
        return ""

    # round-robin across all sends (global), so it keeps changing even if some day only has 1 message
    today_key = now.strftime("%Y-%m-%d")
    if _state.get("last_day") != today_key:
        _state["last_day"] = today_key
        save_state()

    idx = int(_state.get("rr_index", 0)) % len(candidates)
    _state["rr_index"] = int(_state.get("rr_index", 0)) + 1
    save_state()
    return candidates[idx]


def pick_image(cfg: dict, slot_hhmm: str) -> str:
    slot_image_key = (cfg.get("slot_image") or {}).get(slot_hhmm, "")
    img_val = (cfg.get("images") or {}).get(slot_image_key, "")
    pool = _normalize_list(img_val)
    return random.choice(pool) if pool else ""


def maybe_jitter(cfg: dict):
    jmin = int(cfg.get("jitter_seconds_min", 0) or 0)
    jmax = int(cfg.get("jitter_seconds_max", 0) or 0)
    if jmax <= 0 or jmax < jmin:
        return
    delay = random.randint(jmin, jmax)
    if delay > 0:
        logging.info("Jitter delay %ss before send", delay)
        time.sleep(delay)


def send_slot(slot_hhmm: str, target_chat: str | None = None) -> tuple[bool, str]:
    cfg = fetch_config()

    # jitter before sending
    maybe_jitter(cfg)

    text = pick_weekly_message(cfg, slot_hhmm)
    if not text:
        return False, f"slot {slot_hhmm}: no text configured for today"

    tzinfo = ZoneInfo(cfg.get("timezone", TZ))
    now = datetime.now(tzinfo).strftime("%Y-%m-%d %H:%M:%S")
    logging.info("Run slot=%s at %s TZ=%s", slot_hhmm, now, cfg.get("timezone", TZ))

    ok_text = send_text(text, chat_id=target_chat)
    if not ok_text:
        return False, f"slot {slot_hhmm}: send_text failed"

    photo_ref = pick_image(cfg, slot_hhmm)
    ok_photo = send_photo(photo_ref, chat_id=target_chat)
    if not ok_photo:
        return False, f"slot {slot_hhmm}: send_photo failed"
    return True, f"slot {slot_hhmm}: ok"


def scheduled_job(slot_hhmm: str):
    ok, summary = send_slot(slot_hhmm, target_chat=None)
    if ok:
        logging.info(summary)
    else:
        logging.error(summary)


def self_check():
    if not (SELF_CHECK and ADMIN_CHAT_ID):
        logging.info("Self-check skipped.")
        return
    cfg = fetch_config()
    tzinfo = ZoneInfo(cfg.get("timezone", TZ))
    now = datetime.now(tzinfo).strftime("%Y-%m-%d %H:%M:%S")
    msg = (
        "✅ Bot online\n"
        f"TZ: {cfg.get('timezone', TZ)}\n"
        "Schedule: 11:00 / 15:00 / 22:00\n"
        f"Time now: {now}\n"
    )
    send_text(msg, chat_id=ADMIN_CHAT_ID)


def is_admin_user(update: dict) -> bool:
    if not ADMIN_CHAT_ID:
        return True
    try:
        from_id = str(update["message"]["from"]["id"])
        return from_id == str(ADMIN_CHAT_ID)
    except Exception:
        return False


def handle_command(text: str):
    parts = (text or "").strip().split()
    if not parts or parts[0] != "/test":
        return None

    target = "channel"
    slot = ""
    if len(parts) == 2:
        slot = parts[1]
    elif len(parts) == 3 and parts[1].lower() == "me":
        target = "me"
        slot = parts[2]
    else:
        return ("usage", "用法：/test 11 | /test 15 | /test 22 | /test me 11")

    slot_map = {"11": "11:00", "15": "15:00", "22": "22:00", "11:00": "11:00", "15:00": "15:00", "22:00": "22:00"}
    slot_hhmm = slot_map.get(slot)
    if not slot_hhmm:
        return ("usage", "用法：/test 11 | /test 15 | /test 22 | /test me 11")
    return (target, slot_hhmm)


def poll_commands():
    if not ENABLE_COMMANDS:
        logging.info("Command polling disabled.")
        return

    offset = 0
    url = f"{API_BASE}/getUpdates"
    logging.info("Command polling started.")

    while True:
        try:
            params = {"timeout": 30, "offset": offset}
            r = requests.get(url, params=params, timeout=40)
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                time.sleep(2)
                continue

            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                text = msg.get("text") or ""
                if not text.startswith("/"):
                    continue
                if not is_admin_user(upd):
                    continue

                parsed = handle_command(text)
                if not parsed:
                    continue

                target, slot_hhmm = parsed
                reply_chat = str(msg.get("chat", {}).get("id")) or ADMIN_CHAT_ID or CHAT_ID

                if target == "usage":
                    send_text(slot_hhmm, chat_id=reply_chat)
                    continue

                if target == "me":
                    ok, summary = send_slot(slot_hhmm, target_chat=ADMIN_CHAT_ID or reply_chat)
                    send_text(("✅ " if ok else "❌ ") + summary, chat_id=ADMIN_CHAT_ID or reply_chat)
                else:
                    ok, summary = send_slot(slot_hhmm, target_chat=None)
                    if ADMIN_CHAT_ID:
                        send_text(("✅ " if ok else "❌ ") + summary, chat_id=ADMIN_CHAT_ID)

        except Exception as e:
            logging.warning("poll_commands loop error: %s", e)
            time.sleep(3)


def main():
    load_state()
    cfg = fetch_config()
    tzinfo = ZoneInfo(cfg.get("timezone", TZ))

    scheduler = BackgroundScheduler(timezone=tzinfo)
    for slot in ("11:00", "15:00", "22:00"):
        hour, minute = slot.split(":")
        trigger = CronTrigger(hour=int(hour), minute=int(minute), timezone=tzinfo)
        scheduler.add_job(lambda s=slot: scheduled_job(s), trigger=trigger, id=f"slot_{slot}", replace_existing=True)

    scheduler.start()
    logging.info("Scheduler started. TZ=%s CHAT_ID=%s", cfg.get("timezone", TZ), CHAT_ID)
    self_check()
    poll_commands()


if __name__ == "__main__":
    main()
