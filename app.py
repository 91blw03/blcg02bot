import os
import time
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)

# -----------------------------
# Env
# -----------------------------
TOKEN = os.getenv("TOKEN")          # required
CHAT_ID = os.getenv("CHAT_ID")      # required: @channelusername OR numeric id
TZ = os.getenv("TZ", "Asia/Ho_Chi_Minh")  # Vietnam timezone
CONFIG_URL = os.getenv("CONFIG_URL")      # raw GitHub URL to config.json
PARSE_MODE = os.getenv("PARSE_MODE", "")  # optional: "HTML" or "Markdown"
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

# Optional: send "online" and test results to admin chat (your personal chat id)
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")  # optional
SELF_CHECK = os.getenv("SELF_CHECK", "1") == "1"  # default ON

# Polling for /test command
ENABLE_COMMANDS = os.getenv("ENABLE_COMMANDS", "1") == "1"  # default ON

if not TOKEN or not CHAT_ID:
    raise RuntimeError("Missing env vars: TOKEN and CHAT_ID are required.")

API_BASE = f"https://api.telegram.org/bot{TOKEN}"

# -----------------------------
# Default config (fallback)
# -----------------------------
DEFAULT_CONFIG = {
    "messages": {
        "11:00": ["11点文案A", "11点文案B", "11点文案C"],
        "15:00": ["15点文案A", "15点文案B", "15点文案C"],
        "22:00": ["22点文案A", "22点文案B", "22点文案C"],
    },
    "images": {
        "left": "",   # file_id OR https URL
        "right": "",
    },
    "slot_image": {
        "11:00": "left",
        "15:00": "right",
        "22:00": "left",
    },
    "rotation": "round_robin",   # "round_robin" or "daily"
    "refresh_seconds": 120,      # config refresh cache
}

_cache = {"loaded_at": 0, "data": DEFAULT_CONFIG}

# round-robin state
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
    """Fetch config from CONFIG_URL (raw json). Cached for refresh_seconds."""
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
        cfg = r.json()

        merged = DEFAULT_CONFIG.copy()
        merged.update(cfg or {})
        merged["messages"] = {**DEFAULT_CONFIG["messages"], **(cfg.get("messages") or {})}
        merged["images"] = {**DEFAULT_CONFIG["images"], **(cfg.get("images") or {})}
        merged["slot_image"] = {**DEFAULT_CONFIG["slot_image"], **(cfg.get("slot_image") or {})}

        _cache = {"loaded_at": now, "data": merged}
        logging.info("Config refreshed from CONFIG_URL")
        return merged
    except Exception as e:
        logging.warning("Config refresh failed, using cached config. err=%s", e)
        return data


def _post(url: str, payload: dict) -> bool:
    """POST with retries. Returns True if success."""
    if DRY_RUN:
        logging.info("[DRY_RUN] POST %s payload_keys=%s", url, list(payload.keys()))
        if "text" in payload:
            logging.info("[DRY_RUN] text:\n%s", payload["text"])
        if "caption" in payload:
            logging.info("[DRY_RUN] caption:\n%s", payload["caption"])
        if "photo" in payload:
            logging.info("[DRY_RUN] photo=%s", payload["photo"])
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
    """
    photo_ref can be:
      - Telegram file_id
      - http(s) URL to image
    """
    if not photo_ref:
        logging.info("No photo configured, skip send_photo.")
        return True

    url = f"{API_BASE}/sendPhoto"
    payload = {
        "chat_id": chat_id or CHAT_ID,
        "photo": photo_ref,
    }
    return _post(url, payload)


def pick_message(cfg: dict, slot_hhmm: str) -> str:
    messages = (cfg.get("messages") or {}).get(slot_hhmm) or []
    if not messages:
        return ""

    rotation = (cfg.get("rotation") or "round_robin").lower()
    tzinfo = ZoneInfo(TZ)
    now = datetime.now(tzinfo)
    today_key = now.strftime("%Y-%m-%d")

    if rotation == "daily":
        day_of_year = int(now.strftime("%j"))
        slot_bias = sum(ord(c) for c in slot_hhmm) % 97
        idx = (day_of_year + slot_bias) % len(messages)
        return messages[idx]

    # round_robin
    if _state.get("last_day") != today_key:
        _state["last_day"] = today_key
        save_state()

    idx = int(_state.get("rr_index", 0)) % len(messages)
    _state["rr_index"] = int(_state.get("rr_index", 0)) + 1
    save_state()
    return messages[idx]


def send_slot(slot_hhmm: str, target_chat: str | None = None) -> tuple[bool, str]:
    """
    Sends: text first, then slot image.
    Returns (ok, message_summary)
    """
    cfg = fetch_config()

    text = pick_message(cfg, slot_hhmm)
    if not text:
        return False, f"slot {slot_hhmm}: no text configured"

    tzinfo = ZoneInfo(TZ)
    now = datetime.now(tzinfo).strftime("%Y-%m-%d %H:%M:%S")

    logging.info("Run slot=%s at %s TZ=%s", slot_hhmm, now, TZ)

    ok_text = send_text(text, chat_id=target_chat)
    if not ok_text:
        return False, f"slot {slot_hhmm}: send_text failed"

    slot_image_key = (cfg.get("slot_image") or {}).get(slot_hhmm, "")
    photo_ref = (cfg.get("images") or {}).get(slot_image_key, "")

    ok_photo = send_photo(photo_ref, chat_id=target_chat)
    if not ok_photo:
        return False, f"slot {slot_hhmm}: send_photo failed (key={slot_image_key})"

    return True, f"slot {slot_hhmm}: ok (photo_key={slot_image_key})"


def scheduled_job(slot_hhmm: str):
    ok, summary = send_slot(slot_hhmm, target_chat=None)
    if ok:
        logging.info(summary)
    else:
        logging.error(summary)


# -----------------------------
# Self-check (startup ping)
# -----------------------------
def self_check():
    if not (SELF_CHECK and ADMIN_CHAT_ID):
        logging.info("Self-check skipped (SELF_CHECK=%s ADMIN_CHAT_ID=%s)", SELF_CHECK, bool(ADMIN_CHAT_ID))
        return

    tzinfo = ZoneInfo(TZ)
    now = datetime.now(tzinfo).strftime("%Y-%m-%d %H:%M:%S")
    cfg = fetch_config()
    msg = (
        "✅ Bot online\n"
        f"TZ: {TZ}\n"
        f"Schedule: 11:00 / 15:00 / 22:00\n"
        f"Rotation: {cfg.get('rotation')}\n"
        f"Time now: {now}\n"
    )
    send_text(msg, chat_id=ADMIN_CHAT_ID)


# -----------------------------
# Commands via long polling
# -----------------------------
def is_admin_user(update: dict) -> bool:
    """Allow /test only from ADMIN_CHAT_ID if provided; otherwise allow anyone (not recommended)."""
    if not ADMIN_CHAT_ID:
        return True
    try:
        from_id = str(update["message"]["from"]["id"])
        return from_id == str(ADMIN_CHAT_ID)
    except Exception:
        return False


def handle_command(text: str) -> tuple[str, str] | None:
    """
    Supported:
      /test 11  -> trigger slot 11:00 to channel
      /test 15  -> trigger slot 15:00 to channel
      /test 22  -> trigger slot 22:00 to channel
      /test me 11 -> trigger slot 11:00 to admin chat (preview)
    """
    parts = (text or "").strip().split()
    if not parts:
        return None
    if parts[0] != "/test":
        return None

    # defaults
    target = "channel"
    slot = ""

    if len(parts) == 2:
        slot = parts[1]
    elif len(parts) == 3 and parts[1].lower() == "me":
        target = "me"
        slot = parts[2]
    else:
        return None

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
                if target == "usage":
                    # reply usage to admin chat or sender
                    chat_id = str(msg.get("chat", {}).get("id")) or ADMIN_CHAT_ID or CHAT_ID
                    send_text(slot_hhmm, chat_id=chat_id)
                    continue

                if target == "me":
                    if not ADMIN_CHAT_ID:
                        chat_id = str(msg.get("chat", {}).get("id"))
                    else:
                        chat_id = ADMIN_CHAT_ID
                    ok, summary = send_slot(slot_hhmm, target_chat=chat_id)
                    send_text(("✅ " if ok else "❌ ") + summary, chat_id=chat_id)
                else:
                    ok, summary = send_slot(slot_hhmm, target_chat=None)
                    # reply result to admin
                    if ADMIN_CHAT_ID:
                        send_text(("✅ " if ok else "❌ ") + summary, chat_id=ADMIN_CHAT_ID)

        except Exception as e:
            logging.warning("poll_commands loop error: %s", e)
            time.sleep(3)


def main():
    load_state()

    tzinfo = ZoneInfo(TZ)
    scheduler = BackgroundScheduler(timezone=tzinfo)

    # Vietnam time daily: 11:00 / 15:00 / 22:00
    for slot in ("11:00", "15:00", "22:00"):
        hour, minute = slot.split(":")
        trigger = CronTrigger(hour=int(hour), minute=int(minute), timezone=tzinfo)
        scheduler.add_job(lambda s=slot: scheduled_job(s), trigger=trigger, id=f"slot_{slot}", replace_existing=True)

    scheduler.start()
    logging.info("Scheduler started. TZ=%s CHAT_ID=%s", TZ, CHAT_ID)

    # startup self-check ping
    self_check()

    # command polling (runs in main thread)
    poll_commands()


if __name__ == "__main__":
    main()
