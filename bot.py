# -*- coding: utf-8 -*-
"""
NgânMiu.Store — BOT CHECK ĐƠN HÀNG SHOPEE + TRA MÃ VẬN ĐƠN SPX
✅ STEP 1 OPTIMIZATION: Cache Cookie + Batch Log + Timeout tối ưu
"""

import os
import re
import json
import time
import html
import traceback
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

import requests
from flask import Flask, request, jsonify

# =========================================================
# LOAD ENV
# =========================================================
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BOT_TOKEN  = (os.getenv("TELEGRAM_TOKEN") or "").strip()
SHEET_ID   = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
CREDS_JSON = (os.getenv("GOOGLE_SHEETS_CREDS_JSON") or "").strip()

if not BOT_TOKEN:
    raise Exception("TELEGRAM_TOKEN missing")
if not SHEET_ID:
    raise Exception("GOOGLE_SHEET_ID missing")
if not CREDS_JSON:
    raise Exception("GOOGLE_SHEETS_CREDS_JSON missing")

BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

# =========================================================
# 🔥 STEP 1 OPTIMIZATION CONFIG
# =========================================================
print("="*60)
print(" BOT OPTIMIZED - STEP 1: CACHE + BATCH + TIMEOUT")
print("="*60)

USE_PARALLEL = os.getenv("USE_PARALLEL", "true").lower() == "true"
CHECK_LIMIT = 3
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))

# ✅ FIX 1: GIẢM TIMEOUT (từ 8s/6s → 5s/4s)
TIMEOUT_LIST = 5    # Giảm từ 8s
TIMEOUT_DETAIL = 4  # Giảm từ 6s
TIMEOUT_RETRY = 1   # Số lần retry khi timeout

# ✅ FIX 2: CACHE COOKIE (mới)
CACHE_COOKIE_TTL = int(os.getenv("CACHE_COOKIE_TTL", "45"))  # 45 giây
order_cache = {}  # {cookie: {"data": [...], "time": timestamp}}

# ✅ FIX 3: BATCH LOG (mới)
LOG_BATCH_SIZE = int(os.getenv("LOG_BATCH_SIZE", "10"))     # Gom 10 dòng
LOG_BATCH_INTERVAL = int(os.getenv("LOG_BATCH_INTERVAL", "3"))  # Hoặc 3 giây
log_queue = Queue()

print(f"[PERF] Mode: {'✅ PARALLEL' if USE_PARALLEL else '⚠️ SEQUENTIAL'}")
print(f"[PERF] Timeout: list={TIMEOUT_LIST}s, detail={TIMEOUT_DETAIL}s, retry={TIMEOUT_RETRY}")
print(f"[PERF] ✅ Cache cookie: {CACHE_COOKIE_TTL}s")
print(f"[PERF] ✅ Batch log: {LOG_BATCH_SIZE} rows or {LOG_BATCH_INTERVAL}s")

# Payment Integration
BOT1_API_URL = os.getenv("BOT1_API_URL", "").strip()
if BOT1_API_URL:
    PRICE_CHECK_COOKIE = int(os.getenv("PRICE_CHECK_COOKIE", "10"))
    PRICE_CHECK_SPX = int(os.getenv("PRICE_CHECK_SPX", "10"))
    PRICE_CHECK_GHN = int(os.getenv("PRICE_CHECK_GHN", "10"))
    print(f"[PAYMENT] Active: {PRICE_CHECK_COOKIE}đ/check")
else:
    PRICE_CHECK_COOKIE = PRICE_CHECK_SPX = PRICE_CHECK_GHN = 0
    print("[PAYMENT] Disabled")

# User cache (giữ nguyên từ version trước)
CACHE_USERS_SECONDS = int(os.getenv("CACHE_USERS_SECONDS", "60"))
user_cache = {
    "data": None,
    "timestamp": 0
}
print(f"[PERF] ✅ Cache users: {CACHE_USERS_SECONDS}s")

print("="*60)

# =========================================================
# GOOGLE SHEET CONNECT
# =========================================================
import gspread
from oauth2client.service_account import ServiceAccountCredentials

GS_SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

creds = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(CREDS_JSON), GS_SCOPE
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)

# =========================================================
# SHEET CONFIG
# =========================================================
TAB_USERS       = "Thanh Toan"
TAB_LOGS_CHECK  = "LogsCheck"
TAB_LOGS_SPAM   = "LogsSpam"

COL_NOTE_INDEX  = 5

# =========================================================
# LIMIT CONFIG
# =========================================================
FREE_LIMIT_PER_DAY = 10
SPAM_LIMIT_PER_MIN = 20

BAND_1_HOURS = 1
BAND_2_HOURS = 24
BAND_3_DAYS  = 7

# =========================================================
# FLASK APP
# =========================================================
app = Flask(__name__)

# =========================================================
# RUNTIME CACHE
# =========================================================
spam_cache: Dict[str, Dict[str, int]] = {}

# =========================================================
# COMMON UTILS
# =========================================================
def now() -> datetime:
    return datetime.now()

def safe_text(v: Any, default: str = "") -> str:
    try:
        return str(v)
    except Exception:
        return default

def safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(str(v).replace(",", "").strip())
    except Exception:
        return default

# =========================================================
# 🔥 FIX 2: CACHE COOKIE FUNCTIONS
# =========================================================
def get_cached_orders(cookie: str):
    """Lấy kết quả đã cache theo cookie"""
    item = order_cache.get(cookie)
    if not item:
        return None
    
    # Kiểm tra TTL
    if time.time() - item["time"] > CACHE_COOKIE_TTL:
        # Cache hết hạn
        del order_cache[cookie]
        return None
    
    return item["data"]

def set_cached_orders(cookie: str, data):
    """Lưu kết quả vào cache"""
    order_cache[cookie] = {
        "data": data,
        "time": time.time()
    }

def clear_expired_cache():
    """Dọn cache cũ (chạy định kỳ)"""
    current_time = time.time()
    expired = [
        k for k, v in order_cache.items()
        if current_time - v["time"] > CACHE_COOKIE_TTL
    ]
    for k in expired:
        del order_cache[k]

# =========================================================
# 🔥 FIX 3: BATCH LOG WORKER
# =========================================================
def log_worker():
    """
    Worker thread xử lý batch ghi log
    Gom log → Ghi 1 lần khi:
    - Đủ LOG_BATCH_SIZE dòng
    - Hoặc sau LOG_BATCH_INTERVAL giây
    """
    buffer_check = []
    buffer_spam = []
    last_flush = time.time()
    
    print("[LOG] Batch log worker started")
    
    while True:
        try:
            # Lấy item từ queue (timeout 0.5s)
            item = log_queue.get(timeout=0.5)
            
            log_type = item.get("type")
            data = item.get("data")
            
            if log_type == "check":
                buffer_check.append(data)
            elif log_type == "spam":
                buffer_spam.append(data)
                
        except:
            # Timeout → Không có item mới
            pass
        
        # Kiểm tra điều kiện flush
        current_time = time.time()
        should_flush = (
            len(buffer_check) >= LOG_BATCH_SIZE or
            len(buffer_spam) >= LOG_BATCH_SIZE or
            (current_time - last_flush) >= LOG_BATCH_INTERVAL
        )
        
        if should_flush:
            # Flush buffer_check
            if buffer_check:
                try:
                    ws_log_check.append_rows(
                        buffer_check,
                        value_input_option="USER_ENTERED"
                    )
                    print(f"[LOG] Flushed {len(buffer_check)} check logs")
                except Exception as e:
                    print(f"[LOG] Error flushing check: {e}")
                buffer_check.clear()
            
            # Flush buffer_spam
            if buffer_spam:
                try:
                    ws_log_spam.append_rows(
                        buffer_spam,
                        value_input_option="USER_ENTERED"
                    )
                    print(f"[LOG] Flushed {len(buffer_spam)} spam logs")
                except Exception as e:
                    print(f"[LOG] Error flushing spam: {e}")
                buffer_spam.clear()
            
            last_flush = current_time

# =========================================================
# BOT 1 API INTEGRATION
# =========================================================
def check_balance_bot1(user_id: int) -> tuple:
    """Check user balance from Bot 1"""
    if not BOT1_API_URL:
        return True, 999999, ""
    
    try:
        response = requests.post(
            f"{BOT1_API_URL}/api/check_balance",
            json={"user_id": user_id},
            timeout=10
        )
        data = response.json()
        
        if response.status_code == 200 and data.get("success"):
            return True, data.get("balance", 0), ""
        else:
            return False, 0, data.get("error", "Unknown error")
    except Exception as e:
        return False, 0, str(e)

def deduct_balance_bot1(user_id: int, amount: int, reason: str, username: str = "") -> tuple:
    """Deduct money from Bot 1"""
    if not BOT1_API_URL:
        return True, 999999, ""
    
    try:
        response = requests.post(
            f"{BOT1_API_URL}/api/deduct",
            json={
                "user_id": user_id,
                "amount": amount,
                "reason": reason,
                "username": username
            },
            timeout=10
        )
        data = response.json()
        
        if response.status_code == 200 and data.get("success"):
            return True, data.get("new_balance", 0), ""
        else:
            return False, data.get("balance", 0), data.get("error", "Unknown error")
    except Exception as e:
        return False, 0, str(e)

def format_insufficient_balance_msg(balance: int, required: int) -> str:
    """Format insufficient balance message"""
    return (
        f"❌ <b>KHÔNG ĐỦ TIỀN</b>\n\n"
        f"💰 <b>Cần:</b> {required:,}đ\n"
        f"💰 <b>Có:</b> {balance:,}đ\n"
        f"💰 <b>Thiếu:</b> {(required - balance):,}đ\n\n"
        f"👉 Vui lòng nạp thêm tiền tại:\n"
        f"@nganmiu_bot (Bot ADD Voucher Shopee)"
    )

def check_shopee_orders_with_payment(cookie: str, user_id: int, username: str = "") -> tuple:
    """Check Shopee orders with auto payment"""
    if BOT1_API_URL:
        success, balance, error = check_balance_bot1(user_id)
        
        if not success:
            return False, f"⚠️ Lỗi hệ thống: {error}", 0
        
        if balance < PRICE_CHECK_COOKIE:
            msg = format_insufficient_balance_msg(balance, PRICE_CHECK_COOKIE)
            return False, msg, balance
    else:
        balance = 0
    
    result_html, result_text = check_shopee_orders(cookie)
    
    if not result_html or result_text:
        error_msg = "❌ Check cookie thất bại"
        if result_text == "cookie_expired":
            error_msg = "❌ Cookie hết hạn hoặc không hợp lệ"
        elif result_text == "no_orders":
            error_msg = "📭 Không có đơn hàng nào"
        return False, error_msg, balance
    
    if BOT1_API_URL:
        success, new_balance, error = deduct_balance_bot1(
            user_id, PRICE_CHECK_COOKIE, "Check cookie Shopee", username
        )
        
        if not success:
            return True, f"{result_html}\n\n⚠️ Không trừ được tiền: {error}", balance
        
        final = (
            f"{result_html}\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💸 <b>Phí check:</b> -{PRICE_CHECK_COOKIE:,}đ\n"
            f"💰 <b>Số dư còn:</b> {new_balance:,}đ"
        )
        return True, final, new_balance
    else:
        return True, result_html, 0

def check_spx_with_payment(code: str, user_id: int, username: str = "") -> tuple:
    """Check SPX with payment"""
    if BOT1_API_URL:
        success, balance, error = check_balance_bot1(user_id)
        if not success:
            return False, f"⚠️ Lỗi: {error}", 0
        if balance < PRICE_CHECK_SPX:
            return False, format_insufficient_balance_msg(balance, PRICE_CHECK_SPX), balance
    else:
        balance = 0
    
    result = check_spx(code)
    if "❌" in result or "Lỗi" in result:
        return False, result, balance
    
    if BOT1_API_URL:
        success, new_balance, error = deduct_balance_bot1(
            user_id, PRICE_CHECK_SPX, f"Check SPX: {code}", username
        )
        if not success:
            return True, f"{result}\n\n⚠️ Không trừ được tiền: {error}", balance
        final = f"{result}\n\n━━━━━━━━━━━━━━━\n💸 <b>Phí:</b> -{PRICE_CHECK_SPX:,}đ\n💰 <b>Còn:</b> {new_balance:,}đ"
        return True, final, new_balance
    else:
        return True, result, 0

def check_ghn_with_payment(order_code: str, user_id: int, username: str = "") -> tuple:
    """Check GHN with payment"""
    if BOT1_API_URL:
        success, balance, error = check_balance_bot1(user_id)
        if not success:
            return False, f"⚠️ Lỗi: {error}", 0
        if balance < PRICE_CHECK_GHN:
            return False, format_insufficient_balance_msg(balance, PRICE_CHECK_GHN), balance
    else:
        balance = 0
    
    result = check_ghn(order_code)
    if "❌" in result or "Lỗi" in result:
        return False, result, balance
    
    if BOT1_API_URL:
        success, new_balance, error = deduct_balance_bot1(
            user_id, PRICE_CHECK_GHN, f"Check GHN: {order_code}", username
        )
        if not success:
            return True, f"{result}\n\n⚠️ Không trừ được tiền: {error}", balance
        final = f"{result}\n\n━━━━━━━━━━━━━━━\n💸 <b>Phí:</b> -{PRICE_CHECK_GHN:,}đ\n💰 <b>Còn:</b> {new_balance:,}đ"
        return True, final, new_balance
    else:
        return True, result, 0

def mask_value(val: str) -> str:
    if not val:
        return ""
    if len(val) <= 18:
        return val
    return val[:10] + "..." + val[-6:]

def split_lines(text: str) -> List[str]:
    if not text:
        return []
    return [x.strip() for x in text.splitlines() if x.strip()]

def is_cookie(val: str) -> bool:
    return val.startswith("SPC_ST=") or ("SPC_ST=" in val)

def is_spx(val: str) -> bool:
    return re.fullmatch(r"SPXVN[0-9A-Z]+", val.strip()) is not None

def is_ghn_code(text: str) -> bool:
    t = text.strip().upper()
    return t.startswith(("GHN", "GYP")) or (t.isdigit() and len(t) >= 8)

def esc(s: str) -> str:
    return html.escape(s or "")

# =========================================================
# WORKSHEET HELPER
# =========================================================
def get_or_create_worksheet(title: str, headers: List[str]):
    title = (title or "").strip()
    for ws in sh.worksheets():
        if ws.title.strip() == title:
            try:
                first = ws.row_values(1)
                if not first or all((c.strip() == "" for c in first)):
                    ws.update("A1", [headers])
            except Exception:
                pass
            return ws

    ws = sh.add_worksheet(title=title, rows="5000", cols="20")
    ws.update("A1", [headers])
    return ws

ws_user = sh.worksheet(TAB_USERS)

ws_log_check = get_or_create_worksheet(
    TAB_LOGS_CHECK,
    ["time", "Tele ID", "username", "value", "balance_sau", "note"]
)

ws_log_spam = get_or_create_worksheet(
    TAB_LOGS_SPAM,
    ["time", "Tele ID", "username", "count_minute", "strike", "band"]
)

# =========================================================
# SHEET SAFE READ
# =========================================================
def _normalize_header(h: str) -> str:
    return re.sub(r"\s+", " ", (h or "").strip()).lower()

def ws_get_all_records_safe(ws) -> List[Dict[str, Any]]:
    try:
        values = ws.get_all_values()
    except Exception:
        return []

    if not values:
        return []

    headers = values[0]
    norm_headers = [_normalize_header(h) for h in headers]
    out = []
    for row in values[1:]:
        if not row or all((str(c).strip() == "" for c in row)):
            continue
        d = {}
        for i, cell in enumerate(row):
            key = norm_headers[i] if i < len(norm_headers) else f"col_{i+1}"
            d[key] = cell
        out.append(d)
    return out

def ws_has_headers(ws, required: List[str]) -> bool:
    try:
        first = ws.row_values(1)
    except Exception:
        return False
    norm = set(_normalize_header(x) for x in first)
    return all((_normalize_header(x) in norm) for x in required)

# =========================================================
# USER CACHE (giữ nguyên)
# =========================================================
def get_all_users_cached():
    """
    ⚠️ DEPRECATED: Không dùng nữa vì get_user_row đọc trực tiếp
    Giữ lại để không break code khác
    """
    return []

def get_user_row(tele_id: Any) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    """
    ✅ FIXED: Đọc theo INDEX cột thay vì tên (tránh lỗi header trùng)
    
    Sheet structure (by INDEX):
    - Cột 0 (A): Tele ID
    - Cột 1 (B): username
    - Cột 2 (C): balance
    - Cột 3 (D): Trạng Thái (active)
    - Cột 4 (E): ghi Chú
    - Cột 5 (F): ghi Chú (trùng tên)
    """
    tele_id = safe_text(tele_id)
    
    try:
        # Lấy RAW data từ cache (không dùng get_all_records vì có header trùng)
        try:
            values = ws_user.get_all_values()
        except Exception:
            return None, None
        
        if not values or len(values) < 2:
            return None, None
        
        # DEBUG: In headers
        headers = values[0]
        print(f"[DEBUG] Raw headers: {headers}")
        
        # Duyệt từng row (bỏ qua header)
        for idx, row in enumerate(values[1:], start=2):
            if not row or len(row) < 4:  # Cần ít nhất 4 cột
                continue
            
            # Đọc theo INDEX
            row_tele_id = safe_text(row[0]) if len(row) > 0 else ""  # Cột A
            row_username = safe_text(row[1]) if len(row) > 1 else ""  # Cột B
            row_balance = safe_text(row[2]) if len(row) > 2 else "0"  # Cột C
            row_status = safe_text(row[3]) if len(row) > 3 else ""    # Cột D
            row_note = safe_text(row[4]) if len(row) > 4 else ""      # Cột E
            
            # DEBUG: In row đầu tiên
            if idx == 2:
                print(f"[DEBUG] Sample row: {row}")
                print(f"[DEBUG] Parsed: ID={row_tele_id}, user={row_username}, balance={row_balance}, status={row_status}")
            
            # So sánh Tele ID
            if row_tele_id == tele_id:
                print(f"[DEBUG] ✅ Found user {tele_id} at row {idx}")
                print(f"[DEBUG] Status value: '{row_status}'")
                
                # Return normalized data
                user_data = {
                    "Tele ID": row_tele_id,
                    "username": row_username,
                    "balance": row_balance,
                    "trang thai": row_status.lower().strip(),  # Normalize status
                    "ghi chu": row_note
                }
                
                print(f"[DEBUG] Normalized user: {user_data}")
                return idx, user_data
        
        print(f"[DEBUG] ❌ User {tele_id} NOT FOUND in sheet")
        
    except Exception as e:
        print(f"[ERROR] get_user_row exception: {e}")
        import traceback
        traceback.print_exc()
    
    return None, None

def get_balance(user: Dict[str, Any]) -> int:
    return safe_int(user.get("balance", 0))

def get_note(row_idx: int) -> str:
    """Đọc cột E (index 4) - ghi Chú/note/strike/band"""
    try:
        # Cột E = index 5 (1-based) trong gspread
        return ws_user.cell(row_idx, 5).value or ""
    except Exception:
        return ""

def set_note(row_idx: int, value: str) -> None:
    """Ghi cột E (index 4) - ghi Chú/note/strike/band"""
    try:
        # Cột E = index 5 (1-based) trong gspread
        ws_user.update_cell(row_idx, 5, value)
    except Exception:
        pass

# =========================================================
# STRIKE / BAND
# =========================================================
def parse_strike(note: str) -> int:
    if not note:
        return 0
    m = re.search(r"strike:(\d+)", note)
    if not m:
        return 0
    return safe_int(m.group(1), 0)

def parse_band_until(note: str) -> Optional[datetime]:
    if not note or "band:" not in note:
        return None
    try:
        t = note.split("band:")[1].strip()
        return datetime.strptime(t, "%Y-%m-%d %H:%M")
    except Exception:
        return None

def check_band(row_idx: int) -> Tuple[bool, Optional[datetime]]:
    note = get_note(row_idx)
    until = parse_band_until(note)
    if not until:
        return False, None
    if now() < until:
        return True, until
    set_note(row_idx, "")
    return False, None

def inc_strike_and_band(row_idx: int, tele_id: Any, username: str, count_minute: int) -> Tuple[int, datetime]:
    note = get_note(row_idx)
    strike = parse_strike(note) + 1

    if strike == 1:
        band_until = now() + timedelta(hours=BAND_1_HOURS)
        band_text = "1h"
    elif strike == 2:
        band_until = now() + timedelta(hours=BAND_2_HOURS)
        band_text = "1d"
    else:
        band_until = now() + timedelta(days=BAND_3_DAYS)
        band_text = "7d"

    new_note = f"strike:{strike}|band:{band_until.strftime('%Y-%m-%d %H:%M')}"
    set_note(row_idx, new_note)

    # ✅ BATCH LOG: Đẩy vào queue thay vì ghi trực tiếp
    log_queue.put({
        "type": "spam",
        "data": [
            now().strftime("%Y-%m-%d %H:%M:%S"),
            safe_text(tele_id),
            username or "",
            count_minute,
            strike,
            band_text
        ]
    })

    return strike, band_until

# =========================================================
# LOG CHECK + COUNT
# =========================================================
def log_check(tele_id: Any, username: str, value: str, balance_after: int, note: str) -> None:
    """✅ BATCH LOG: Đẩy vào queue"""
    log_queue.put({
        "type": "check",
        "data": [
            now().strftime("%Y-%m-%d %H:%M:%S"),
            safe_text(tele_id),
            username or "",
            mask_value(value),
            balance_after,
            note
        ]
    })

def count_today_request(tele_id: Any) -> int:
    tele_id = safe_text(tele_id)
    today = now().strftime("%Y-%m-%d")

    try:
        if ws_has_headers(ws_log_check, ["time", "Tele ID"]):
            rows = ws_log_check.get_all_records()
            cnt = 0
            for r in rows:
                t = safe_text(r.get("time"))
                if t.startswith(today) and safe_text(r.get("Tele ID")) == tele_id:
                    cnt += 1
            return cnt
    except Exception:
        pass

    rows = ws_get_all_records_safe(ws_log_check)
    cnt = 0
    for r in rows:
        t = safe_text(r.get("time"))
        tid = safe_text(r.get("tele id"))
        if t.startswith(today) and tid == tele_id:
            cnt += 1
    return cnt

# =========================================================
# TELEGRAM UTIL
# =========================================================
def tg_send(chat_id: Any, text: str, keyboard: Optional[Dict[str, Any]] = None) -> None:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    if keyboard:
        payload["reply_markup"] = keyboard

    try:
        requests.post(f"{BASE_URL}/sendMessage", json=payload, timeout=15)
    except Exception:
        pass

def tg_answer_callback(callback_query_id: str, text: str = "") -> None:
    try:
        requests.post(
            f"{BASE_URL}/answerCallbackQuery",
            json={"callback_query_id": callback_query_id, "text": text},
            timeout=10
        )
    except Exception:
        pass

def main_keyboard():
    return {
        "keyboard": [
            ["✅ Kích Hoạt", "💰 Số dư"],
            ["💳 Nạp Tiền", "📘 Hướng dẫn"],
            ["🧩 Hệ Thống Bot NgânMiu"]
        ],
        "resize_keyboard": True
    }

# =========================================================
# CALLBACK HANDLER
# =========================================================
def handle_callback_query(data: Dict[str, Any]) -> None:
    cq = data.get("callback_query")
    if not cq:
        return

    callback_id = cq.get("id")
    if callback_id:
        tg_answer_callback(callback_id)

    from_user = cq.get("from", {})
    message   = cq.get("message", {})

    tele_id  = from_user.get("id")
    username = from_user.get("username") or ""
    chat_id  = (message.get("chat") or {}).get("id")

    action = cq.get("data", "")

    row_idx, user = get_user_row(tele_id)

    if action == "ACTIVATE":
        tg_send(
            chat_id,
            "✅ <b>KÍCH HOẠT</b>\n\n"
            f"🆔 Tele ID: <code>{tele_id}</code>\n"
            f"👤 Username: @{esc(username) if username else '(none)'}\n\n"
            "👉 Nếu chưa có trong Sheet, bạn thêm Tele ID vào tab <b>Thanh Toan</b>."
        )
        return

    if action == "BALANCE":
        if not user:
            tg_send(
                chat_id,
                "❌ <b>Tài khoản chưa có trong Sheet</b>\n\n"
                "Bấm <b>✅ Kích hoạt</b> để lấy Tele ID rồi thêm vào tab <b>Thanh Toan</b>.",
                main_keyboard()
            )
            return
        balance = get_balance(user)
        tg_send(chat_id, f"💰 <b>Số Dư HIỆN TẠI</b>\n\n{balance:,}đ")
        return

    if action == "HELP":
        tg_send(
            chat_id,
            "📌 <b>HƯỚNG DẪN</b>\n\n"
            "1) Gửi <b>cookie SPC_ST</b> để bot trả <b>thông tin đơn hàng</b>\n"
            "   Ví dụ:\n"
            "<code>SPC_ST=.xxxxx</code>\n\n"
            "2) Gửi <b>mã vận đơn SPX</b> để tra lịch trình \n"
            "   Ví dụ:\n"
            "<code>SPXVN05805112503C</code>\n\n"
            "💡 Mỗi dòng 1 dữ liệu. Gửi nhiều dòng bot sẽ check lần lượt."
        )
        return

    if action == "CHECK":
        tg_send(
            chat_id,
            "📦 <b>GỬI DỮ LIỆU CHECK</b>\n\n"
            "• Mỗi dòng 1 cookie hoặc 1 mã SPX\n"
            "• Ví dụ:\n"
            "<code>SPC_ST=.xxxxx</code>\n"
            "<code>SPXVN05805112503C</code>"
        )
        return

# =========================================================
# STATUS ALIAS
# =========================================================
GHN_STATUS_EMOJI = {
    "Chờ lấy hàng": "🕓",
    "Nhận hàng tại bưu cục": "📦",
    "Sẵn sàng xuất đến Kho trung chuyển": "🚚",
    "Xuất hàng đi khỏi kho": "🚛",
    "Đang trung chuyển hàng": "🚚",
    "Nhập hàng vào kho trung chuyển": "🏬",
    "Đang giao hàng": "🚴",
    "Giao hàng thành công": "✅",
    "Giao hàng không thành công": "❌",
    "Hoàn hàng": "↩️"
}

CODE_MAP = {
    "order_status_text_to_receive_delivery_done": ("✅ Giao hàng thành công", "success"),
    "order_tooltip_to_receive_delivery_done":     ("✅ Giao hàng thành công", "success"),
    "label_order_delivered":                      ("✅ Giao hàng thành công", "success"),
    "order_list_text_to_receive_non_cod":         ("🚚 Đang chờ nhận (không COD)", "info"),
    "label_to_receive":                           ("🚚 Đang chờ nhận", "info"),
    "label_order_to_receive":                     ("🚚 Đang chờ nhận", "info"),
    "label_order_to_ship":                        ("📦 Chờ giao hàng", "warning"),
    "label_order_being_packed":                   ("📦 Đang chuẩn bị hàng", "warning"),
    "label_order_processing":                     ("🔄 Đang xử lý", "warning"),
    "label_order_paid":                           ("💰 Đã thanh toán", "info"),
    "label_order_unpaid":                         ("💸 Chưa thanh toán", "info"),
    "label_order_waiting_shipment":               ("📦 Chờ bàn giao vận chuyển", "info"),
    "label_order_shipped":                        ("🚛 Đã bàn giao vận chuyển", "info"),
    "label_order_delivery_failed":                ("❌ Giao không thành công", "danger"),
    "label_order_cancelled":                      ("❌ Đã hủy", "danger"),
    "label_order_return_refund":                  ("↩️ Trả hàng / Hoàn tiền", "info"),
    "order_list_text_to_ship_ship_by_date_not_calculated": ("🎖 Đơn hàng chờ Shopee duyệt", "warning"),
    "order_status_text_to_ship_ship_by_date_not_calculated": ("🎖 Đơn hàng chờ Shopee duyệt", "warning"),
    "label_ship_by_date_not_calculated": ("🎖 Đơn hàng chờ Shopee duyệt", "warning"),
    "label_preparing_order":                      ("📦 Chờ shop gửi hàng", "warning"),
    "order_list_text_to_ship_order_shipbydate":   ("📦 Chờ shop gửi hàng", "warning"),
    "order_status_text_to_ship_order_shipbydate": ("📦 Người gửi đang chuẩn bị hàng", "warning"),
    "order_list_text_to_ship_order_shipbydate_cod": ("📦 Chờ shop gửi hàng (COD)", "warning"),
    "order_status_text_to_ship_order_shipbydate_cod": ("📦 Chờ shop gửi hàng (COD)", "warning"),
    "order_status_text_to_ship_order_edt_cod": ("📦 Chờ shop gửi hàng (COD)", "warning"),
}

def normalize_status_text(status: str) -> str:
    if not isinstance(status, str):
        return ""
    s = status.strip()
    s = re.sub(r"^tình trạng\s*:?\s*", "", s, flags=re.I)
    return s.strip()

# =========================================================
# SHOPEE CHECK
# =========================================================
UA = "Android app Shopee appver=28320 app_type=1"
SHOPEE_BASE = "https://shopee.vn/api/v4"

def build_headers(cookie: str) -> dict:
    return {
        "User-Agent": UA,
        "Cookie": cookie.strip(),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def find_first_key(data, key):
    dq = deque([data])
    while dq:
        cur = dq.popleft()
        if isinstance(cur, dict):
            if key in cur:
                return cur[key]
            dq.extend(v for v in cur.values() if isinstance(v, (dict, list)))
        elif isinstance(cur, list):
            dq.extend(x for x in cur if isinstance(x, (dict, list)))
    return None

def bfs_values_by_key(data, target_keys=("order_id",)):
    out, dq, tset = [], deque([data]), set(target_keys)
    while dq:
        cur = dq.popleft()
        if isinstance(cur, dict):
            for k, v in cur.items():
                if k in tset:
                    out.append(v)
                if isinstance(v, (dict, list)):
                    dq.append(v)
        elif isinstance(cur, list):
            dq.extend(cur)
    return out

def fmt_ts(ts):
    if isinstance(ts, str) and ts.isdigit():
        ts = int(ts)
    if isinstance(ts, (int, float)) and ts > 1_000_000:
        try:
            return datetime.fromtimestamp(int(ts)).strftime("%H:%M %d-%m-%Y")
        except Exception:
            return str(ts)
    return str(ts) if ts is not None else None

# =========================================================
# ✅ FIX 1: TIMEOUT + RETRY
# =========================================================
def fetch_single_order_detail(order_id: str, headers: dict) -> Optional[dict]:
    """Fetch chi tiết 1 order với retry"""
    url = f"{SHOPEE_BASE}/order/get_order_detail"
    
    for attempt in range(TIMEOUT_RETRY + 1):
        try:
            r = requests.get(
                url,
                headers=headers,
                params={"order_id": order_id},
                timeout=TIMEOUT_DETAIL  # 4s
            )
            if r.status_code == 200:
                return r.json()
        except requests.exceptions.Timeout:
            if attempt < TIMEOUT_RETRY:
                continue  # Retry
            return None
        except Exception:
            return None
    
    return None

# =========================================================
# PARALLEL VERSION
# =========================================================
def fetch_orders_and_details_parallel(cookie: str, limit: int = 5):
    """PARALLEL VERSION với timeout mới"""
    headers = build_headers(cookie)
    list_url = f"{SHOPEE_BASE}/order/get_all_order_and_checkout_list"
    
    # Step 1: Lấy list orders
    for attempt in range(TIMEOUT_RETRY + 1):
        try:
            r = requests.get(
                list_url,
                headers=headers,
                params={
                    "limit": limit,
                    "offset": 0,
                    "need_order_response": 1,  # ✅ FIX 4: Giảm payload
                    "need_shipping_info": 0
                },
                timeout=TIMEOUT_LIST  # 5s
            )
            
            if r.status_code == 200:
                data = r.json()
                break
        except requests.exceptions.Timeout:
            if attempt < TIMEOUT_RETRY:
                continue
            return None, "timeout"
        except Exception as e:
            return None, f"error: {e}"
    else:
        return None, "timeout"
    
    # Cookie validation
    if isinstance(data, dict):
        if (
            data.get("error") in (401, 403)
            or data.get("error_msg")
            or data.get("msg") in ("unauthorized", "forbidden")
        ):
            return None, "cookie_expired"
    
    # Parse order IDs
    order_ids = bfs_values_by_key(data, ("order_id",)) if isinstance(data, dict) else []
    
    if not order_ids:
        if not data or len(data.keys()) <= 2:
            return None, "cookie_expired"
        return None, "no_orders"
    
    # Remove duplicates
    seen, uniq = set(), []
    for oid in order_ids:
        if oid not in seen:
            seen.add(oid)
            uniq.append(oid)
    
    # Step 2: Parallel fetch details
    details = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_oid = {
            executor.submit(fetch_single_order_detail, oid, headers): oid 
            for oid in uniq[:limit]
        }
        
        for future in as_completed(future_to_oid, timeout=TIMEOUT_DETAIL + 2):
            try:
                result = future.result(timeout=1)
                if result:
                    details.append(result)
            except Exception:
                pass
    
    if not details:
        return None, "cookie_expired"
    
    return details, None

def fetch_orders_and_details(cookie: str, limit: int = None):
    """Smart dispatcher"""
    if limit is None:
        limit = CHECK_LIMIT
    
    if USE_PARALLEL:
        return fetch_orders_and_details_parallel(cookie, limit)
    
    # Sequential mode
    headers = build_headers(cookie)
    list_url = f"{SHOPEE_BASE}/order/get_all_order_and_checkout_list"
    
    try:
        r = requests.get(
            list_url,
            headers=headers,
            params={
                "limit": limit,
                "offset": 0,
                "need_order_response": 1,  # ✅ Giảm payload
                "need_shipping_info": 0
            },
            timeout=TIMEOUT_LIST
        )
        
        if r.status_code != 200:
            return None, f"http_{r.status_code}"
        
        data = r.json()
    except Exception as e:
        return None, f"timeout: {e}"
    
    if isinstance(data, dict):
        if (
            data.get("error") in (401, 403)
            or data.get("error_msg")
            or data.get("msg") in ("unauthorized", "forbidden")
        ):
            return None, "cookie_expired"
    
    order_ids = bfs_values_by_key(data, ("order_id",)) if isinstance(data, dict) else []
    
    if not order_ids:
        if not data or len(data.keys()) <= 2:
            return None, "cookie_expired"
        return None, "no_orders"
    
    seen, uniq = set(), []
    for oid in order_ids:
        if oid not in seen:
            seen.add(oid)
            uniq.append(oid)
    
    details = []
    for oid in uniq[:limit]:
        detail = fetch_single_order_detail(oid, headers)
        if detail:
            details.append(detail)
    
    if not details:
        return None, "cookie_expired"
    
    return details, None

def format_order_simple(detail: dict) -> str:
    """Format đơn hàng Shopee"""

    def short_text(s: str, max_len: int) -> str:
        s = (s or "").strip()
        if len(s) <= max_len:
            return s
        return s[:max_len - 3].rstrip() + "..."

    tracking = (
        find_first_key(detail, "tracking_no")
        or find_first_key(detail, "tracking_number")
        or "-"
    )

    status_text = "-"
    tracking_info = find_first_key(detail, "tracking_info")
    if isinstance(tracking_info, dict):
        status_text = (
            tracking_info.get("description")
            or tracking_info.get("text")
            or tracking_info.get("status_text")
            or "-"
        )

    status_text = status_text.strip() if isinstance(status_text, str) else "-"

    if not status_text or status_text == "-":
        status_obj = find_first_key(detail, "status")
        raw_status = "-"
        if isinstance(status_obj, dict):
            raw_status = (
                status_obj.get("text")
                or status_obj.get("header_text")
                or status_obj.get("list_view_text")
                or "-"
            )
        elif status_obj is not None:
            raw_status = str(status_obj)

        raw_status = normalize_status_text(str(raw_status))
        st2, _ = map_code(raw_status)
        status_text = st2 or raw_status or "-"

    cod_amount = 0
    try:
        cod_amount = (
            find_first_key(detail, "cod_amount")
            or find_first_key(detail, "total_cod")
            or find_first_key(detail, "buyer_total_amount")
            or 0
        )
        cod_amount = int(cod_amount)
    except Exception:
        cod_amount = 0

    product_names = []
    items = find_first_key(detail, "item_list") or find_first_key(detail, "items")
    if isinstance(items, list):
        for it in items:
            if isinstance(it, dict):
                name = it.get("name") or it.get("item_name")
                if name:
                    product_names.append(name.strip())

    if product_names:
        product_text = product_names[0]
        if len(product_names) > 1:
            product_text += f" (+{len(product_names)-1} SP)"
    else:
        product_text = "-"

    product_text = short_text(product_text, 68)

    rec_addr = find_first_key(detail, "recipient_address") or {}
    if not isinstance(rec_addr, dict):
        rec_addr = {}

    recipient_name = (
        find_first_key(detail, "shipping_name")
        or rec_addr.get("name")
        or "-"
    )
    recipient_phone = (
        find_first_key(detail, "shipping_phone")
        or rec_addr.get("phone")
        or "-"
    )
    address = (
        find_first_key(detail, "shipping_address")
        or rec_addr.get("full_address")
        or "-"
    )
    address = short_text(address, 78)

    shipper_name = find_first_key(detail, "driver_name") or "-"
    shipper_phone = find_first_key(detail, "driver_phone") or "-"

    output = (
        "🧾 <u><b>ĐƠN HÀNG</b></u>\n"
        f"📦 <b>MVĐ:</b> <code>{esc(tracking)}</code>\n"
        f"📊 <b>Trạng thái:</b> {esc(status_text)}\n"
        f"🎁 <b>Sản phẩm:</b> {esc(product_text)}\n"
    )

    if cod_amount > 0:
        output += f"💵 <b>COD:</b> {cod_amount:,}đ\n"

    output += (
        "\n🚚 <u><b>GIAO NHẬN</b></u>\n"
        f"👤 <b>Người nhận:</b> {esc(recipient_name)}\n"
        f"📞 <b>SĐT:</b> {esc(recipient_phone)}\n"
        f"📍 <b>Địa chỉ:</b> {esc(address)}\n"
        f"🚚 <b>Shipper:</b> {esc(shipper_name)}\n"
        f"📱 <b>SĐT ship:</b> {esc(shipper_phone)}\n\n"
        "<i>ℹ️ Tap vào MVĐ để copy nhanh.</i>"
    )

    return output

def map_code(code):
    if not isinstance(code, str):
        return None, "secondary"
    return CODE_MAP.get(code, (code, "secondary"))

def check_shopee_orders(cookie: str) -> Tuple[Optional[str], Optional[str]]:
    """✅ CACHE COOKIE: Check với cache"""
    cookie = cookie.strip()
    if "SPC_ST=" not in cookie:
        return None, "missing_spc_st"

    # ✅ Kiểm tra cache trước
    cached = get_cached_orders(cookie)
    if cached:
        print(f"[CACHE] HIT cookie: {cookie[:20]}...")
        # Format từ cache
        blocks = []
        for d in cached:
            if isinstance(d, dict):
                block = format_order_simple(d)
                blocks.append(block)
        return "\n\n".join(blocks), None

    # Cache miss → Fetch mới
    print(f"[CACHE] MISS cookie: {cookie[:20]}...")
    details, error = fetch_orders_and_details(cookie)
    
    if error:
        return None, error

    if not details:
        return "📭 <b>Không có đơn hàng</b>", None

    # ✅ Lưu vào cache
    set_cached_orders(cookie, details)

    blocks = []
    for idx, d in enumerate(details, 1):
        if isinstance(d, dict):
            block = format_order_simple(d)
            blocks.append(block)

    return "\n\n".join(blocks), None

# =========================================================
# SPX CHECK
# =========================================================
SPX_API = "https://tramavandon.com/api/spx.php"

def check_spx(code: str) -> str:
    code = (code or "").strip().upper()

    payload = {"tracking_id": code}
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Connection": "close"
    }

    try:
        r = requests.post(
            SPX_API,
            json=payload,
            headers=headers,
            timeout=(5, 10)
        )
        data = r.json()

        if data.get("retcode") != 0:
            return f"🔎 <b>{esc(code)}</b>\n❌ Không tìm thấy thông tin"

        info = data["data"]["sls_tracking_info"]
        records = info.get("records", [])

        timeline = []
        phone = ""
        last_ts = None
        first_ts = None

        for rec in records:
            ts = rec.get("actual_time")
            if not ts:
                continue

            if not first_ts:
                first_ts = ts
            last_ts = ts

            dt = datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M")

            status_text = rec.get("buyer_description", "").strip()
            location = rec.get("current_location", {}).get("location_name", "").strip()

            if not phone:
                found = re.findall(r"\b0\d{9,10}\b", status_text)
                if found:
                    phone = found[0]

            line = f"• {dt} — {status_text}"
            if location:
                line += f" — {location}"

            timeline.append(line)

        eta_text = "-"
        if last_ts:
            eta = datetime.fromtimestamp(last_ts) + timedelta(days=1)
            eta_text = eta.strftime("%d/%m/%Y")

        timeline_text = "\n".join(timeline[-5:]) if timeline else "Chưa có thông tin"

        return (
            "📦 <b>Shopee Express (SPX)</b>\n"
            "━━━━━━━━━━━━━━━\n"
            f"🔎 <b>MVĐ:</b> <code>{esc(code)}</code>\n"
            f"🚚 <b>Trạng thái:</b> Đang vận chuyển\n"
            f"🕒 <b>Dự kiến giao:</b> {eta_text}\n"
            f"📱 <b>SĐT shipper:</b> <code>{esc(phone) if phone else '-'}</code>\n\n"
            "📜 <b>Timeline:</b>\n"
            f"{timeline_text}"
        )

    except requests.exceptions.ReadTimeout:
        return f"🔎 <b>{esc(code)}</b>\n⏱️ SPX phản hồi quá chậm, thử lại sau"

    except Exception as e:
        return f"🔎 <b>{esc(code)}</b>\n❌ Lỗi SPX: {e}"

# =========================================================
# GHN CHECK
# =========================================================
def clean_ghn_status(text: str) -> str:
    if not text:
        return ""

    text = text.strip()

    if " – " in text:
        return text.split(" – ", 1)[1].strip()

    if " - " in text:
        return text.split(" - ", 1)[1].strip()

    return text

def check_ghn(order_code: str, max_steps: int = 4) -> str:
    import requests
    from datetime import datetime

    url = "https://fe-online-gateway.ghn.vn/order-tracking/public-api/client/tracking-logs"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": "https://donhang.ghn.vn",
        "Referer": "https://donhang.ghn.vn/",
        "User-Agent": "Mozilla/5.0"
    }

    payload = {"order_code": order_code.strip()}

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=10)
        r.raise_for_status()
        res = r.json()
    except Exception as e:
        return f"❌ <b>LỖI GHN</b>\nKhông kết nối được hệ thống\n{e}"

    if res.get("code") != 200:
        return "❌ <b>KHÔNG TÌM THẤY ĐƠN GHN</b>"

    data = res.get("data", {})
    info = data.get("order_info", {})
    logs = data.get("tracking_logs", [])

    carrier = "GHN | GIAO HÀNG NHANH"
    status_name = info.get("status_name", "-")
    emoji = GHN_STATUS_EMOJI.get(status_name, "🚚")

    eta = "-"
    leadtime = info.get("leadtime")
    if leadtime:
        try:
            eta = datetime.fromisoformat(leadtime.replace("Z", "")).strftime("%d/%m/%Y")
        except Exception:
            eta = leadtime[:10]

    timeline = []
    last_key = None

    for lg in reversed(logs):
        status = clean_ghn_status(lg.get("status_name", "").strip())

        addr = lg.get("location", {}).get("address", "").strip()

        if not status:
            continue

        key = f"{status}|{addr}"
        if key == last_key:
            continue

        t = lg.get("action_at", "")
        if t:
            try:
                t = datetime.fromisoformat(t.replace("Z", "")).strftime("%d/%m %H:%M")
            except Exception:
                t = t.replace("T", " ")[:16]

        content = status
        if addr and addr not in status:
            content = f"{status} — {addr}"

        timeline.append(f"🕔 {t} — {content}")
        last_key = key

        if len(timeline) >= max_steps:
            break

    if not timeline:
        timeline.append("Chưa có lịch trình")

    timeline_text = "\n".join(timeline)

    return (
        f"📦 <b>{carrier}</b>\n"
        "━━━━━━━━━━━━━━━\n"
        f"🔎 <b>MVĐ:</b> <code>{order_code}</code>\n"
        f"📊 <b>Trạng thái:</b> {emoji} {status_name}\n"
        f"🕒 <b>Dự kiến giao:</b> {eta}\n\n"
        "📜 <b>Timeline (gần nhất):</b>\n"
        f"{timeline_text}"
    )

# =========================================================
# WEBHOOK HANDLER
# =========================================================
def _handle_message(chat_id: Any, tele_id: Any, username: str, text: str) -> None:
    if text == "/start":
        tg_send(
            chat_id,
            "🤖 <b>BOT CHECK ĐƠN HÀNG SHOPEE + SPX</b>\n\n"
            "Chọn chức năng bên dưới 👇",
            main_keyboard()
        )
        return

    if text == "✅ Kích Hoạt":
        row_idx, user = get_user_row(tele_id)

        if not user:
            tg_send(
                chat_id,
                "❌ <b>CHƯA KÍCH HOẠT</b>\n\n"
                f"🆔 <b>Tele ID của bạn:</b> <code>{tele_id}</code>\n\n"
                "👉 Vui lòng kích hoạt tại bot lưu voucher trước:\n"
                "🎟️ @nganmiu_bot",
                main_keyboard()
            )
            return

        # Lấy cột "Trạng Thái" (có thể là "trang thai" hoặc "trạng thái")
        status = safe_text(
            user.get("trang thai")  # Cột D: "Trạng Thái"
            or user.get("trạng thái")
            or user.get("Trang Thái")
            or user.get("status")
        ).lower().strip()

        if status == "active":
            balance = get_balance(user)
            tg_send(
                chat_id,
                "✅ <b>TÀI KHOẢN ĐÃ KÍCH HOẠT</b>\n\n"
                f"🆔 <b>Tele ID:</b> <code>{tele_id}</code>\n"
                f"👤 <b>Username:</b> {user.get('username') or '(chưa có)'}\n"
                f"💰 <b>Số dư:</b> {balance:,}đ\n\n"
                "Bạn có thể sử dụng bot bình thường 🚀",
                main_keyboard()
            )
            return

        tg_send(
            chat_id,
            "❌ <b>CHƯA KÍCH HOẠT</b>\n\n"
            f"🆔 <b>Tele ID của bạn:</b> <code>{tele_id}</code>\n"
            f"📊 <b>Trạng thái:</b> {status or '(trống)'}\n\n"
            "👉 Hãy kích hoạt tại bot lưu voucher:\n"
            "🎟️ @nganmiu_bot",
            main_keyboard()
        )
        return
        
    if text == "📘 Hướng dẫn":
        tg_send(
            chat_id,
            "📘 <b>HƯỚNG DẪN SỬ DỤNG BOT</b>\n"
            "━━━━━━━━━━━━━━━\n\n"
            "📦 <b>Check đơn hàng Shopee</b>\n"
            "👉 Gửi <b>cookie</b> dạng:\n"
            "<code>SPC_ST=xxxxx</code>\n\n"
            "🚚 <b>Tra mã vận đơn</b>\n"
            "👉 Gửi mã dạng:\n"
            "<code>SPXVNxxxxx</code>\n\n"
            "🚛 <b>Hỗ trợ các bên vận chuyển</b>\n"
            "• 🟠 <b>Shopee Express (SPX)</b>\n"
            "• 🟢 <b>Giao Hàng Nhanh (GHN)</b>\n\n"
            "⚠️ <b>Lưu ý</b>\n"
            "• Mỗi dòng 1 dữ liệu\n"
            "• Gửi nhiều dòng → bot check lần lượt\n"
            "• Spam quá nhanh sẽ bị khóa tạm thời\n\n"
            "🧩 <i>Hệ thống NgânMiu.Store – Tự động & An toàn</i>",
            main_keyboard()
        )
        return

    if text == "💰 Số dư":
        row_idx, user = get_user_row(tele_id)

        if not user:
            tg_send(
                chat_id,
                "❌ <b>Bạn chưa kích hoạt</b>\n\n"
                "👉 Kích hoạt tại @nganmiu_bot",
                main_keyboard()
            )
            return

        balance = get_balance(user)

        tg_send(
            chat_id,
            f"💰 <b>SỐ DƯ HIỆN TẠI</b>\n\n"
            f"{balance:,} đ",
            main_keyboard()
        )
        return

    if text == "💳 Nạp Tiền":
        tg_send(
            chat_id,
            "💳 <b>NẠP TIỀN</b>\n\n"
            "👉 Vui lòng nạp tiền tại bot chính:\n"
            "💸 @nganmiu_bot",
            main_keyboard()
        )
        return

    if text == "🎟️ Bot Lưu Voucher":
        tg_send(
            chat_id,
            "🎟️ <b>BOT LƯU VOUCHER</b>\n\n"
            "👉 Mở bot tại:\n"
            "https://t.me/nganmiu_bot",
            main_keyboard()
        )
        return
        
    if text == "🧩 Hệ Thống Bot NgânMiu":
        tg_send(
            chat_id,
            "🧩 <b>HỆ THỐNG BOT NGÂNMIU</b>\n"
            "━━━━━━━━━━━━━━━\n\n"
            "🧑‍💼 <b>Admin hỗ trợ</b>\n"
            "👉 @BonBonxHPx\n\n"
            "👥 <b>Group Hỗ Trợ</b>\n"
            "👉 https://t.me/botxshopee\n\n"
            "🤖 <b>Danh sách Bot</b>\n"
            "━━━━━━━━━━━━━━━\n"
            "🎟️ <b>Bot Lưu Voucher</b>\n"
            "👉 @nganmiu_bot\n\n"
            "📦 <b>Bot Check Đơn Hàng</b>\n"
            "👉 @ShopeexCheck_Bot\n\n"
            "📱 <b>Bot Thuê Số</b>\n"
            "👉 <i>Sắp mở</i> 🔜\n\n"
            "✨ <i>Book Đơn Mã New tại NganMiu.Store</i>",
            main_keyboard()
        )
        return
        
    if is_ghn_code(text):
        result = check_ghn(text)
        tg_send(chat_id, result)
        return

    row_idx, user = get_user_row(tele_id)
    if not user:
        tg_send(
            chat_id,
            "❌ <b>Tài khoản chưa có trong Sheet</b>\n\n"
            "Bấm <b>✅ Kích hoạt</b> để lấy Tele ID rồi thêm vào tab <b>Thanh Toan</b>.",
            main_keyboard()
        )
        return

    is_band, until = check_band(row_idx)
    if is_band:
        tg_send(
            chat_id,
            "🚫 <b>Tài khoản đang bị khóa</b>\n\n"
            f"⏱️ Mở lại lúc: <b>{until.strftime('%H:%M %d/%m')}</b>"
        )
        return

    lines = split_lines(text)
    values = [v.strip() for v in lines if is_cookie(v.strip()) or is_spx(v.strip())]
    if not values:
        tg_send(
            chat_id,
            "❌ <b>Dữ liệu không hợp lệ</b>\n\n"
            "🪙 Cookie: <code>SPC_ST=.xxxxx</code>\n"
            "🚚 SPX: <code>SPXVNxxxxx</code>",
            main_keyboard()
        )
        return

    balance = get_balance(user)

    for val in values:
        minute_key = now().strftime("%Y-%m-%d %H:%M")
        tid = safe_text(tele_id)
        spam_cache.setdefault(tid, {})
        spam_cache[tid][minute_key] = spam_cache[tid].get(minute_key, 0) + 1

        if spam_cache[tid][minute_key] > SPAM_LIMIT_PER_MIN:
            strike, band_until = inc_strike_and_band(row_idx, tele_id, username, spam_cache[tid][minute_key])
            tg_send(
                chat_id,
                "🚫 <b>SPAM PHÁT HIỆN</b>\n\n"
                f"⚠️ Strike: <b>{strike}</b>\n"
                f"⏱️ Band tới: <b>{band_until.strftime('%H:%M %d/%m')}</b>"
            )
            return

        # ================= FREE LOGIC =================
        # Balance > 10,000đ → Dùng FREE không giới hạn
        # Balance ≤ 10,000đ → Giới hạn 10 lượt/ngày
        
        if balance > 10000:
            # User có nhiều tiền → Dùng FREE không giới hạn
            print(f"[FREE] User {tele_id} balance={balance:,}đ > 10,000đ → FREE unlimited")
        else:
            # User ít tiền → Giới hạn 10 lượt/ngày
            used = count_today_request(tele_id)
            if used >= FREE_LIMIT_PER_DAY:
                tg_send(
                    chat_id,
                    "⚠️ <b>HẾT LƯỢT MIỄN PHÍ HÔM NAY</b>\n\n"
                    f"📊 Đã dùng: {used}/{FREE_LIMIT_PER_DAY} lượt\n"
                    f"💰 Số dư hiện tại: {balance:,}đ\n\n"
                    f"💡 <b>Để dùng không giới hạn:</b>\n"
                    f"👉 Nạp thêm để số dư > 10,000đ tại @nganmiu_bot"
                )
                return
            print(f"[FREE] User {tele_id} balance={balance:,}đ ≤ 10,000đ → Free limited: {used}/{FREE_LIMIT_PER_DAY}")

        # ================= DO CHECK =================
        if is_cookie(val):
            result, error = check_shopee_orders(val)

            if not result:
                if error == "cookie_expired":
                    tg_send(
                        chat_id,
                        "🔒 <b>COOKIE KHÔNG HỢP LỆ</b>\n\n"
                        "❌ Cookie đã <b>hết hạn</b> hoặc <b>bị Shopee khóa</b>."
                    )
                    log_check(tele_id, username, val, balance, "cookie_expired")
                else:
                    tg_send(
                        chat_id,
                        "📭 <b>KHÔNG CÓ ĐƠN HÀNG</b>\n\n"
                        "Cookie hợp lệ nhưng hiện <b>không có đơn nào</b>."
                    )
                    log_check(tele_id, username, val, balance, "no_orders")

            else:
                tg_send(chat_id, result)
                log_check(tele_id, username, val, balance, "check_orders")

        elif is_spx(val):
            result = check_spx(val)
            tg_send(chat_id, result)
            log_check(tele_id, username, val, balance, "check_spx")

        time.sleep(0.2)

@app.route("/", methods=["POST", "GET"])
def webhook_root():
    if request.method == "GET":
        return jsonify({"ok": True, "msg": "Bot STEP 1 Optimized"}), 200

    data = request.get_json(silent=True) or {}

    if "callback_query" in data:
        try:
            handle_callback_query(data)
        except Exception:
            pass
        return "OK"

    msg = data.get("message") or {}
    chat_id = (msg.get("chat") or {}).get("id")
    tele_id = (msg.get("from") or {}).get("id")
    username = (msg.get("from") or {}).get("username") or ""
    text = (msg.get("text") or "").strip()

    if not chat_id or not tele_id:
        return "OK"

    try:
        _handle_message(chat_id, tele_id, username, text)
    except Exception:
        err = traceback.format_exc()
        tg_send(chat_id, "❌ Bot gặp lỗi nội bộ, bạn gửi lại sau nhé.")
        try:
            print(err)
        except Exception:
            pass

    return "OK"

@app.route("/webhook", methods=["POST", "GET"])
def webhook_alias():
    return webhook_root()

# =========================================================
# 🔥 START LOG WORKER THREAD
# =========================================================
log_thread = threading.Thread(target=log_worker, daemon=True)
log_thread.start()

# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    print("=" * 50)
    print("🤖 BOT STEP 1 OPTIMIZED - RUNNING")
    print("=" * 50)
    print(f"📋 Sheet ID: {SHEET_ID[:20]}...")
    print(f"🔑 Bot Token: {BOT_TOKEN[:20]}...")
    print("✅ Log worker thread started")
    print("=" * 50)
    
    # Cleanup cache định kỳ mỗi 5 phút
    def cleanup_cache_worker():
        while True:
            time.sleep(300)  # 5 phút
            clear_expired_cache()
            print("[CACHE] Cleaned expired cache")
    
    cache_thread = threading.Thread(target=cleanup_cache_worker, daemon=True)
    cache_thread.start()
    
    app.run(host="0.0.0.0", port=5000, debug=False)
