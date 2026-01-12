# -*- coding: utf-8 -*-
"""
NgânMiu.Store — BOT CHECK ĐƠN HÀNG SHOPEE + TRA MÃ VẬN ĐƠN SPX + GET COOKIE QR
✅ STEP 1 OPTIMIZATION: Cache Cookie + Batch Log + Timeout tối ưu
✅ TÍCH HỢP GET COOKIE QR SHOPEE

🔧 FIXED (Jan 2026):
- Fix logic get_qr_cookie(): nếu session đã có cookie thì trả ngay (không gọi API lại)
- Fix logic check_shopee_orders_with_payment(): tách rõ error và result để không hiểu nhầm
- Add basic locks cho qr_sessions / order_cache / spam_cache (giảm race condition trong 1 instance)
- Prune spam_cache theo phút (giảm phình RAM)
"""

import os
import re
import json
import time
import html
import traceback
import threading
import base64
import random
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
print(" BOT OPTIMIZED - STEP 1: CACHE + BATCH + TIMEOUT + QR LOGIN")
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
cache_lock = threading.Lock()

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
    PRICE_GET_COOKIE = int(os.getenv("PRICE_GET_COOKIE", "50"))  # Phí lấy cookie mới (thu khi lấy thành công)  # Phí lấy cookie mới
    print(f"[PAYMENT] Active: {PRICE_CHECK_COOKIE}đ/check, {PRICE_GET_COOKIE}đ/get_cookie")
else:
    PRICE_CHECK_COOKIE = PRICE_CHECK_SPX = PRICE_CHECK_GHN = PRICE_GET_COOKIE = 0
    print("[PAYMENT] Disabled")

# QR API Configuration
QR_API_BASE = os.getenv("QR_API_BASE", "https://qr-shopee-puce.vercel.app").strip()
QR_POLL_INTERVAL = float(os.getenv("QR_POLL_INTERVAL", "3.0"))  # giây check 1 lần  # giây check 1 lần (tăng tốc)
QR_TIMEOUT = 300  # 5 phút timeout
COOKIE_VALIDITY_DAYS = 7  # ✅ Cookie hiệu lực 7 ngày


# Auto watcher (bot tự theo dõi QR và trả cookie sau khi quét)
AUTO_QR = os.getenv("AUTO_QR", "true").lower() == "true"
AUTO_QR_MAX_SECONDS = int(os.getenv("AUTO_QR_MAX_SECONDS", str(QR_TIMEOUT)))

# AUTO detect status mapping (Shopee có thể trả nhiều biến thể)
SCANNED_STATUSES = {"SCANNED", "CONFIRMED", "AUTHORIZED", "AUTHED", "SUCCESS", "APPROVED", "OK", "DONE"}
PENDING_STATUSES = {"PENDING", "WAITING", "UNKNOWN", "INIT", "CREATED"}

# QR Session Management
qr_sessions = {}  # {session_id: {"user_id": user_id, "created": timestamp, "status": "waiting", "qr_image": base64}}
qr_lock = threading.Lock()

# User cache (giữ nguyên từ version trước)
CACHE_USERS_SECONDS = int(os.getenv("CACHE_USERS_SECONDS", "60"))
user_cache = {
    "data": None,
    "timestamp": 0
}
print(f"[PERF] ✅ Cache users: {CACHE_USERS_SECONDS}s")
print(f"[QR API] ✅ Base URL: {QR_API_BASE}")

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
TAB_LOGS_QR     = "LogsQR"  # Log riêng cho QR login

COL_NOTE_INDEX  = 5

# =========================================================
# LIMIT CONFIG
# =========================================================
FREE_LIMIT_PER_DAY = 10
SPAM_LIMIT_PER_MIN = 20
QR_COOLDOWN_SECONDS = 60  # 60 giây giữa các lần tạo QR

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
spam_lock = threading.Lock()

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


def normalize_tele_id(val: Any) -> str:
    """Chuẩn hoá Tele ID để so sánh (tránh lỗi: khoảng trắng, .0, dạng scientific 1.23E9)."""
    s = safe_text(val).strip()
    if not s:
        return ""
    # strip common float suffix
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    # if scientific notation like 1.999E9 -> keep digits
    digits = re.sub(r"\D", "", s)
    return digits or s

# =========================================================
# 🔥 CHECK SỐ ĐIỆN THOẠI SHOPEE ZIN
# =========================================================

# Config Google Sheet cho Cookie check số
GS_COOKIE_SHEET_ID = os.getenv("GOOGLE_SHEET_COOKIE_ID", "").strip()
GS_COOKIE_TAB = os.getenv("GOOGLE_SHEET_COOKIE_TAB", "Cookie").strip()
PRIMARY_POOL_SIZE = 6  # Số cookie tối đa lấy từ sheet

def _gs_read_live_cookies() -> List[str]:
    """
    Đọc cookies từ Google Sheet để check số
    Mặc định đọc từ tab "Cookie" trong sheet chính (SHEET_ID)
    Nếu set GOOGLE_SHEET_COOKIE_ID thì đọc từ sheet riêng
    """
    try:
        # Nếu có sheet riêng cho cookie thì dùng sheet đó
        if GS_COOKIE_SHEET_ID and GS_COOKIE_SHEET_ID != SHEET_ID:
            cookie_sheet = gc.open_by_key(GS_COOKIE_SHEET_ID)
            ws = cookie_sheet.worksheet(GS_COOKIE_TAB or "Cookie")
        else:
            # Dùng sheet chính, tab Cookie
            try:
                ws = sh.worksheet("Cookie")
            except Exception:
                # Tab Cookie chưa có → tạo mới
                ws = sh.add_worksheet("Cookie", rows=100, cols=2)
                ws.update('A1', [['Cookie']])
                return []
        
        col = ws.col_values(1) or []
    except Exception as e:
        print(f"[ERROR] _gs_read_live_cookies: {e}")
        return []
    
    if col and col[0].strip().lower() == "cookie":
        col = col[1:]
    
    seen, out = set(), []
    for c in col:
        c = (c or "").strip()
        if not c:
            continue
        if "SPC_ST=" not in c and "=" not in c:
            continue
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
    
    if not out:
        print("[WARN] No cookies found in sheet")
        return []
    
    random.shuffle(out)
    return out[:PRIMARY_POOL_SIZE]

def normalize_phone_to_84(raw: str) -> str:
    """Chuẩn hóa số điện thoại về dạng 84xxxxxxxxx"""
    if not isinstance(raw, str):
        return None
    
    digits = "".join(ch for ch in raw if ch.isdigit())
    
    if digits.startswith("84"):
        core = digits[2:]
    elif digits.startswith("0"):
        core = digits[1:]
    else:
        core = digits[-9:] if len(digits) >= 9 else digits
    
    if len(core) != 9 or not core.isdigit():
        return None
    
    return "84" + core

def is_phone_number(text: str) -> bool:
    """
    Kiểm tra có phải số điện thoại không
    Hỗ trợ 3 dạng:
    - 84912345678 (11 số)
    - 0912345678 (10 số)
    - 912345678 (9 số)
    """
    if not text:
        return False
    
    # Lấy chỉ các chữ số
    digits = "".join(ch for ch in text if ch.isdigit())
    
    # Kiểm tra độ dài (9, 10, hoặc 11 số)
    if len(digits) < 9 or len(digits) > 11:
        return False
    
    # Kiểm tra prefix hợp lệ
    if len(digits) == 11:
        # Format: 84xxxxxxxxx
        return digits.startswith("84")
    elif len(digits) == 10:
        # Format: 0xxxxxxxxx
        return digits.startswith("0")
    elif len(digits) == 9:
        # Format: xxxxxxxxx (không có số 0 đầu)
        return not digits.startswith("0")
    
    return False

def extract_phone_numbers(text: str) -> List[str]:
    """
    Trích xuất tất cả số điện thoại từ text
    Hỗ trợ nhiều số trên nhiều dòng
    """
    lines = text.split('\n')
    phones = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Kiểm tra từng dòng có phải số không
        if is_phone_number(line):
            phones.append(line)
    
    return phones

def check_shopee_phone_api(cookie: str, phone84: str) -> tuple:
    """
    Check số điện thoại qua API Shopee
    Returns: (req_ok, is_zin, error_code, note)
    
    Logic API Shopee (từ appv2.py):
    - error = 12301116 → is_ok = False → KHÔNG ZIN (đã đăng ký)
    - error != 12301116 → is_ok = True → ZIN (chưa đăng ký)
    """
    url = "https://shopee.vn/api/v4/account/management/check_unbind_phone"
    
    headers = {
        "User-Agent": UA,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Cookie": cookie.strip(),
    }
    
    payload = {
        "phone": phone84,
        "device_sz_fingerprint": os.getenv("SHOPEE_FINGERPRINT", "")
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=4)
        
        # Log để debug
        print(f"[CHECK] Phone: {phone84}")
        print(f"[CHECK] Status: {response.status_code}")
        
        if response.status_code in (401, 403):
            return False, False, response.status_code, "Cookie hết hạn"
        
        if response.status_code != 200:
            return False, False, response.status_code, f"HTTP {response.status_code}"
        
        try:
            data = response.json()
            print(f"[CHECK] Response: {data}")  # Log full response
        except Exception:
            return False, False, -1, "JSON parse error"
        
        if not isinstance(data, dict):
            return False, False, -1, "Invalid response"
        
        error_code = data.get("error")
        print(f"[CHECK] Error code: {error_code}")
        
        # ✅ LOGIC CHÍNH XÁC từ appv2.py line 875-877
        # CHỈ CÓ error = 12301116 mới là KHÔNG ZIN
        if error_code == 12301116:
            return True, False, error_code, "Đã đăng ký Shopee"
        
        # ✅ Tất cả các trường hợp khác đều là ZIN
        return True, True, error_code, f"Chưa đăng ký (error={error_code})"
        
    except requests.exceptions.Timeout:
        return False, False, -1, "Timeout"
    except Exception as e:
        print(f"[CHECK] Exception: {e}")
        return False, False, -1, f"Error: {str(e)}"

def check_shopee_phone_with_sheet_cookies(phone: str, cookies: List[str]) -> tuple:
    """
    Check số điện thoại với cookies từ Google Sheet
    Returns: (success, is_zin, note)
    """
    phone84 = normalize_phone_to_84(phone)
    if not phone84:
        return False, False, "Số không hợp lệ"
    
    if not cookies:
        return False, False, "Không có cookie"
    
    # Thử tối đa 2 cookie
    for cookie in cookies[:2]:
        req_ok, is_zin, error_code, note = check_shopee_phone_api(cookie, phone84)
        
        if not req_ok:
            continue  # Thử cookie tiếp
        
        return True, is_zin, note
    
    return False, False, "Cookies lỗi"

def check_multiple_phones(phones: List[str]) -> List[dict]:
    """
    Check nhiều số cùng lúc (max 10 số)
    Returns: [{"phone": "0xxx", "success": True/False, "is_zin": True/False, "note": "..."}]
    """
    # Giới hạn 10 số
    phones = phones[:10]
    
    # Đọc cookies từ Google Sheet
    cookies = _gs_read_live_cookies()
    
    if not cookies:
        return [{
            "phone": p, 
            "success": False, 
            "is_zin": False, 
            "note": "Không có cookie trong sheet"
        } for p in phones]
    
    results = []
    
    for phone in phones:
        success, is_zin, note = check_shopee_phone_with_sheet_cookies(phone, cookies)
        
        results.append({
            "phone": phone,
            "success": success,
            "is_zin": is_zin,
            "note": note
        })
        
        # Delay nhẹ giữa các request
        time.sleep(0.3)
    
    return results

# =========================================================
# 🔥 QR LOGIN FUNCTIONS
# =========================================================
def create_qr_session(user_id: int) -> Tuple[bool, str, str]:
    """Tạo QR session mới"""
    try:
        response = requests.post(
            f"{QR_API_BASE}/api/qr/create",
            json={"user_id": user_id},
            timeout=10
        )

        if response.status_code != 200:
            return False, f"API error: {response.status_code}", ""

        data = response.json()

        if not data.get("success"):
            error_msg = data.get("error", "Unknown error")
            return False, f"Create QR failed: {error_msg}", ""

        session_id = data.get("session_id")
        qr_image = data.get("qr_image", "").replace("data:image/png;base64,", "")

        # Lưu session (lock)
        with qr_lock:
            qr_sessions[session_id] = {
                "user_id": user_id,
                "created": time.time(),
                "status": "waiting",  # waiting, scanned, done, expired
                "qr_image": qr_image,
                "cookie": ""
            }

        return True, session_id, qr_image

    except Exception as e:
        return False, f"Error: {str(e)}", ""

def check_qr_status(session_id: str) -> Tuple[bool, str, bool, Optional[str], Optional[str]]:
    """
    Kiểm tra trạng thái QR
    Returns: (ok, status, has_token, cookie_st, cookie_f)
    """
    with qr_lock:
        if session_id not in qr_sessions:
            return False, "NOT_FOUND", False, None, None
        session = qr_sessions[session_id]

    # Check timeout
    if time.time() - session["created"] > QR_TIMEOUT:
        with qr_lock:
            if session_id in qr_sessions:
                qr_sessions[session_id]["status"] = "expired"
        return False, "EXPIRED", False, None, None

    try:
        response = requests.get(
            f"{QR_API_BASE}/api/qr/status/{session_id}",
            timeout=5
        )

        if response.status_code != 200:
            return False, f"API_ERROR_{response.status_code}", False, None, None

        data = response.json()

        if not data.get("success"):
            return False, data.get("status", "UNKNOWN"), False, None, None

        status = data.get("status", "")
        has_token = data.get("has_token", False)
        cookie_st = data.get("cookie_st")
        cookie_f = data.get("cookie_f")

        if status == "SCANNED" or has_token:
            with qr_lock:
                if session_id in qr_sessions:
                    qr_sessions[session_id]["status"] = "scanned"
            return True, "SCANNED", has_token, cookie_st, cookie_f
        elif status == "NOT_FOUND":
            with qr_lock:
                if session_id in qr_sessions:
                    qr_sessions[session_id]["status"] = "expired"
            return False, "EXPIRED", False, None, None
        else:
            return True, status, has_token, None, None

    except Exception:
        return False, "CHECK_ERROR", False, None, None

def get_qr_cookie(session_id: str) -> Tuple[bool, str, Optional[str], Optional[dict]]:
    """
    Lấy cookie sau khi quét QR thành công
    Returns: (success, cookie_st/error_msg, cookie_f, user_info)
    """
    with qr_lock:
        if session_id not in qr_sessions:
            return False, "Session not found", None, None
        session = qr_sessions[session_id]
        # ✅ FIX: Nếu đã có cookie thì trả luôn (không gọi API lại)
        if session.get("cookie"):
            return True, session["cookie"], session.get("cookie_f"), session.get("user_info")

    try:
        response = requests.post(
            f"{QR_API_BASE}/api/qr/login/{session_id}",
            timeout=10
        )

        if response.status_code != 200:
            return False, f"API error: {response.status_code}", None, None

        data = response.json()

        if not data.get("success"):
            error_msg = data.get("error", "Login failed")
            return False, error_msg, None, None

        cookie_st = data.get("cookie", "")
        cookie_f = data.get("cookie_f", "")
        
        # ✅ Debug log
        print(f"[QR] API Response - cookie_st: {cookie_st[:50] if cookie_st else 'None'}...")
        print(f"[QR] API Response - cookie_f: {cookie_f[:50] if cookie_f else 'None'}")
        
        if not cookie_st:
            return False, "No cookie returned", None, None

        # ✅ Lấy thông tin user
        user_info = None
        try:
            headers = {
                "Cookie": cookie_st,
                "User-Agent": "Mozilla/5.0"
            }
            response = requests.get(
                "https://shopee.vn/api/v4/account/basic/get_account_info",
                headers=headers,
                timeout=5
            )
            if response.status_code == 200:
                user_data = response.json()
                if user_data.get("data"):
                    user_info = {
                        "username": user_data["data"].get("username", "N/A"),
                        "user_id": user_data["data"].get("userid", "N/A")
                    }
        except Exception:
            pass

        # Lưu cookie vào session (lock)
        with qr_lock:
            if session_id in qr_sessions:
                qr_sessions[session_id]["cookie"] = cookie_st
                qr_sessions[session_id]["cookie_f"] = cookie_f
                qr_sessions[session_id]["user_info"] = user_info
                qr_sessions[session_id]["status"] = "done"

        return True, cookie_st, cookie_f, user_info

    except Exception as e:
        return False, f"Error: {str(e)}", None, None

def cleanup_qr_sessions():
    """Dọn session QR cũ"""
    current_time = time.time()
    expired_sessions = []

    with qr_lock:
        for session_id, session in list(qr_sessions.items()):
            if current_time - session["created"] > QR_TIMEOUT:
                expired_sessions.append(session_id)

        for session_id in expired_sessions:
            qr_sessions.pop(session_id, None)

    return len(expired_sessions)

# =========================================================
# 🔥 FIX 2: CACHE COOKIE FUNCTIONS
# =========================================================
def get_cached_orders(cookie: str):
    """Lấy kết quả đã cache theo cookie"""
    with cache_lock:
        item = order_cache.get(cookie)
        if not item:
            return None

        # Kiểm tra TTL
        if time.time() - item["time"] > CACHE_COOKIE_TTL:
            # Cache hết hạn
            order_cache.pop(cookie, None)
            return None

        return item["data"]

def set_cached_orders(cookie: str, data):
    """Lưu kết quả vào cache"""
    with cache_lock:
        order_cache[cookie] = {
            "data": data,
            "time": time.time()
        }

def clear_expired_cache():
    """Dọn cache cũ (chạy định kỳ)"""
    current_time = time.time()
    with cache_lock:
        expired = [
            k for k, v in list(order_cache.items())
            if current_time - v["time"] > CACHE_COOKIE_TTL
        ]
        for k in expired:
            order_cache.pop(k, None)

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
    buffer_qr = []
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
            elif log_type == "qr":
                buffer_qr.append(data)

        except Exception:
            # Timeout → Không có item mới
            pass

        # Kiểm tra điều kiện flush
        current_time = time.time()
        should_flush = (
            len(buffer_check) >= LOG_BATCH_SIZE or
            len(buffer_spam) >= LOG_BATCH_SIZE or
            len(buffer_qr) >= LOG_BATCH_SIZE or
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

            # Flush buffer_qr
            if buffer_qr:
                try:
                    ws_log_qr.append_rows(
                        buffer_qr,
                        value_input_option="USER_ENTERED"
                    )
                    print(f"[LOG] Flushed {len(buffer_qr)} QR logs")
                except Exception as e:
                    print(f"[LOG] Error flushing QR: {e}")
                buffer_qr.clear()

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

    # ✅ FIX: tách rõ result và error
    result_html, err = check_shopee_orders(cookie)

    if err:
        if err == "cookie_expired":
            return False, "❌ Cookie hết hạn hoặc không hợp lệ", balance
        if err == "no_orders":
            return False, "📭 Không có đơn hàng nào", balance
        return False, f"❌ Check cookie thất bại ({err})", balance

    if not result_html:
        return False, "❌ Check cookie thất bại", balance

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

ws_log_qr = get_or_create_worksheet(
    TAB_LOGS_QR,
    ["time", "Tele ID", "username", "session_id", "status", "balance_sau", "note"]
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
    tele_id = normalize_tele_id(tele_id)

    try:
        # Lấy RAW data từ cache (không dùng get_all_records vì có header trùng)
        try:
            values = ws_user.get_all_values()
        except Exception:
            return None, None

        if not values or len(values) < 2:
            return None, None

        # Duyệt từng row (bỏ qua header)
        for idx, row in enumerate(values[1:], start=2):
            if not row or len(row) < 4:  # Cần ít nhất 4 cột
                continue

            # Đọc theo INDEX
            row_tele_id = normalize_tele_id(row[0]) if len(row) > 0 else ""  # Cột A
            row_username = safe_text(row[1]) if len(row) > 1 else ""  # Cột B
            row_balance = safe_text(row[2]) if len(row) > 2 else "0"  # Cột C
            row_status = safe_text(row[3]) if len(row) > 3 else ""    # Cột D
            row_note = safe_text(row[4]) if len(row) > 4 else ""      # Cột E

            # So sánh Tele ID
            if row_tele_id and tele_id and row_tele_id == tele_id:
                # Return normalized data
                user_data = {
                    "Tele ID": row_tele_id,
                    "username": row_username,
                    "balance": row_balance,
                    "trang thai": row_status.lower().strip(),  # Normalize status
                    "ghi chu": row_note
                }

                return idx, user_data

    except Exception as e:
        print(f"[ERROR] get_user_row exception: {e}")
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

def log_qr(tele_id: Any, username: str, session_id: str, status: str, balance_after: int, note: str) -> None:
    """✅ LOG QR: Đẩy vào queue"""
    log_queue.put({
        "type": "qr",
        "data": [
            now().strftime("%Y-%m-%d %H:%M:%S"),
            safe_text(tele_id),
            username or "",
            session_id,
            status,
            balance_after,
            note
        ]
    })

def count_today_request(tele_id: Any) -> int:
    tele_id = normalize_tele_id(tele_id)
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

def tg_send_photo(chat_id: Any, photo_base64: str, caption: str = "", keyboard: Optional[Dict[str, Any]] = None) -> None:
    """Gửi ảnh từ base64 (hỗ trợ inline keyboard)"""
    try:
        # Decode base64
        photo_bytes = base64.b64decode(photo_base64)

        # Tạo file object
        files = {"photo": ("qr.png", photo_bytes, "image/png")}

        payload = {
            "chat_id": chat_id,
            "caption": caption,
            "parse_mode": "HTML"
        }

        # ⚠️ Với multipart/form-data, reply_markup nên là JSON string
        if keyboard:
            payload["reply_markup"] = json.dumps(keyboard, ensure_ascii=False)

        requests.post(f"{BASE_URL}/sendPhoto", data=payload, files=files, timeout=15)
    except Exception as e:
        print(f"[ERROR] Send photo failed: {e}")
        # Fallback gửi text
        tg_send(chat_id, f"📷 {caption}\\n\\n❌ Không thể gửi ảnh QR, vui lòng thử lại.")

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
            ["🔑 Get Cookie QR", "📘 Hướng dẫn"],
            ["💳 Nạp Tiền", "🧩 Hệ Thống Bot NgânMiu"]
        ],
        "resize_keyboard": True
    }

def get_cookie_keyboard():
    """Keyboard khi đang chờ quét QR"""
    return {
        "keyboard": [
            ["🔄 Check QR Status", "❌ Cancel QR"]
        ],
        "resize_keyboard": True
    }

def inline_qr_keyboard(session_id: str) -> Dict[str, Any]:
    """Inline keyboard nằm ngay dưới ảnh QR"""
    sid = safe_text(session_id)
    return {
        "inline_keyboard": [
            [
                {"text": "🔄 Check QR Status", "callback_data": f"QR_CHECK|{sid}"},
                {"text": "❌ Cancel QR", "callback_data": f"QR_CANCEL|{sid}"}
            ]
        ]
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

    # ================= INLINE QR BUTTONS =================
    if action.startswith("QR_CHECK|"):
        sid = action.split("|", 1)[1].strip()
        handle_check_qr_status(chat_id, tele_id, username, sid)
        return

    if action.startswith("QR_CANCEL|"):
        sid = action.split("|", 1)[1].strip()
        handle_cancel_qr(chat_id, tele_id, username, sid)
        return

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
            "3) Bấm <b>🔑 Get Cookie QR</b> để lấy cookie qua QR\n\n"
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
                    "need_order_response": 1,
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
        if not data or (isinstance(data, dict) and len(data.keys()) <= 2):
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
                "need_order_response": 1,
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
        if not data or (isinstance(data, dict) and len(data.keys()) <= 2):
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
        blocks = []
        for d in cached:
            if isinstance(d, dict):
                blocks.append(format_order_simple(d))
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
    for d in details:
        if isinstance(d, dict):
            blocks.append(format_order_simple(d))

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

        for rec in records:
            ts = rec.get("actual_time")
            if not ts:
                continue

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
# 📢 THÔNG BÁO SYSTEM (ADMIN ONLY) - 3 LỚP BẢO VỆ
# =========================================================
ADMIN_IDS = [
    1359771167,  # BonBonxHPx
]

# =========================================================
# BROADCAST STATE MANAGEMENT (Serverless-safe)
# =========================================================
IS_BROADCASTING = False  # Lock để chặn broadcast song song

def get_broadcast_sheet():
    """Get or create BroadcastState sheet"""
    try:
        try:
            return sh.worksheet("BroadcastState")
        except Exception:
            ws = sh.add_worksheet("BroadcastState", 100, 4)
            ws.update('A1:D1', [['Timestamp', 'AdminID', 'Status', 'MessageID']])
            return ws
    except Exception as e:
        print(f"[ERROR] get_broadcast_sheet: {e}")
        return None

def get_last_broadcast_time_from_sheet():
    """Lấy thời gian broadcast gần nhất từ sheet"""
    ws = get_broadcast_sheet()
    if not ws:
        return None
    try:
        all_values = ws.get_all_values()
        if len(all_values) <= 1:
            return None

        for row in reversed(all_values[1:]):
            if len(row) >= 3 and row[2] in ["STARTED", "COMPLETED"]:
                timestamp_str = row[0]
                dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                return dt.timestamp()

        return None
    except Exception as e:
        print(f"[ERROR] get_last_broadcast_time_from_sheet: {e}")
        return None

def set_broadcast_state_to_sheet(admin_id, status, message_id=""):
    """Lưu broadcast state vào sheet"""
    ws = get_broadcast_sheet()
    if not ws:
        return False
    try:
        ws.append_row([
            now().strftime("%Y-%m-%d %H:%M:%S"),
            str(admin_id),
            status,
            str(message_id)
        ])
        print(f"[BROADCAST] State saved: {status}")
        return True
    except Exception as e:
        print(f"[ERROR] set_broadcast_state_to_sheet: {e}")
        return False

def is_broadcast_message_processed(message_id):
    """LỚP 1: Check message_id đã từng broadcast chưa"""
    if not message_id:
        return False

    ws = get_broadcast_sheet()
    if not ws:
        return False

    try:
        col_message_ids = ws.col_values(4)
        return str(message_id) in col_message_ids
    except Exception as e:
        print(f"[ERROR] is_broadcast_message_processed: {e}")
        return False

def check_broadcast_cooldown_from_sheet():
    """LỚP 2: Check cooldown từ sheet (serverless-safe)"""
    last_time = get_last_broadcast_time_from_sheet()
    if not last_time:
        return True, 0

    current_time = time.time()
    time_since_last = current_time - last_time

    BROADCAST_COOLDOWN = 60
    print(f"[BROADCAST] Time since last: {time_since_last:.1f}s")

    if time_since_last < BROADCAST_COOLDOWN:
        wait_time = int(BROADCAST_COOLDOWN - time_since_last)
        return False, wait_time

    return True, 0

def handle_thongbao(chat_id: Any, tele_id: Any, username: str, text: str, message_id: int) -> None:
    """3 lớp bảo vệ broadcast"""
    global IS_BROADCASTING

    if tele_id not in ADMIN_IDS:
        tg_send(chat_id, "❌ <b>KHÔNG CÓ QUYỀN</b>\n\nChỉ admin mới được sử dụng lệnh này.")
        return

    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        tg_send(
            chat_id,
            "📢 <b>HƯỚNG DẪN GỬI THÔNG BÁO</b>\n\n"
            "<b>Cú pháp:</b>\n"
            "<code>/thongbao Nội dung thông báo</code>\n\n"
            "<b>Ví dụ:</b>\n"
            "<code>/thongbao Hệ thống bảo trì từ 22h-23h tối nay</code>\n\n"
            "💡 <b>Lưu ý:</b>\n"
            "• Hỗ trợ HTML: &lt;b&gt;bold&lt;/b&gt;, &lt;i&gt;italic&lt;/i&gt;\n"
            "• Chống spam: 3 lớp bảo vệ tự động"
        )
        return

    message_content = parts[1].strip()

    if is_broadcast_message_processed(message_id):
        tg_send(
            chat_id,
            "⚠️ <b>THÔNG BÁO NÀY ĐÃ ĐƯỢC GỬI</b>\n\n"
            "Bot đã tự động bỏ qua để tránh gửi lặp.\n\n"
            "<i>Hệ thống phát hiện message_id trùng lặp.</i>"
        )
        print(f"[BROADCAST] ❌ BLOCKED - Duplicate message_id: {message_id}")
        return

    can_broadcast, wait_time = check_broadcast_cooldown_from_sheet()
    if not can_broadcast:
        tg_send(
            chat_id,
            f"⏳ <b>VUI LÒNG ĐỢI {wait_time}s</b>\n\n"
            f"🔒 Broadcast gần đây chưa đủ thời gian cooldown\n\n"
            f"<i>Hệ thống tự động chống spam broadcast.</i>"
        )
        print(f"[BROADCAST] ❌ BLOCKED - Cooldown: {wait_time}s")
        return

    if IS_BROADCASTING:
        tg_send(chat_id, "⛔ <b>ĐANG CÓ BROADCAST KHÁC CHẠY</b>\n\nVui lòng đợi broadcast trước hoàn tất.")
        print(f"[BROADCAST] ❌ BLOCKED - Already broadcasting")
        return

    IS_BROADCASTING = True

    try:
        try:
            values = ws_user.get_all_values()
        except Exception:
            IS_BROADCASTING = False
            tg_send(chat_id, "❌ Không thể đọc danh sách users từ Sheet")
            return

        if not values or len(values) < 2:
            IS_BROADCASTING = False
            tg_send(chat_id, "❌ Không tìm thấy user nào trong Sheet")
            return

        total_users = len(values) - 1

        if not set_broadcast_state_to_sheet(tele_id, "STARTED", message_id):
            IS_BROADCASTING = False
            tg_send(chat_id, "❌ Lỗi khi lưu trạng thái broadcast")
            return

        tg_send(
            chat_id,
            f"📢 <b>ĐANG GỬI THÔNG BÁO...</b>\n\n"
            f"👥 Tổng số users: <b>{total_users}</b>\n"
            f"⏱️ Thời gian ước tính: ~{total_users * 0.1:.0f}s\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"{message_content}\n"
            f"━━━━━━━━━━━━━━━"
        )

        success_count = 0
        fail_count = 0
        sent_to = set()

        for row in values[1:]:
            if not row or len(row) < 1:
                continue

            user_tele_id = safe_text(row[0])
            if not user_tele_id or not user_tele_id.isdigit():
                continue

            if user_tele_id in sent_to:
                continue

            try:
                full_message = (
                    f"📢 <b>THÔNG BÁO TỪ ADMIN</b>\n"
                    f"━━━━━━━━━━━━━━━\n\n"
                    f"{message_content}\n\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"<i>Từ: NgânMiu.Store Bot System</i>"
                )

                tg_send(user_tele_id, full_message)
                sent_to.add(user_tele_id)
                success_count += 1
                time.sleep(0.05)

            except Exception as e:
                fail_count += 1
                print(f"[BROADCAST] Failed to send to {user_tele_id}: {e}")

        set_broadcast_state_to_sheet(tele_id, "COMPLETED", message_id)

        tg_send(
            chat_id,
            f"✅ <b>GỬI THÔNG BÁO HOÀN TẤT</b>\n\n"
            f"📊 <b>Kết quả:</b>\n"
            f"• Thành công: {success_count} users\n"
            f"• Thất bại: {fail_count} users\n"
            f"• Tổng cộng: {total_users} users"
        )

    except Exception as e:
        set_broadcast_state_to_sheet(tele_id, "FAILED", message_id)
        tg_send(chat_id, f"❌ <b>LỖI GỬI THÔNG BÁO</b>\n\n{str(e)}")
        traceback.print_exc()

    finally:
        IS_BROADCASTING = False

# =========================================================
# 🔑 GET COOKIE QR HANDLER
# =========================================================
def handle_get_cookie_qr(chat_id: Any, tele_id: Any, username: str) -> None:
    """Xử lý khi user bấm nút Get Cookie QR"""

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
        tg_send(chat_id, "🚫 <b>Tài khoản đang bị khóa</b>\n\n" f"⏱️ Mở lại lúc: <b>{until.strftime('%H:%M %d/%m')}</b>")
        return

    # Payment
    balance = get_balance(user)
    if BOT1_API_URL and PRICE_GET_COOKIE > 0:
        success, current_balance, error = check_balance_bot1(tele_id)
        if not success:
            tg_send(chat_id, f"❌ Lỗi check số dư: {error}")
            return
        if current_balance < PRICE_GET_COOKIE:
            tg_send(chat_id, format_insufficient_balance_msg(current_balance, PRICE_GET_COOKIE))
            return

    # Cooldown QR (60s) — lấy session gần nhất trong RAM
    current_time = time.time()
    with qr_lock:
        user_sessions = [s for s in qr_sessions.values() if s.get("user_id") == tele_id]

    if user_sessions:
        latest_session = max(user_sessions, key=lambda x: x.get("created", 0))
        time_since_last = current_time - latest_session.get("created", 0)
        if time_since_last < QR_COOLDOWN_SECONDS:
            wait_time = int(QR_COOLDOWN_SECONDS - time_since_last)
            tg_send(chat_id, f"⏳ <b>VUI LÒNG ĐỢI {wait_time}s</b>\n\nChờ {wait_time} giây nữa trước khi tạo QR mới.")
            return

    tg_send(chat_id, "🔄 <b>Đang tạo mã QR đăng nhập Shopee...</b>")

    success, session_id, qr_image = create_qr_session(tele_id)
    if not success:
        tg_send(chat_id, f"❌ <b>Lỗi tạo QR:</b>\n{session_id}", main_keyboard())
        return

    caption = (
        "🔑 <b>QR LOGIN SHOPEE</b>\n\n"
        "1️⃣ <b>Mở app Shopee</b>\n"
        "2️⃣ <b>Ở Trang Chủ - Góc trên bên trái - Ô Vuông cạnh Shopee Pay - Bấm vào để Quét QR</b>\n"
        "3️⃣ <b>Quét mã bên dưới</b>\n\n"
        "⚠️ QR có hiệu lực trong <b>5 phút</b>\n"
        "🤖 Bot sẽ <b>tự kiểm tra</b> mỗi <b>3 giây</b> và tự trả cookie sau khi bạn quét.\n"
        "👉 Nếu chưa thấy trả cookie, bấm <b>🔄 Check QR Status</b> ngay dưới ảnh"
    )

    try:
        tg_send_photo(chat_id, qr_image, caption, keyboard=inline_qr_keyboard(session_id))
    except Exception:
        tg_send(chat_id, f"{caption}\n\n❌ <b>Không thể tạo ảnh QR, vui lòng thử lại sau.</b>")

    log_qr(tele_id, username, session_id, "created", (current_balance if BOT1_API_URL else balance), "QR created")

    # Lưu thêm thông tin để auto watcher có thể gửi lại cookie
    with qr_lock:
        if session_id in qr_sessions:
            qr_sessions[session_id]["chat_id"] = chat_id
            qr_sessions[session_id]["username"] = username
            qr_sessions[session_id]["cancelled"] = False
            qr_sessions[session_id]["paid"] = False
            qr_sessions[session_id]["fee"] = PRICE_GET_COOKIE

    # ✅ AUTO (FAST): chờ nhanh trong CHÍNH request này (giúp serverless trả cookie nhanh nếu bạn quét liền)
    if AUTO_QR:
        try:
            tg_send(
                chat_id,
                f"🤖 <b>Auto đang bật</b> — bot đang chờ bạn quét trong <b>{AUTO_QR_FAST_SECONDS}s</b>...\\n\\n"
                f"<i>Nếu bạn quét muộn hơn, vẫn có thể bấm 🔄 Check QR Status.</i>"
            )

            started_fast = time.time()
            while time.time() - started_fast < AUTO_QR_FAST_SECONDS:
                ok, status, has_token = check_qr_status(session_id)
                st = (status or "").strip().upper()

                if ok and (has_token or st in SCANNED_STATUSES or (st and st not in PENDING_STATUSES)):
                    ok2, cookie2 = get_qr_cookie(session_id)
                    if ok2 and cookie2:
                        _send_cookie_success(chat_id, tele_id, username, session_id, cookie2)
                        return

                time.sleep(QR_POLL_INTERVAL)

            # ✅ AUTO (BG): fallback thread (chỉ ổn định khi bot chạy server luôn-on)
            t = threading.Thread(target=_auto_watch_qr_and_send_cookie, args=(session_id,), daemon=True)
            t.start()

        except Exception:
            pass


    tg_send(
        chat_id,
        "📱 <b>Sau khi quét QR trên app Shopee:</b>\n"
        "👉 Bấm <b>🔄 Check QR Status</b> để kiểm tra\n"
        "👉 Bấm <b>❌ Cancel QR</b> để hủy",
        get_cookie_keyboard()
    )

def _auto_watch_qr_and_send_cookie(session_id: str):
    """
    ✅ RÚT GỌN: Tự động poll QR → lấy cookie → trả về user
    - Đợi 3s rồi gửi thông báo nhắc quét
    - Check mỗi 3s cho đến khi quét xong hoặc timeout
    """
    try:
        # ✅ Đợi 3 giây trước khi gửi thông báo
        time.sleep(3)
        
        with qr_lock:
            sess = qr_sessions.get(session_id)
        
        if not sess or sess.get("cancelled"):
            return
            
        chat_id = sess.get("chat_id")
        
        # ✅ Gửi thông báo nhắc quét (RÚT GỌN)
        tg_send(
            chat_id,
            "⏳ <b>VUI LÒNG QUÉT MÃ QR</b>\n\n"
            "📱 Mở Shopee App → Quét QR\n"
            "⚠️ QR có hiệu lực trong 5 phút"
        )
        
        started = time.time()
        last_login_try = 0

        while True:
            # Timeout tổng
            if time.time() - started > AUTO_QR_MAX_SECONDS:
                with qr_lock:
                    sess = qr_sessions.get(session_id)
                if sess:
                    tg_send(
                        sess.get("chat_id"), 
                        "⏰ <b>HẾT THỜI GIAN</b>\n\n"
                        "❌ QR đã hết hiệu lực (5 phút)\n"
                        "👉 Vui lòng tạo QR mới", 
                        main_keyboard()
                    )
                    log_qr(sess.get("user_id"), sess.get("username",""), session_id, "expired", 0, "Auto timeout")
                    with qr_lock:
                        qr_sessions.pop(session_id, None)
                return

            with qr_lock:
                sess = qr_sessions.get(session_id)

            if not sess or sess.get("cancelled"):
                return

            tele_id = sess.get("user_id")
            chat_id = sess.get("chat_id")
            username = sess.get("username") or ""

            # Check status (giờ có thêm cookie_st và cookie_f)
            ok, status, has_token, cookie_st, cookie_f = check_qr_status(session_id)

            # Nếu API status lỗi, thử login thưa thớt
            if not ok and status in ("API_ERROR", "CHECK_ERROR"):
                if time.time() - last_login_try > 2:
                    last_login_try = time.time()
                    ok2, cookie2, cookie_f2, user_info2 = get_qr_cookie(session_id)
                    if ok2 and cookie2:
                        _send_cookie_success(chat_id, tele_id, username, session_id, cookie2, cookie_f2, user_info2)
                        return
                time.sleep(QR_POLL_INTERVAL)
                continue

            if not ok and status == "EXPIRED":
                tg_send(
                    chat_id, 
                    "⏰ <b>HẾT THỜI GIAN</b>\n\n"
                    "❌ QR đã hết hiệu lực (5 phút)\n"
                    "👉 Vui lòng tạo QR mới", 
                    main_keyboard()
                )
                log_qr(tele_id, username, session_id, "expired", 0, "Expired")
                with qr_lock:
                    qr_sessions.pop(session_id, None)
                return

            # Nếu đã quét
            st = (status or "").strip().upper()
            if ok and (has_token or st in SCANNED_STATUSES or (st and st not in PENDING_STATUSES)):
                ok2, cookie, cookie_f, user_info = get_qr_cookie(session_id)
                if ok2 and cookie:
                    _send_cookie_success(chat_id, tele_id, username, session_id, cookie, cookie_f, user_info)
                    return

            time.sleep(QR_POLL_INTERVAL)

    except Exception as e:
        try:
            with qr_lock:
                sess = qr_sessions.get(session_id)
            if sess:
                tg_send(
                    sess.get("chat_id"), 
                    f"❌ <b>Lỗi theo dõi QR</b>\n\n{esc(str(e))}\n\n"
                    "👉 Bạn có thể bấm <b>🔄 Check QR Status</b> để thử lại.", 
                    get_cookie_keyboard()
                )
        except Exception:
            pass


def _send_cookie_success(chat_id: Any, tele_id: Any, username: str, session_id: str, 
                        cookie_st: str, cookie_f: Optional[str] = None, 
                        user_info: Optional[dict] = None) -> None:
    """
    ✅ Gửi cookie thành công với thông tin đầy đủ:
    - Username và User ID
    - Cookie ST và Cookie F
    - Ngày hết hạn (7 ngày)
    - Lưu ý về voucher
    """
    fee = PRICE_GET_COOKIE if BOT1_API_URL else 0

    # Lấy config từ session (nếu có)
    with qr_lock:
        sess = qr_sessions.get(session_id, {}) if session_id else {}
    if sess:
        fee = safe_int(sess.get("fee"), fee)
        already_paid = bool(sess.get("paid"))
    else:
        already_paid = False

    balance_after = 0

    # ================= PAYMENT (chỉ khi bot1 active) =================
    if BOT1_API_URL and fee > 0 and not already_paid:
        ok_bal, bal, err = check_balance_bot1(tele_id)
        if not ok_bal:
            tg_send(
                chat_id,
                f"⚠️ <b>Lỗi hệ thống thanh toán:</b> {esc(err)}\n\n"
                "👉 Bạn có thể bấm <b>🔄 Check QR Status</b> để thử lại.",
                get_cookie_keyboard()
            )
            log_qr(tele_id, username, session_id, "pay_error", 0, f"check_balance error: {err}")
            return

        if bal < fee:
            tg_send(
                chat_id,
                format_insufficient_balance_msg(bal, fee) +
                "\n\n👉 Nạp xong, bấm <b>🔄 Check QR Status</b> để nhận cookie.",
                main_keyboard()
            )
            log_qr(tele_id, username, session_id, "no_money", bal, "insufficient for get_cookie")
            return

        ok_d, new_bal, err2 = deduct_balance_bot1(
            tele_id, fee, "Get Cookie QR Shopee (success)", username
        )
        if not ok_d:
            tg_send(
                chat_id,
                f"⚠️ <b>Không trừ được tiền:</b> {esc(err2)}\n\n"
                "👉 Bạn có thể bấm <b>🔄 Check QR Status</b> để thử lại.",
                get_cookie_keyboard()
            )
            log_qr(tele_id, username, session_id, "pay_fail", bal, f"deduct failed: {err2}")
            return

        balance_after = new_bal
        with qr_lock:
            if session_id in qr_sessions:
                qr_sessions[session_id]["paid"] = True

    elif BOT1_API_URL and fee > 0 and already_paid:
        # Đã thu tiền trước đó (user bấm lại) → lấy số dư mới nhất để hiển thị
        ok_bal, bal, _ = check_balance_bot1(tele_id)
        balance_after = bal if ok_bal else 0

    # ================= TÍNH NGÀY HẾT HẠN =================
    expiry_date = (datetime.now() + timedelta(days=COOKIE_VALIDITY_DAYS)).strftime("%d/%m/%Y")

    # ================= XÂY DỰNG MESSAGE =================
    message = "🎉 <b>LẤY COOKIE THÀNH CÔNG!</b>\n\n"
    
    # Thông tin user (nếu có)
    if user_info:
        message += f"👤 <b>User:</b> <code>{esc(user_info.get('username', 'N/A'))}</code>\n\n"
    
    # Cookie ST
    message += f"🍪 <b>Cookie ST:</b>\n<code>{esc(cookie_st)}</code>\n\n"
    
    # Cookie F (nếu có)
    if cookie_f:
        message += f"🍪 <b>Cookie F:</b>\n<code>{esc(cookie_f)}</code>\n\n"
    
    # Hướng dẫn copy
    message += "💡 <i>Tap vào cookie để auto copy</i>\n\n"
    
    # Hiệu lực và lưu ý
    message += (
        f"⏰ <b>Hiệu lực:</b> {COOKIE_VALIDITY_DAYS} ngày (đến {expiry_date})\n"
        "⚠️ Bảo mật tuyệt đối!\n\n"
        "━━━━━━━━━━━━━━━\n"
        "💡 <b>LƯU Ý:</b>\n"
        "• Để Lưu Voucher 100k:\n"
        "👉 @nganmiu_bot"
    )

    # Thêm thông tin phí (nếu có)
    if BOT1_API_URL and fee > 0:
        message += (
            f"\n\n━━━━━━━━━━━━━━━\n"
            f"💸 <b>Phí lấy cookie:</b> -{fee:,}đ\n"
            f"💰 <b>Số dư còn:</b> {balance_after:,}đ"
        )

    tg_send(chat_id, message, main_keyboard())

    log_qr(tele_id, username, session_id, "success", balance_after, "Cookie delivered")

    with qr_lock:
        qr_sessions.pop(session_id, None)

def handle_check_qr_status(chat_id: Any, tele_id: Any, username: str, session_id: Optional[str] = None) -> None:
    """Kiểm tra trạng thái QR (hỗ trợ inline button theo session_id)"""

    tele_id = int(tele_id) if safe_text(tele_id).isdigit() else tele_id

    # Lấy session hợp lệ (ưu tiên session_id được truyền vào)
    with qr_lock:
        if session_id and session_id in qr_sessions and qr_sessions[session_id].get("user_id") == tele_id:
            sid = session_id
        else:
            user_sessions = [s for s, sess in qr_sessions.items() if sess.get("user_id") == tele_id]
            sid = max(user_sessions, key=lambda s: qr_sessions[s].get("created", 0)) if user_sessions else None

    if not sid:
        tg_send(chat_id, "❌ <b>Không tìm thấy QR session</b>\n\nBấm <b>🔑 Get Cookie QR</b> để tạo QR mới.", main_keyboard())
        return

    # Nếu session đã có cookie (ví dụ: quét xong nhưng chưa thu phí/ chưa gửi) → gửi luôn
    with qr_lock:
        sess = qr_sessions.get(sid, {})
        cached_cookie = safe_text(sess.get("cookie", "")).strip()
        cached_cookie_f = sess.get("cookie_f")
        cached_user_info = sess.get("user_info")
        cancelled = bool(sess.get("cancelled", False))

    if cancelled:
        tg_send(chat_id, "❌ <b>QR đã bị hủy</b>\n\nBấm <b>🔑 Get Cookie QR</b> để tạo QR mới.", main_keyboard())
        with qr_lock:
            qr_sessions.pop(sid, None)
        return

    if cached_cookie:
        _send_cookie_success(chat_id, tele_id, username, sid, cached_cookie, cached_cookie_f, cached_user_info)
        return

    # ✅ Bỏ thông báo "Đang kiểm tra..." - check trực tiếp
    ok, status, has_token, cookie_st, cookie_f = check_qr_status(sid)

    if not ok:
        if status == "EXPIRED":
            tg_send(
                chat_id,
                "⏰ <b>HẾT THỜI GIAN</b>\n\n"
                "❌ QR đã hết hiệu lực (5 phút)\n"
                "👉 Bấm <b>🔑 Get Cookie QR</b> để tạo QR mới.",
                main_keyboard()
            )
            with qr_lock:
                qr_sessions.pop(sid, None)
        else:
            tg_send(chat_id, f"❌ <b>Lỗi kiểm tra QR:</b>\n{esc(status)}", get_cookie_keyboard())
        return

    st = (status or "").strip().upper()

    # Shopee status có thể rất nhiều biến thể → dùng mapping mềm
    if ok and (has_token or st in SCANNED_STATUSES or (st and st not in PENDING_STATUSES)):
        # ✅ Bỏ thông báo "QR đã được quét! Đang lấy cookie..." - lấy luôn
        ok2, cookie, cookie_f2, user_info = get_qr_cookie(sid)
        if not ok2:
            tg_send(
                chat_id,
                f"❌ <b>Lỗi lấy cookie:</b>\n{esc(cookie)}\n\n"
                "👉 Bạn có thể bấm <b>🔄 Check QR Status</b> để thử lại.",
                get_cookie_keyboard()
            )
            return

        _send_cookie_success(chat_id, tele_id, username, sid, cookie, cookie_f2, user_info)
        return

    tg_send(
        chat_id,
        "⏳ <b>CHƯA QUÉT QR</b>\n\n"
        "Mở app Shopee và quét mã QR đã gửi.\n\n"
        "👉 Sau khi quét, bot sẽ tự check mỗi 3s. Nếu chưa thấy trả cookie, bấm <b>🔄 Check QR Status</b> lại.",
        get_cookie_keyboard()
    )


def handle_cancel_qr(chat_id: Any, tele_id: Any, username: str, session_id: Optional[str] = None) -> None:
    """Hủy QR session (hỗ trợ inline button theo session_id)"""

    tele_id = int(tele_id) if safe_text(tele_id).isdigit() else tele_id

    cancelled_any = False

    with qr_lock:
        if session_id and session_id in qr_sessions and qr_sessions[session_id].get("user_id") == tele_id:
            qr_sessions.pop(session_id, None)
            cancelled_any = True
        else:
            user_sessions = [sid for sid, sess in qr_sessions.items() if sess.get("user_id") == tele_id]
            if not user_sessions:
                cancelled_any = False
            else:
                for sid in user_sessions:
                    qr_sessions.pop(sid, None)
                cancelled_any = True

    if not cancelled_any:
        tg_send(chat_id, "❌ <b>Không có QR nào đang chờ</b>", main_keyboard())
        return

    tg_send(
        chat_id,
        "✅ <b>Đã hủy QR</b>\n\nBạn có thể tạo QR mới khi cần.",
        main_keyboard()
    )

    log_qr(tele_id, username, session_id or "multiple", "cancelled", 0, "QR cancelled")



# =========================================================
# WEBHOOK HANDLER
# =========================================================
def _prune_spam_cache_for_user(tid: str, keep_minutes: int = 3) -> None:
    """Giữ lại vài phút gần nhất để tránh spam_cache phình"""
    # minute_key format: YYYY-mm-dd HH:MM
    try:
        now_dt = now().replace(second=0, microsecond=0)
        allowed = set()
        for i in range(keep_minutes):
            allowed.add((now_dt - timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M"))

        with spam_lock:
            mp = spam_cache.get(tid, {})
            for k in list(mp.keys()):
                if k not in allowed:
                    mp.pop(k, None)
            spam_cache[tid] = mp
    except Exception:
        pass

def _handle_message(chat_id: Any, tele_id: Any, username: str, text: str, data: Dict[str, Any]) -> None:
    if text == "/start":
        tg_send(
            chat_id,
            "👋 <b>CHÀO MỪNG ĐẾN BOT NGÂNMIU!</b>\n\n"
            "🤖 <b>Bot Check Đơn Hàng Shopee</b>\n"
            "━━━━━━━━━━━━━━━\n\n"
            "📦 <b>HỖ TRỢ CHECK:</b>\n"
            "✅ Check Đơn Hàng bằng Cookie Shopee\n"
            "✅ Check MVĐ Shopee Express (SPX)\n"
            "✅ Check MVĐ Giao Hàng Nhanh (GHN)\n"
            "✅ Check Số Điện Thoại Zin Shopee\n\n"
            "🔑 <b>GET COOKIE SHOPEE:</b>\n"
            "✅ Get Cookie qua QR Code\n"
            "   <i>(Quét QR trong app Shopee → Nhận cookie ngay)</i>\n\n"
            "━━━━━━━━━━━━━━━\n"
            "📖 <b>HƯỚNG DẪN SỬ DỤNG:</b>\n\n"
            "🍪 <b>Check Đơn Hàng bằng Cookie:</b>\n"
            "👉 Gửi cookie Shopee cho bot\n"
            "   (Dạng: SPC_ST=xxx...)\n\n"
            "📱 <b>Check Số Zin:</b>\n"
            "👉 Gửi số điện thoại (1-10 số)\n"
            "   VD: 0912345678\n"
            "   VD: 84912345678\n"
            "   VD: 912345678\n\n"
            "🔑 <b>Get Cookie QR:</b>\n"
            "👉 Bấm nút <b>🔑 Get Cookie QR</b>\n"
            "👉 Quét QR trong app Shopee\n"
            "👉 Nhận cookie ngay lập tức\n\n"
            "📦 <b>Check MVĐ:</b>\n"
            "👉 Gửi mã vận đơn SPX hoặc GHN\n\n"
            "━━━━━━━━━━━━━━━\n"
            "🧩 <b>HỆ THỐNG BOT NGÂNMIU:</b>\n\n"
            "🎟️ <b>Bot Lưu Voucher:</b> @nganmiu_bot\n"
            "📦 <b>Bot Check Đơn Hàng:</b> @ShopeexCheck_Bot\n"
            "🔑 <b>Bot Get Cookie QR:</b> <i>Đã tích hợp</i> ✅\n\n"
            "━━━━━━━━━━━━━━━\n"
            "🧑‍💼 <b>Admin hỗ trợ:</b> @BonBonxHPx\n"
            "👥 <b>Group Hỗ Trợ:</b> https://t.me/botxshopee\n\n"
            "✨ <i>Book Đơn Mã New tại NganMiu.Store</i>",
            main_keyboard()
        )
        return

    if text.startswith("/thongbao"):
        msg_obj = data.get("message", {})
        message_id = msg_obj.get("message_id", 0)
        handle_thongbao(chat_id, tele_id, username, text, message_id)
        return

    if text == "🔑 Get Cookie QR":
        handle_get_cookie_qr(chat_id, tele_id, username)
        return

    if text == "🔄 Check QR Status":
        handle_check_qr_status(chat_id, tele_id, username)
        return

    if text == "❌ Cancel QR":
        handle_cancel_qr(chat_id, tele_id, username)
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

        status = safe_text(
            user.get("trang thai")
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
            "🔑 <b>Get Cookie QR Shopee</b>\n"
            "👉 Bấm <b>🔑 Get Cookie QR</b> → Quét QR → Lấy cookie\n\n"
            "📦 <b>Check đơn hàng Shopee</b>\n"
            "👉 Gửi <b>cookie</b> dạng:\n"
            "<code>SPC_ST=xxxxx</code>\n\n"
            "🚚 <b>Tra mã vận đơn</b>\n"
            "👉 Gửi mã dạng:\n"
            "<code>SPXVNxxxxx</code>\n\n"
            "🚛 <b>Hỗ trợ các bên vận chuyển</b>\n"
            "• 🟠 <b>Shopee Express (SPX)</b>\n"
            "• 🟢 <b>Giao Hàng Nhanh (GHN)</b>\n\n"
            "💸 <b>Phí dịch vụ</b>\n"
            f"• Get Cookie QR: <b>{PRICE_GET_COOKIE:,}đ</b>\n"
            f"• Check cookie: <b>{PRICE_CHECK_COOKIE:,}đ</b>\n"
            f"• Check SPX: <b>{PRICE_CHECK_SPX:,}đ</b>\n\n"
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
            tg_send(chat_id, "❌ <b>Bạn chưa kích hoạt</b>\n\n👉 Kích hoạt tại @nganmiu_bot", main_keyboard())
            return

        balance = get_balance(user)
        tg_send(chat_id, f"💰 <b>SỐ DƯ HIỆN TẠI</b>\n\n{balance:,} đ", main_keyboard())
        return

    if text == "💳 Nạp Tiền":
        tg_send(chat_id, "💳 <b>NẠP TIỀN</b>\n\n👉 Vui lòng nạp tiền tại bot chính:\n💸 @nganmiu_bot", main_keyboard())
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
            "🔑 <b>Bot Get Cookie QR</b>\n"
            "👉 <i>Đã tích hợp trong bot này</i> ✅\n\n"
            "✨ <i>Book Đơn Mã New tại NganMiu.Store</i>",
            main_keyboard()
        )
        return

    # ✅ CHECK SỐ ĐIỆN THOẠI SHOPEE ZIN TRƯỚC (tránh conflict với GHN)
    # Vì is_ghn_code cũng nhận số 10 chữ số là GHN code
    if is_phone_number(text) or ('\n' in text and any(is_phone_number(line.strip()) for line in text.split('\n'))):
        row_idx, user = get_user_row(tele_id)
        if not user:
            tg_send(
                chat_id,
                "❌ <b>Bạn chưa kích hoạt</b>\n\n"
                "👉 Kích hoạt tại @nganmiu_bot",
                main_keyboard()
            )
            return
        
        # Check band
        is_band, until = check_band(row_idx)
        if is_band:
            tg_send(
                chat_id, 
                "🚫 <b>Tài khoản đang bị khóa</b>\n\n"
                f"⏱️ Mở lại lúc: <b>{until.strftime('%H:%M %d/%m')}</b>"
            )
            return
        
        # Trích xuất các số điện thoại
        phones = extract_phone_numbers(text)
        
        if not phones:
            # Không extract được số → có thể là GHN code, để check tiếp
            pass
        else:
            # Có số điện thoại → check số
            
            # Giới hạn 10 số
            if len(phones) > 10:
                tg_send(
                    chat_id,
                    f"⚠️ <b>QUÁ NHIỀU SỐ</b>\n\n"
                    f"📊 Bạn gửi {len(phones)} số\n"
                    f"🔢 Bot chỉ check tối đa 10 số/lần\n\n"
                    f"👉 Vui lòng gửi lại với tối đa 10 số",
                    main_keyboard()
                )
                return
            
            # Check spam
            balance = get_balance(user)
            minute_key = now().strftime("%Y-%m-%d %H:%M")
            tid = safe_text(tele_id)

            _prune_spam_cache_for_user(tid, keep_minutes=3)

            with spam_lock:
                spam_cache.setdefault(tid, {})
                spam_cache[tid][minute_key] = spam_cache[tid].get(minute_key, 0) + len(phones)
                count_min = spam_cache[tid][minute_key]

            if count_min > SPAM_LIMIT_PER_MIN:
                strike, band_until = inc_strike_and_band(row_idx, tele_id, username, count_min)
                tg_send(
                    chat_id,
                    "🚫 <b>SPAM PHÁT HIỆN</b>\n\n"
                    f"⚠️ Strike: <b>{strike}</b>\n"
                    f"⏱️ Band tới: <b>{band_until.strftime('%H:%M %d/%m')}</b>"
                )
                return
            
            # Gửi thông báo đang check
            if len(phones) == 1:
                tg_send(chat_id, f"🔄 <b>Đang kiểm tra số {phones[0]}...</b>")
            else:
                tg_send(chat_id, f"🔄 <b>Đang kiểm tra {len(phones)} số...</b>")
            
            # Check tất cả số với try-catch
            try:
                results = check_multiple_phones(phones)
            except Exception as e:
                print(f"[ERROR] check_multiple_phones: {e}")
                print(traceback.format_exc())
                tg_send(
                    chat_id,
                    f"❌ <b>LỖI CHECK SỐ</b>\n\n"
                    f"⚠️ Lỗi: {str(e)}\n\n"
                    f"💡 <b>Nguyên nhân có thể:</b>\n"
                    f"• Chưa cấu hình Google Sheet cho cookie\n"
                    f"• Biến GOOGLE_SHEET_COOKIE_ID chưa set\n"
                    f"• Tab Cookie chưa tạo trong sheet\n"
                    f"• Không có cookie trong sheet\n\n"
                    f"👉 Xem hướng dẫn tại HUONG_DAN_CHECK_SO_ZIN.md",
                    main_keyboard()
                )
                return
            
            # Xây dựng message kết quả
            zin_count = sum(1 for r in results if r.get("success") and r.get("is_zin"))
            not_zin_count = sum(1 for r in results if r.get("success") and not r.get("is_zin"))
            error_count = sum(1 for r in results if not r.get("success"))
            
            result_msg = f"📊 <b>KẾT QUẢ CHECK {len(phones)} SỐ</b>\n\n"
            result_msg += f"✅ Số zin: <b>{zin_count}</b>\n"
            result_msg += f"❌ Số không zin: <b>{not_zin_count}</b>\n"
            
            if error_count > 0:
                result_msg += f"⚠️ Lỗi: <b>{error_count}</b>\n"
            
            result_msg += "\n━━━━━━━━━━━━━━━\n"
            
            # Chi tiết từng số
            for r in results:
                phone = r["phone"]
                success = r["success"]
                is_zin = r["is_zin"]
                note = r["note"]
                
                if not success:
                    result_msg += f"\n⚠️ <code>{phone}</code> - Lỗi: {note}"
                elif is_zin:
                    result_msg += f"\n✅ <code>{phone}</code> - ZIN"
                else:
                    result_msg += f"\n❌ <code>{phone}</code> - KHÔNG ZIN"
            
            result_msg += "\n\n💡 <i>Tap vào số để copy</i>"
            
            tg_send(chat_id, result_msg, main_keyboard())
            
            # Log
            log_check(tele_id, username, f"{len(phones)} số", balance, f"check_phones:zin={zin_count},not_zin={not_zin_count}")
            return

    # Check GHN SAU (sau khi check phone)
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
        tg_send(chat_id, "🚫 <b>Tài khoản đang bị khóa</b>\n\n" f"⏱️ Mở lại lúc: <b>{until.strftime('%H:%M %d/%m')}</b>")
        return

    lines = split_lines(text)
    values = [v.strip() for v in lines if is_cookie(v.strip()) or is_spx(v.strip()) or is_ghn_code(v.strip())]
    if not values:
        tg_send(
            chat_id,
            "❌ <b>Dữ liệu không hợp lệ</b>\n\n"
            "🪙 Cookie: <code>SPC_ST=.xxxxx</code>\n"
            "🚚 SPX: <code>SPXVNxxxxx</code>\n"
            "🚛 GHN: <code>GHN...</code>",
            main_keyboard()
        )
        return

    balance = get_balance(user)

    for val in values:
        minute_key = now().strftime("%Y-%m-%d %H:%M")
        tid = safe_text(tele_id)

        _prune_spam_cache_for_user(tid, keep_minutes=3)

        with spam_lock:
            spam_cache.setdefault(tid, {})
            spam_cache[tid][minute_key] = spam_cache[tid].get(minute_key, 0) + 1
            count_min = spam_cache[tid][minute_key]

        if count_min > SPAM_LIMIT_PER_MIN:
            strike, band_until = inc_strike_and_band(row_idx, tele_id, username, count_min)
            tg_send(
                chat_id,
                "🚫 <b>SPAM PHÁT HIỆN</b>\n\n"
                f"⚠️ Strike: <b>{strike}</b>\n"
                f"⏱️ Band tới: <b>{band_until.strftime('%H:%M %d/%m')}</b>"
            )
            return

        # FREE LOGIC
        if balance <= 10000:
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

        # DO CHECK
        if is_cookie(val):
            result, err = check_shopee_orders(val)

            if not result:
                if err == "cookie_expired":
                    tg_send(chat_id, "🔒 <b>COOKIE KHÔNG HỢP LỆ</b>\n\n❌ Cookie đã <b>hết hạn</b> hoặc <b>bị Shopee khóa</b>.")
                    log_check(tele_id, username, val, balance, "cookie_expired")
                else:
                    tg_send(chat_id, "📭 <b>KHÔNG CÓ ĐƠN HÀNG</b>\n\nCookie hợp lệ nhưng hiện <b>không có đơn nào</b>.")
                    log_check(tele_id, username, val, balance, f"no_orders:{err or ''}")
            else:
                tg_send(chat_id, result)
                log_check(tele_id, username, val, balance, "check_orders")

        elif is_spx(val):
            result = check_spx(val)
            tg_send(chat_id, result)
            log_check(tele_id, username, val, balance, "check_spx")

        elif is_ghn_code(val):
            result = check_ghn(val)
            tg_send(chat_id, result)
            log_check(tele_id, username, val, balance, "check_ghn")

        time.sleep(0.2)

@app.route("/", methods=["POST", "GET"])
def webhook_root():
    if request.method == "GET":
        return jsonify({"ok": True, "msg": "Bot STEP 1 Optimized + QR Login"}), 200

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
        _handle_message(chat_id, tele_id, username, text, data)
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
# 🔥 CLEANUP QR SESSIONS THREAD
# =========================================================
def cleanup_qr_worker():
    """Thread dọn dẹp QR sessions hết hạn"""
    while True:
        time.sleep(60)
        cleaned = cleanup_qr_sessions()
        if cleaned > 0:
            print(f"[QR] Cleaned {cleaned} expired sessions")

cleanup_thread = threading.Thread(target=cleanup_qr_worker, daemon=True)
cleanup_thread.start()

# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    print("=" * 50)
    print("🤖 BOT STEP 1 OPTIMIZED + QR LOGIN - RUNNING")
    print("=" * 50)
    print(f"📋 Sheet ID: {SHEET_ID[:20]}...")
    print(f"🔑 Bot Token: {BOT_TOKEN[:20]}...")
    print(f"🔗 QR API: {QR_API_BASE}")
    print("✅ Log worker thread started")
    print("✅ QR cleanup thread started")
    print("=" * 50)

    def cleanup_cache_worker():
        while True:
            time.sleep(300)  # 5 phút
            clear_expired_cache()
            print("[CACHE] Cleaned expired cache")

    cache_thread = threading.Thread(target=cleanup_cache_worker, daemon=True)
    cache_thread.start()

    app.run(host="0.0.0.0", port=5000, debug=False)
