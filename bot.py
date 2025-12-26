# -*- coding: utf-8 -*-
"""
NgânMiu.Store — BOT CHECK ĐƠN HÀNG SHOPEE + TRA MÃ VẬN ĐƠN SPX
✅ FIX: Dùng ĐÚNG API từ app.py (đang chạy ngon)
"""

import os
import re
import json
import time
import html
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

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

COL_NOTE_INDEX  = 5   # cột E (1-based) – note/strike/band

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
# USER DATA
# =========================================================
def get_user_row(tele_id: Any) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    tele_id = safe_text(tele_id)
    try:
        if ws_has_headers(ws_user, ["Tele ID", "username", "balance"]):
            rows = ws_user.get_all_records()
            for idx, r in enumerate(rows, start=2):
                if safe_text(r.get("Tele ID")) == tele_id:
                    return idx, r
    except Exception:
        pass

    raw = ws_get_all_records_safe(ws_user)
    for idx, r in enumerate(raw, start=2):
        if safe_text(r.get("tele id")) == tele_id:
            return idx, {
                "Tele ID": r.get("tele id"),
                "username": r.get("username"),
                "balance": r.get("balance"),
            }
    return None, None

def get_balance(user: Dict[str, Any]) -> int:
    return safe_int(user.get("balance", 0))

def get_note(row_idx: int) -> str:
    try:
        return ws_user.cell(row_idx, COL_NOTE_INDEX).value or ""
    except Exception:
        return ""

def set_note(row_idx: int, value: str) -> None:
    try:
        ws_user.update_cell(row_idx, COL_NOTE_INDEX, value)
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

    try:
        ws_log_spam.append_row([
            now().strftime("%Y-%m-%d %H:%M:%S"),
            safe_text(tele_id),
            username or "",
            count_minute,
            strike,
            band_text
        ], value_input_option="USER_ENTERED")
    except Exception:
        pass

    return strike, band_until

# =========================================================
# LOG CHECK + COUNT
# =========================================================
def log_check(tele_id: Any, username: str, value: str, balance_after: int, note: str) -> None:
    try:
        ws_log_check.append_row([
            now().strftime("%Y-%m-%d %H:%M:%S"),
            safe_text(tele_id),
            username or "",
            mask_value(value),
            balance_after,
            note
        ], value_input_option="USER_ENTERED")
    except Exception:
        pass

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
            ["💳 Nạp Tiền", "🎟️ Bot Lưu Voucher"],
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
# STATUS ALIAS (ĐỒNG BỘ app.py)
# =========================================================

CODE_MAP = {
    # ===== GIAO THÀNH CÔNG =====
    "order_status_text_to_receive_delivery_done": ("✅ Giao hàng thành công", "success"),
    "order_tooltip_to_receive_delivery_done":     ("✅ Giao hàng thành công", "success"),
    "label_order_delivered":                      ("✅ Giao hàng thành công", "success"),

    # ===== ĐANG CHỜ NHẬN =====
    "order_list_text_to_receive_non_cod":         ("🚚 Đang chờ nhận (không COD)", "info"),
    "label_to_receive":                           ("🚚 Đang chờ nhận", "info"),
    "label_order_to_receive":                     ("🚚 Đang chờ nhận", "info"),

    # ===== CHỜ GIAO / ĐANG CHUẨN BỊ =====
    "label_order_to_ship":                        ("📦 Chờ giao hàng", "warning"),
    "label_order_being_packed":                   ("📦 Đang chuẩn bị hàng", "warning"),
    "label_order_processing":                     ("🔄 Đang xử lý", "warning"),

    # ===== THANH TOÁN / VẬN CHUYỂN =====
    "label_order_paid":                           ("💰 Đã thanh toán", "info"),
    "label_order_unpaid":                         ("💸 Chưa thanh toán", "info"),
    "label_order_waiting_shipment":               ("📦 Chờ bàn giao vận chuyển", "info"),
    "label_order_shipped":                        ("🚛 Đã bàn giao vận chuyển", "info"),

    # ===== LỖI / HỦY =====
    "label_order_delivery_failed":                ("❌ Giao không thành công", "danger"),
    "label_order_cancelled":                      ("❌ Đã hủy", "danger"),
    "label_order_return_refund":                  ("↩️ Trả hàng / Hoàn tiền", "info"),

    # ===== SHOPEE DUYỆT =====
    "order_list_text_to_ship_ship_by_date_not_calculated": (
        "🎖 Đơn hàng chờ Shopee duyệt", "warning"
    ),
    "order_status_text_to_ship_ship_by_date_not_calculated": (
        "🎖 Đơn hàng chờ Shopee duyệt", "warning"
    ),
    "label_ship_by_date_not_calculated": (
        "🎖 Đơn hàng chờ Shopee duyệt", "warning"
    ),

    # ===== SHOP CHUẨN BỊ =====
    "label_preparing_order":                      ("📦 Chờ shop gửi hàng", "warning"),
    "order_list_text_to_ship_order_shipbydate":   ("📦 Chờ shop gửi hàng", "warning"),
    "order_status_text_to_ship_order_shipbydate": ("📦 Người gửi đang chuẩn bị hàng", "warning"),
    "order_list_text_to_ship_order_shipbydate_cod": (
        "📦 Chờ shop gửi hàng (COD)", "warning"
    ),
    "order_status_text_to_ship_order_shipbydate_cod": (
        "📦 Chờ shop gửi hàng (COD)", "warning"
    ),
    "order_status_text_to_ship_order_edt_cod": (
        "📦 Chờ shop gửi hàng (COD)", "warning"
    ),
}
def normalize_status_text(status: str) -> str:
    """
    Chuẩn hóa text trạng thái (bỏ 'Tình trạng:' + emoji dư)
    """
    if not isinstance(status, str):
        return ""
    s = status.strip()
    s = re.sub(r"^tình trạng\s*:?\s*", "", s, flags=re.I)
    return s.strip()

# =========================================================
# 🔥 SHOPEE CHECK (ĐÚNG LOGIC TỪ app.py)
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
    """Tìm key đầu tiên trong nested dict/list (BFS)"""
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
    """Lấy tất cả giá trị của key trong nested structure"""
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
    """Format timestamp"""
    if isinstance(ts, str) and ts.isdigit():
        ts = int(ts)
    if isinstance(ts, (int, float)) and ts > 1_000_000:
        try:
            return datetime.fromtimestamp(int(ts)).strftime("%H:%M %d-%m-%Y")
        except Exception:
            return str(ts)
    return str(ts) if ts is not None else None
def fetch_orders_and_details(cookie: str, limit: int = 5):
    """
    FIX:
    - Phân biệt cookie chết / hết hạn
    - Không báo nhầm no_orders
    """
    headers = build_headers(cookie)

    list_url = f"{SHOPEE_BASE}/order/get_all_order_and_checkout_list"
    try:
        r = requests.get(
            list_url,
            headers=headers,
            params={"limit": limit, "offset": 0},
            timeout=20
        )

        if r.status_code != 200:
            return None, f"http_{r.status_code}"

        data = r.json()

    except Exception as e:
        return None, f"timeout: {e}"

    # ================== COOKIE DIE DETECT ==================
    # Shopee trả error/auth fail nhưng vẫn HTTP 200
    if isinstance(data, dict):
        # các dấu hiệu cookie chết / hết hạn
        if (
            data.get("error") in (401, 403)
            or data.get("error_msg")
            or data.get("msg") in ("unauthorized", "forbidden")
        ):
            return None, "cookie_expired"

    # ================== PARSE ORDER IDS ==================
    order_ids = bfs_values_by_key(data, ("order_id",)) if isinstance(data, dict) else []

    # ❌ Không có order_id
    if not order_ids:
        # Nếu data gần như trống → cookie chết
        if not data or len(data.keys()) <= 2:
            return None, "cookie_expired"
        return None, "no_orders"

    # ================== REMOVE DUP ==================
    seen, uniq = set(), []
    for oid in order_ids:
        if oid not in seen:
            seen.add(oid)
            uniq.append(oid)

    # ================== FETCH DETAIL ==================
    details = []
    for oid in uniq[:limit]:
        try:
            r2 = requests.get(
                f"{SHOPEE_BASE}/order/get_order_detail",
                headers=headers,
                params={"order_id": oid},
                timeout=15
            )
            if r2.status_code == 200:
                details.append(r2.json())
        except Exception:
            pass

    if not details:
        return None, "cookie_expired"

    return details, None

def format_order_simple(detail: dict) -> str:
    """Format đơn hàng Shopee – card mềm, đẹp trên mobile"""

    def short_text(s: str, max_len: int) -> str:
        s = (s or "").strip()
        if len(s) <= max_len:
            return s
        return s[:max_len - 3].rstrip() + "..."

    # ===== MVĐ =====
    tracking = (
        find_first_key(detail, "tracking_no")
        or find_first_key(detail, "tracking_number")
        or "-"
    )

    # ===== TRẠNG THÁI (ƯU TIÊN TIMELINE) =====
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

    # ===== COD =====
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

    # ===== SẢN PHẨM =====
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

    # ===== NGƯỜI NHẬN =====
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

    # ===== SHIPPER =====
    shipper_name = find_first_key(detail, "driver_name") or "-"
    shipper_phone = find_first_key(detail, "driver_phone") or "-"

    # ===== OUTPUT =====
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
    """Map status code sang text + color"""
    if not isinstance(code, str):
        return None, "secondary"
    return CODE_MAP.get(code, (code, "secondary"))

def check_shopee_orders(cookie: str) -> Tuple[Optional[str], Optional[str]]:
    """Trả text đơn hàng - HIỂN THỊ TẤT CẢ ĐƠN"""
    cookie = cookie.strip()
    if "SPC_ST=" not in cookie:
        return None, "missing_spc_st"

    # Lấy tối đa 10 đơn để check
    details, error = fetch_orders_and_details(cookie, limit=10)
    if error:
        return None, error

    if not details:
        return "📭 <b>Không có đơn hàng</b>", None

    blocks = []
    for idx, d in enumerate(details, 1):
        if isinstance(d, dict):
            # Thêm số thứ tự cho mỗi đơn
            block = format_order_simple(d)
            blocks.append(block)

    # Hiển thị tổng số đơn tìm thấy
    return "\n\n".join(blocks), None


# =========================================================
# 🔥 SPX CHECK (tramavandon.com - ĐÚNG API)
# =========================================================
SPX_API = "https://tramavandon.com/api/spx.php"

def check_spx(code: str) -> str:
    """
    Call đúng API tramavandon.com như app.py
    """
    code = (code or "").strip().upper()
    
    payload = {"tracking_id": code}
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }

    try:
        r = requests.post(SPX_API, json=payload, headers=headers, timeout=20)
        data = r.json()

        if data.get("retcode") != 0:
            return f"🔎 <b>{esc(code)}</b>\n❌ Không tìm thấy thông tin"

        records = data["data"]["sls_tracking_info"]["records"]
        
        timeline = []
        phone = ""

        for rec in records:
            ts = rec.get("actual_time")
            dt = datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M") if ts else ""
            
            status_text = rec.get("buyer_description", "")
            location = rec.get("current_location", {}).get("location_name", "")

            # Tìm SĐT
            if not phone:
                found = re.findall(r"\b0\d{9,10}\b", status_text)
                if found:
                    phone = found[0]

            timeline.append(f"• {dt} — {status_text} — {location}")

        timeline_text = "\n".join(timeline[-5:]) if timeline else "Chưa có thông tin"
        
        return (
            f"🔎 <b>MVĐ:</b> <code>{esc(code)}</code>\n"
            f"📊 <b>Trạng thái:</b> Đang vận chuyển\n"
            f"📱 <b>SĐT shipper:</b> <code>{esc(phone) if phone else '-'}</code>\n\n"
            f"📜 <b>Timeline:</b>\n{timeline_text}"
        )

    except Exception as e:
        return f"🔎 <b>{esc(code)}</b>\n❌ Lỗi: {e}"

# =========================================================
# WEBHOOK HANDLER
# =========================================================
def _handle_message(chat_id: Any, tele_id: Any, username: str, text: str) -> None:
    # ---------- START ----------
    if text == "/start":
        tg_send(
            chat_id,
            "🤖 <b>BOT CHECK ĐƠN HÀNG SHOPEE + SPX</b>\n\n"
            "Chọn chức năng bên dưới 👇",
            main_keyboard()
        )
        return
    # ================== MENU BUTTONS ==================

    # ✅ KÍCH HOẠT (check đã kích ở bot add voucher)
    if text == "✅ Kích Hoạt":
        row_idx, user = get_user_row(tele_id)

        if not user:
            tg_send(
                chat_id,
                "❌ <b>CHƯA KÍCH HOẠT</b>\n\n"
                "👉 Vui lòng kích hoạt tại bot lưu voucher trước:\n"
                "🎟️ @nganmiu_bot",
                main_keyboard()
            )
            return

        status = safe_text(
            user.get("status")
            or user.get("trạng thái")
            or user.get("active")
        ).lower()

        if status == "active":
            tg_send(
                chat_id,
                "✅ <b>TÀI KHOẢN ĐÃ KÍCH HOẠT</b>\n\n"
                "Bạn có thể sử dụng bot bình thường 🚀",
                main_keyboard()
            )
            return

        tg_send(
            chat_id,
            "❌ <b>CHƯA KÍCH HOẠT</b>\n\n"
            "👉 Hãy kích hoạt tại bot lưu voucher:\n"
            "🎟️ @nganmiu_bot",
            main_keyboard()
        )
        return


    # 💰 SỐ DƯ
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


    # 💳 NẠP TIỀN
    if text == "💳 Nạp Tiền":
        tg_send(
            chat_id,
            "💳 <b>NẠP TIỀN</b>\n\n"
            "👉 Vui lòng nạp tiền tại bot chính:\n"
            "💸 @nganmiu_bot",
            main_keyboard()
        )
        return


    # 🎟️ BOT LƯU VOUCHER
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



    # ---------- USER CHECK ----------
    row_idx, user = get_user_row(tele_id)
    if not user:
        tg_send(
            chat_id,
            "❌ <b>Tài khoản chưa có trong Sheet</b>\n\n"
            "Bấm <b>✅ Kích hoạt</b> để lấy Tele ID rồi thêm vào tab <b>Thanh Toan</b>.",
            main_keyboard()
        )
        return

    # ---------- BAND CHECK ----------
    is_band, until = check_band(row_idx)
    if is_band:
        tg_send(
            chat_id,
            "🚫 <b>Tài khoản đang bị khóa</b>\n\n"
            f"⏱️ Mở lại lúc: <b>{until.strftime('%H:%M %d/%m')}</b>"
        )
        return

    # ---------- PARSE INPUT ----------
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

    # ---------- PROCESS ----------
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

        # FREE LIMIT: chỉ khi balance <= 0
        if balance <= 0:
            used = count_today_request(tele_id)
            if used >= FREE_LIMIT_PER_DAY:
                tg_send(
                    chat_id,
                    "⚠️ <b>HẾT LƯỢT MIỄN PHÍ HÔM NAY</b>\n\n"
                    f"📊 Đã dùng: {used}/{FREE_LIMIT_PER_DAY} request"
                )
                return

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



        # chống flood telegram nhẹ
        time.sleep(0.2)

@app.route("/", methods=["POST", "GET"])
def webhook_root():
    if request.method == "GET":
        return jsonify({"ok": True, "msg": "Bot is running", "path": "/ or /webhook"}), 200

    data = request.get_json(silent=True) or {}

    # ---------- CALLBACK ----------
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
        # không cho crash server
        err = traceback.format_exc()
        tg_send(chat_id, "❌ Bot gặp lỗi nội bộ, bạn gửi lại sau nhé.")
        try:
            print(err)
        except Exception:
            pass

    return "OK"

# FIX 404: Telegram đang bắn /webhook thì route này sẽ nhận
@app.route("/webhook", methods=["POST", "GET"])
def webhook_alias():
    return webhook_root()

# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    print("=" * 50)
    print("🤖 BOT CHECK SHOPEE + SPX - RUNNING")
    print("=" * 50)
    print(f"📋 Sheet ID: {SHEET_ID[:20]}...")
    print(f"🔑 Bot Token: {BOT_TOKEN[:20]}...")
    print("=" * 50)
    app.run(host="0.0.0.0", port=5000, debug=False)
