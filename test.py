# -*- coding: utf-8 -*-
import sys
import threading
import requests

from PyQt5.QtWidgets import (
    QApplication, QWidget, QTextEdit, QPushButton,
    QVBoxLayout, QHBoxLayout, QLabel, QTableWidget,
    QTableWidgetItem, QMessageBox, QLineEdit
)
from PyQt5.QtCore import Qt, pyqtSignal, QObject


# ==================================================
# CONFIG
# ==================================================
API_URL = "https://shopee.vn/api/v4/account/management/check_unbind_phone"
UA = "Mozilla/5.0 (Linux; Android 10; ShopeeApp)"


# ==================================================
# HELPERS
# ==================================================
def normalize_phone(phone: str) -> str:
    phone = phone.strip().replace(" ", "")
    if phone.startswith("0"):
        return "84" + phone[1:]
    if phone.startswith("84"):
        return phone
    return ""


def call_check_unbind(phone84: str, cookie: str) -> dict:
    headers = {
        "User-Agent": UA,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Cookie": cookie,
        "Origin": "https://shopee.vn",
        "Referer": "https://shopee.vn/",
    }

    payload = {
        "phone": phone84
    }

    r = requests.post(API_URL, json=payload, headers=headers, timeout=10)
    try:
        return r.json()
    except Exception:
        return {"_error": "invalid_json"}


def parse_result(data: dict) -> str:
    """
    TRẢ VỀ:
    - 'KHOA'  -> chỉ khi chắc chắn khóa
    - 'SONG'  -> các trường hợp còn lại
    """

    # lỗi HTTP / json
    if not isinstance(data, dict):
        return "SONG"

    # thường Shopee trả error_code / error / msg
    text = str(data).lower()

    # === CHỈ NHỮNG CASE NÀY MỚI COI LÀ KHÓA ===
    LOCK_KEYWORDS = [
        "phone_locked",
        "account_banned",
        "account_disabled",
        "forbidden",
        "locked"
    ]

    for k in LOCK_KEYWORDS:
        if k in text:
            return "KHOA"

    # === CÒN LẠI COI LÀ SỐNG ===
    return "SONG"


# ==================================================
# SIGNAL
# ==================================================
class Signals(QObject):
    result = pyqtSignal(str, str)


# ==================================================
# WORKER
# ==================================================
class CheckWorker(threading.Thread):
    def __init__(self, phone, cookie, signals):
        super().__init__(daemon=True)
        self.phone = phone
        self.cookie = cookie
        self.signals = signals

    def run(self):
        data = call_check_unbind(self.phone, self.cookie)
        status = parse_result(data)
        self.signals.result.emit(self.phone, status)


# ==================================================
# UI
# ==================================================
class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Check SĐT Shopee – Unbind API")
        self.resize(760, 440)

        self.signals = Signals()
        self.signals.result.connect(self.update_result)

        self.init_ui()

    def init_ui(self):
        main = QHBoxLayout(self)

        # LEFT
        left = QVBoxLayout()
        left.addWidget(QLabel("📱 Nhập SĐT (mỗi số 1 dòng):"))

        self.txt_phone = QTextEdit()
        left.addWidget(self.txt_phone)

        left.addWidget(QLabel("🍪 Cookie Shopee (acc sống):"))
        self.txt_cookie = QLineEdit()
        self.txt_cookie.setPlaceholderText("SPC_EC=...; SPC_U=...; ...")
        left.addWidget(self.txt_cookie)

        self.btn_check = QPushButton("🚀 CHECK")
        self.btn_check.setFixedHeight(36)
        self.btn_check.clicked.connect(self.start_check)
        left.addWidget(self.btn_check)

        main.addLayout(left, 1)

        # RIGHT
        right = QVBoxLayout()
        right.addWidget(QLabel("📊 Kết quả:"))

        self.table = QTableWidget(0, 2)
        self.table.setHorizontalHeaderLabels(["SĐT", "Trạng thái"])
        self.table.horizontalHeader().setStretchLastSection(True)
        right.addWidget(self.table)

        main.addLayout(right, 1)

    # ==================================================
    def start_check(self):
        raw = self.txt_phone.toPlainText().strip()
        cookie = self.txt_cookie.text().strip()

        if not raw or not cookie:
            QMessageBox.warning(self, "Thiếu dữ liệu", "Chưa nhập SĐT hoặc Cookie")
            return

        phones = []
        for line in raw.splitlines():
            p = normalize_phone(line)
            if p:
                phones.append(p)

        if not phones:
            QMessageBox.warning(self, "Lỗi", "Không có SĐT hợp lệ")
            return

        self.table.setRowCount(0)

        for phone in phones:
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.table.setItem(row, 0, QTableWidgetItem(phone))
            self.table.setItem(row, 1, QTableWidgetItem("⏳ Đang check..."))

            CheckWorker(phone, cookie, self.signals).start()

    def update_result(self, phone, status):
        label = "🔴 KHÓA" if status == "KHOA" else "🟢 SỐNG"
        for row in range(self.table.rowCount()):
            if self.table.item(row, 0).text() == phone:
                item = QTableWidgetItem(label)
                item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(row, 1, item)
                break


# ==================================================
# RUN
# ==================================================
if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec_())
