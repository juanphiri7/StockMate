#StockMate by Juan

# ========== Imports ==========
import os
import re
import json
import pytz
import fitz
import atexit
import signal
import qrcode
import logging
import requests
import tempfile
import requests_cache
import psycopg2.extras
from PIL import Image
from fpdf import FPDF
from io import BytesIO
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
from psycopg2 import pool, sql
from dotenv import load_dotenv
from flask_caching import Cache
from contextlib import contextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify, render_template_string, redirect, url_for, session, send_file, Response, abort 


# ========= Load Environment Variables and configure caching =========
load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("stockmate")

DATABASE_URL = os.getenv("DATABASE_URL")
FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY") or "fallback-secret-key-for-dev-only"
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD") or "default-dev-password"

# Validate critical env vars early
if not DATABASE_URL:
    logger.error("DATABASE_URL is not set. Exiting.")
    raise ValueError("DATABASE_URL environment variable is required")


# ========== FLASK APP and caching ==========
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY
cache = Cache(app, config={"CACHE_TYPE": "simple"})  # replace with Redis in prod

# ========== Database Context Manager and Connection Pool ==========
_pg_pool = None

def init_db_pool(minconn=1, maxconn=10):
    global _pg_pool
    if _pg_pool is None:
        logger.info("Initializing DB connection pool...")
        _pg_pool = pool.SimpleConnectionPool(minconn, maxconn, dsn=DATABASE_URL)
    return _pg_pool

@contextmanager
def get_db_connection():
    """
    Context manager to get a connection from the pool and automatically return it.
    Usage:
        with get_db_connection() as conn:
            cur = conn.cursor()
            ...
    """
    global _pg_pool
    if _pg_pool is None:
        init_db_pool()
    conn = _pg_pool.getconn()
    try:
        yield conn
    finally:
        try:
            _pg_pool.putconn(conn)
        except Exception:
            conn.close()


# ========== TIMEZONE CONVERTER and Utilities ==========
LOCAL_TZ = pytz.timezone("Africa/Blantyre")  # user's default tz

def convert_to_local_time(utc_dt):
    if not utc_dt:
        return None
    if isinstance(utc_dt, str):
        try:
            utc_dt = datetime.strptime(utc_dt, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return utc_dt
    if utc_dt.tzinfo is None:
        # assume UTC if naive
        utc_dt = pytz.utc.localize(utc_dt)
    return utc_dt.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")

def safe_float(value):
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).replace(",", "").replace("MK", "").replace("%", "").strip()
        # remove any non-digit except dot and minus
        s = re.sub(r"[^\d\.\-]", "", s)
        return float(s) if s != "" else None
    except Exception:
        return None

def safe_int(value):
    if value is None:
        return None
    try:
        if isinstance(value, int):
            return value
        s = str(value).replace(",", "").strip()
        s = re.sub(r"[^\d\-]", "", s)
        return int(s) if s != "" else None
    except Exception:
        return None

def normalize_counter(counter):
    if not counter:
        return None
    return re.sub(r"[^A-Z0-9]", "", counter.upper())
        

# ========== DATABASE INIT ==========
def init_db():
    """Create required tables if missing and add helpful indices."""
    logger.info("Initializing database schema...")
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                id SERIAL PRIMARY KEY,
                counter TEXT NOT NULL,
                last_price NUMERIC,
                change_value NUMERIC,
                volume BIGINT,
                turnover NUMERIC,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stocks_counter_ts ON stocks (counter, timestamp DESC);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fundamentals (
                id SERIAL PRIMARY KEY,
                counter TEXT UNIQUE NOT NULL,
                net_profit NUMERIC,
                number_of_shares_in_issue BIGINT,
                dividend NUMERIC,
                book_value NUMERIC,
                report_link TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fun_counter ON fundamentals (counter);")
        conn.commit()
        cur.close()
    logger.info("Database schema ready.")

# ========== Initialize sample fundamentals (if you want to seed) ==========
def initialize_fundamentals_seed(seed_data):
    """Seed fundamentals; uses UPSERT semantics."""
    logger.info("Seeding fundamentals (if not present)...")
    with get_db_connection() as conn:
        cur = conn.cursor()
        for entry in seed_data:
            counter = normalize_counter(entry.get("counter"))
            if not counter:
                continue
            net_profit = safe_float(entry.get("net_profit"))
            shares = safe_int(entry.get("number_of_shares_in_issue"))
            dividend = safe_float(entry.get("dividend_paid") or entry.get("dividend"))
            book_value = safe_float(entry.get("book_value"))
            cur.execute("""
                INSERT INTO fundamentals (counter, net_profit, number_of_shares_in_issue, dividend, book_value)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (counter)
                DO UPDATE SET
                    net_profit = EXCLUDED.net_profit,
                    number_of_shares_in_issue = EXCLUDED.number_of_shares_in_issue,
                    dividend = EXCLUDED.dividend,
                    book_value = EXCLUDED.book_value,
                    updated_at = CURRENT_TIMESTAMP;
            """, (counter, net_profit, shares, dividend, book_value))
        conn.commit()
        cur.close()
    logger.info("Seeding done.")


# ==================== SCRAPE ====================
HEADERS = {"User-Agent": "Mozilla/5.0 (StockMate Bot)"}
requests.adapters.DEFAULT_RETRIES = 2

def parse_price_cell(text):
    """Return numeric last price or None."""
    return safe_float(text)

def parse_change_cell(text):
    return safe_float(text)

def parse_volume_cell(text):
    # volume is often integer, may include commas
    return safe_int(text)

def parse_turnover_cell(text):
    return safe_float(text)

def scrape_mse():
    """
    Scrape the MSE homepage table of counters.
    Returns a list of dicts with normalized values.
    Cached at function level for a short time via flask-cache on caller.
    """
    url = "https://www.mse.co.mw/"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        logger.error("Failed to fetch MSE page: %s", e)
        return []

    soup = BeautifulSoup(resp.content, "html.parser")
    table = soup.find("table")
    if not table:
        logger.warning("No table found on MSE page.")
        return []

    data = []
    rows = table.find_all("tr")
    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) < 5:
            continue
        raw_counter = cols[0].get_text(strip=True)
        counter = normalize_counter(raw_counter)
        if not counter:
            continue

        last_price = parse_price_cell(cols[1].get_text(strip=True))
        change_val = parse_change_cell(cols[2].get_text(strip=True))
        volume = parse_volume_cell(cols[3].get_text(strip=True))
        turnover = parse_turnover_cell(cols[4].get_text(strip=True))

        data.append({
            "counter": counter,
            "last_price": last_price,
            "change": change_val,
            "volume": volume,
            "turnover": turnover
        })
    logger.info("Scraped %d counters from MSE", len(data))
    return data


# ==================== SAVE ====================
def save_data(stock_data):
    """
    Insert fetched stock rows into the DB if the last record for this counter
    (within the last hour) is different.
    """
    if not stock_data:
        return 0
    inserted = 0
    with get_db_connection() as conn:
        cur = conn.cursor()
        for item in stock_data:
            counter = item.get("counter")
            last_price = item.get("last_price")
            change_val = item.get("change")
            volume = item.get("volume")
            turnover = item.get("turnover")
            # Avoid saving if the most recent row (within 1 hour) matches these values
            cur.execute("""
                SELECT 1 FROM stocks
                WHERE counter = %s
                  AND COALESCE(last_price::text, '') = COALESCE(%s::text, '')
                  AND COALESCE(change_value::text, '') = COALESCE(%s::text, '')
                  AND COALESCE(volume::text, '') = COALESCE(%s::text, '')
                  AND COALESCE(turnover::text, '') = COALESCE(%s::text, '')
                  AND timestamp >= NOW() - INTERVAL '1 hour';
            """, (counter, last_price, change_val, volume, turnover))
            if cur.fetchone():
                continue
            cur.execute("""
                INSERT INTO stocks (counter, last_price, change_value, volume, turnover)
                VALUES (%s, %s, %s, %s, %s)
            """, (counter, last_price, change_val, volume, turnover))
            inserted += 1
        conn.commit()
        cur.close()
    logger.info("Saved %d new stock rows", inserted)
    return inserted


# ================= API ROUTES =================

# ======= Home Route
@app.route("/")
def home():
    return "Hello There! StockMate API is Running!"


# ======= Scrape Route
@app.route("/scrape", methods=["GET"])
def scrape_and_save():
    data = scrape_mse()
    if not data:
        return jsonify({"error": "Failed to scrape data"}), 500
    count = save_data(data)
    return jsonify({"message": "Success! Data scraped and saved", "scraped": len(data), "inserted": count})


# ======= Stocks Route
@app.route("/stocks", methods=["GET"])
def get_stocks():
    limit = int(request.args.get("limit", 20))
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT counter, last_price, change_value, volume, turnover, timestamp
            FROM stocks
            ORDER BY timestamp DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
    return jsonify([
        {
            "counter": r[0],
            "last_price": float(r[1]) if r[1] is not None else None,
            "change": float(r[2]) if r[2] is not None else None,
            "volume": int(r[3]) if r[3] is not None else None,
            "turnover": float(r[4]) if r[4] is not None else None,
            "timestamp": convert_to_local_time(r[5])
        } for r in rows
    ])


# ======= Latest Prices Route
@app.route("/latest_prices", methods=["GET"])
@cache.cached(timeout=120)
def latest_prices():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT s.counter, s.last_price, s.change_value, s.volume, s.turnover, MAX(s.timestamp)
            FROM stocks s
            GROUP BY s.counter, s.last_price, s.change_value, s.volume, s.turnover
        """)
        rows = cur.fetchall()
        cur.close()
    results = []
    for r in rows:
        results.append({
            "counter": r[0],
            "last_price": float(r[1]) if r[1] is not None else None,
            "change": float(r[2]) if r[2] is not None else None,
            "volume": int(r[3]) if r[3] is not None else None,
            "turnover": float(r[4]) if r[4] is not None else None,
            "timestamp": convert_to_local_time(r[5])
        })
    return jsonify(results)


# ======= History Route
@app.route("/history/<counter>", methods=["GET"])
def get_history(counter):
    counter = normalize_counter(counter)
    limit = int(request.args.get("limit", 10))
    order = request.args.get("order", "desc").lower()
    if not counter:
        return jsonify({"error": "Invalid counter"}), 400
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT timestamp, last_price
            FROM stocks
            WHERE counter = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (counter, limit))
        rows = cur.fetchall()
        cur.close()
    history = [{"date": convert_to_local_time(r[0]), "price": float(r[1]) if r[1] is not None else None} for r in rows]
    if order == "asc":
        history = list(reversed(history))
    return jsonify(history)
            


# ======= Insert Fundamentals 
@app.route("/insert_fundamentals", methods=["POST"])
def insert_fundamentals():
    if not request.json:
        return jsonify({"error": "No JSON payload provided"}), 400
    data = request.json
    if not isinstance(data, list):
        return jsonify({"error": "Payload must be a list of fundamentals objects"}), 400
    with get_db_connection() as conn:
        cur = conn.cursor()
        for entry in data:
            counter = normalize_counter(entry.get("counter"))
            if not counter:
                continue
            net_profit = safe_float(entry.get("net_profit"))
            shares = safe_int(entry.get("number_of_shares_in_issue"))
            dividend = safe_float(entry.get("dividend_paid") or entry.get("dividend"))
            book_value = safe_float(entry.get("book_value"))
            report_link = entry.get("report_link")
            cur.execute("""
                INSERT INTO fundamentals (counter, net_profit, number_of_shares_in_issue, dividend, book_value, report_link)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (counter)
                DO UPDATE SET
                    net_profit = EXCLUDED.net_profit,
                    number_of_shares_in_issue = EXCLUDED.number_of_shares_in_issue,
                    dividend = EXCLUDED.dividend,
                    book_value = EXCLUDED.book_value,
                    report_link = COALESCE(EXCLUDED.report_link, fundamentals.report_link),
                    updated_at = CURRENT_TIMESTAMP;
            """, (counter, net_profit, shares, dividend, book_value, report_link))
        conn.commit()
        cur.close()
    return jsonify({"message": "Fundamentals inserted/updated successfully"})


# ======= Fundamentals Route
@app.route("/fundamentals/<counter>", methods=["GET"])
def get_fundamentals(counter):
    counter = normalize_counter(counter)
    if not counter:
        return jsonify({"error": "Invalid counter"}), 400
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT net_profit, number_of_shares_in_issue, dividend, book_value
                FROM fundamentals
                WHERE counter = %s
            """, (counter,))
            row = cur.fetchone()
            cur.close()
        if not row:
            return jsonify({"error": "Data not available for this company"}), 404

        net_profit, shares, dividend, book_value = row
        net_profit = safe_float(net_profit) or 0
        shares = safe_int(shares) or 0
        dividend = safe_float(dividend) or 0
        book_value = safe_float(book_value) or 0

        eps = net_profit / shares if shares else 0
        bvps = book_value / shares if shares else 0
        dvps = dividend / shares if shares else 0

        # latest price
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT last_price FROM stocks
                WHERE counter = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (counter,))
            res = cur.fetchone()
            cur.close()
        if not res:
            return jsonify({"error": "Price data not available"}), 404
        price = safe_float(res[0]) or 0

        pe_ratio = price / eps if eps else None
        pb_ratio = price / bvps if bvps else None
        div_yield = (dvps / price) * 100 if price and dvps else None

        return jsonify({
            "counter": counter,
            "eps": round(eps, 4),
            "pe_ratio": round(pe_ratio, 4) if pe_ratio is not None else None,
            "pb_ratio": round(pb_ratio, 4) if pb_ratio is not None else None,
            "div_yield_percent": round(div_yield, 4) if div_yield is not None else None,
            "price": round(price, 4)
        })
    except Exception as e:
        logger.exception("Error computing fundamentals for %s", counter)
        return jsonify({"error": str(e)}), 500


# ======= Metrics Route
@app.route("/metrics/<counter>", methods=["GET"])
def stock_metrics(counter):
    counter = normalize_counter(counter)
    if not counter:
        return jsonify({"error": "Invalid counter"}), 400
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT net_profit, number_of_shares_in_issue, dividend, book_value, report_link
                FROM fundamentals WHERE counter = %s
            """, (counter,))
            fund = cur.fetchone()
            cur.close()
        if not fund:
            return jsonify({"error": "Fundamentals not available"}), 404

        net_profit, shares, dividend, book_value, report_link = fund
        net_profit = safe_float(net_profit) or 0
        shares = safe_int(shares) or 0
        dividend = safe_float(dividend) or 0
        book_value = safe_float(book_value) or 0

        eps = net_profit / shares if shares else 0
        bvps = book_value / shares if shares else 0
        dvps = dividend / shares if shares else 0

        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT last_price, change_value, volume, turnover, timestamp
                FROM stocks WHERE counter = %s
                ORDER BY timestamp DESC LIMIT 1
            """, (counter,))
            latest = cur.fetchone()
            cur.close()
        if not latest:
            return jsonify({"error": "Latest price data not found"}), 404

        price, change_val, volume, turnover, ts = latest
        price = safe_float(price) or 0
        pe_ratio = price / eps if eps else None
        pb_ratio = price / bvps if bvps else None
        div_yield = (dvps / price) * 100 if price and dvps else None

        return jsonify({
            "counter": counter,
            "last_price": round(price, 4),
            "change": round(safe_float(change_val) or 0, 4),
            "volume": int(volume) if volume else None,
            "turnover": round(turnover, 4) if turnover else None,
            "timestamp": convert_to_local_time(ts),
            "eps": round(eps, 4),
            "pe_ratio": round(pe_ratio, 4) if pe_ratio is not None else None,
            "pb_ratio": round(pb_ratio, 4) if pb_ratio is not None else None,
            "div_yield_percent": round(div_yield, 4) if div_yield is not None else None,
            "report_link": report_link
        })
    except Exception as e:
        logger.exception("Error computing metrics")
        return jsonify({"error": str(e)}), 500


# ========== PDF Class ==========
class PDF(FPDF):
    def header(self):
        logo_path = "StockMate-logo.png"
        if os.path.exists(logo_path):
            self.image(logo_path, 10, 8, 30)
        self.set_xy(50, 10)
        self.set_fill_color(0, 102, 204)
        self.set_text_color(255, 255, 255)
        self.set_font("DejaVu", "B", 20)
        self.cell(140, 10, "StockMate Fundamentals Report", ln=True, align='C', fill=True)
        self.ln(15)
        self.set_text_color(75, 0, 130)
        self.set_font("DejaVu", "I", 10)
        self.set_xy(50, 20)
        self.cell(140, 8, "Smart Insights. Wise Investments.", ln=True, align='C')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font("DejaVu", "", 10)
        self.set_text_color(0, 0, 238)
        self.cell(0, 6, "Call/WhatsApp: +265888695513", ln=True, align='C', link='https://wa.me/265888695513')
        self.cell(0, 6, "Email: juanphiri7@gmail.com", ln=True, align='C', link='mailto:juanphiri7@gmail.com')

def load_fonts(pdf):
    font_files = {
        "": "fonts/DejaVuSans.ttf",
        "B": "fonts/DejaVuSans-Bold.ttf",
        "I": "fonts/DejaVuSans-Oblique.ttf",
        "BI": "fonts/DejaVuSans-BoldOblique.ttf"
    }
    for style, path in font_files.items():
        if os.path.exists(path):
            pdf.add_font("DejaVu", style, path, uni=True)
        else:
            print(f"Font file {path} not found, using default font")
            pdf.set_font("Arial", style, 12)


# ======= Fundamentals Report Route
@app.route('/fundamentals_report/<counter>', methods=['GET'])
def fundamentals_report(counter):
    counter = counter.upper()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT net_profit, number_of_shares_in_issue, dividend_paid, book_value
                FROM fundamentals WHERE counter = %s
            ''', (counter,))
            row = cursor.fetchone()
        if not row:
            return jsonify({"error": "Data not available for this company"}), 404

        net_profit, number_of_shares_in_issue, dividend_paid, book_value = row
        net_profit = float(str(net_profit).replace(',', '')) if net_profit else 0
        shares = float(str(number_of_shares_in_issue).replace(',', '')) if number_of_shares_in_issue else 0
        dividend = float(str(dividend_paid).replace(',', '')) if dividend_paid else 0
        book_value = float(str(book_value).replace(',', '')) if book_value else 0

        eps = net_profit / shares if shares else 0
        bvps = book_value / shares if shares else 0
        dvps = dividend / shares if shares else 0

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT last_price FROM stocks
                WHERE counter = %s
                ORDER BY timestamp DESC LIMIT 1
            ''', (counter,))
            result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Price data not available"}), 404

        price = float(str(result[0]).replace(',', '')) if result[0] else 0
        pe_ratio = price / eps if eps else None
        pb_ratio = price / bvps if bvps else None
        div_yield = (dvps / price) * 100 if price and dvps else None

        pdf = PDF()
        load_fonts(pdf)
        pdf.add_page()
        pdf.ln(10)

        logo_path = f"company_logos/{counter}.png"
        logo_width = 25
        if os.path.exists(logo_path):
            y_start = pdf.get_y()
            pdf.image(logo_path, x=10, y=y_start, w=logo_width)
            pdf.set_xy(10 + logo_width + 10, y_start + 5)
            pdf.set_font("DejaVu", "B", 16)
            pdf.cell(0, 10, f"{counter} Snapshot", ln=True)
            pdf.set_y(y_start + logo_width + 5)
        else:
            pdf.set_font("DejaVu", "B", 16)
            pdf.set_text_color(0)
            pdf.cell(0, 10, f"{counter} Snapshot", ln=True)
            pdf.ln(10)

        pdf.set_text_color(0)
        pdf.set_font("DejaVu", "", 12)
        pdf.cell(0, 10, f"Latest Price: MK {price:,.2f}" if price else "Latest Price: N/A", ln=True)
        pdf.cell(0, 10, f"Net Profit: MK {net_profit:,.2f}" if net_profit else "Net Profit: N/A", ln=True)
        pdf.cell(0, 10, f"Dividend Paid: MK {dividend:,.2f}" if dividend else "Dividend Paid: N/A", ln=True)
        pdf.cell(0, 10, f"Number of Shares in Issue: {shares:,.0f}" if shares else "Number of Shares in Issue: N/A", ln=True)
        pdf.cell(0, 10, f"Book Value: MK {book_value:,.2f}" if book_value else "Book Value: N/A", ln=True)
        pdf.ln(5)
        pdf.set_font("DejaVu", "B", 16)
        pdf.cell(0, 10, "Key Financial Metrics", ln=True)
        pdf.set_font("DejaVu", "", 12)
        pdf.cell(0, 10, f"Earnings Per Share (EPS): {eps:.2f}" if eps else "Earnings Per Share (EPS): N/A", ln=True)
        pdf.cell(0, 10, f"P/E Ratio: {pe_ratio:.2f}" if pe_ratio else "P/E Ratio: N/A", ln=True)
        pdf.cell(0, 10, f"Dividend Yield: {div_yield:.2f}%" if div_yield else "Dividend Yield: N/A", ln=True)
        pdf.cell(0, 10, f"P/B Ratio: {pb_ratio:.2f}" if pb_ratio else "P/B Ratio: N/A", ln=True)
        pdf.cell(0, 10, f"Book Value Per Share (BVPS): {bvps:.2f}" if bvps else "Book Value Per Share (BVPS): N/A", ln=True)
        pdf.ln(12)
        pdf.set_font("DejaVu", "I", 10)
        pdf.set_text_color(90)
        pdf.multi_cell(0, 10, f"Disclaimer: This report is auto-generated based on public financial data from the Malawi Stock Exchange.\nAccuracy is NOT guaranteed. Please Scan the QR Code to verify {counter}'s official data. Invest wisely.")
        pdf.ln(7)
        pdf.set_font("DejaVu", "B", 10)
        pdf.set_text_color(0)
        pdf.cell(0, 10, f"For more information about {counter}, Scan the QR Code.")

        company_urls = {
            "AIRTEL": "https://mse.co.mw/company/MWAIRT001156",
            "BHL": "https://mse.co.mw/company/MWBHL001164",
            "FDHB": "https://mse.co.mw/company/MWFDHB001178",
            "FMBCH": "https://mse.co.mw/company/MWFMBCH00009",
            "ICON": "https://mse.co.mw/company/MWICON001188",
            "ILLOVO": "https://mse.co.mw/company/MWILLV001116",
            "MPICO": "https://mse.co.mw/company/MWMPICO010010",
            "NBS": "https://mse.co.mw/company/MWNBS001174",
            "NBM": "https://mse.co.mw/company/MWNBM001113",
            "NICO": "https://mse.co.mw/company/MWNICO010014",
            "NITL": "https://mse.co.mw/company/MWNITL001117",
            "OMU": "https://mse.co.mw/company/MWOMU001121",
            "PCL": "https://mse.co.mw/company/MWPCL001111",
            "STANDARD": "https://mse.co.mw/company/MWSB0001112",
            "SUNBIRD": "https://mse.co.mw/company/MWSUN001119",
            "TNM": "https://mse.co.mw/company/MWTNM001151"
        }

        qr_url = company_urls.get(counter, "https://mse.co.mw")
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_qr:
            qr_img = qrcode.make(qr_url)
            qr_img.save(tmp_qr.name)
            pdf.image(tmp_qr.name, x=160, y=240, w=40, h=40)
            qr_path = tmp_qr.name

        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_pdf:
            filename = tmp_pdf.name
            pdf.output(filename)
            response = send_file(filename, as_attachment=True, download_name=f"{counter}-Fundamentals-Report.pdf")
            os.remove(filename)
            os.remove(qr_path)
            return response
    except Exception as e:
        return jsonify({"error": str(e)}), 500
     
                
# ========== FINANCIAL REPORTS PDF DOWNLOAD ==========
@app.route('/download_sample_reports/<company>', methods=['GET'])
def download_sample_reports(company):
    company = company.upper()
    pdf_links = {
        "AIRTEL": "https://mse.co.mw/company/MWAIRT001156",
        "BHL": "https://mse.co.mw/company/MWBHL001164",
        "FDHB": "https://mse.co.mw/company/MWFDHB001178",
        "FMBCH": "https://mse.co.mw/company/MWFMBCH00009",
        "ICON": "https://mse.co.mw/company/MWICON001188",            "ILLOVO": "https://mse.co.mw/company/MWILLV001116",
        "MPICO": "https://mse.co.mw/company/MWMPICO010010",
        "NBS": "https://mse.co.mw/company/MWNBS001174",
        "NBM": "https://mse.co.mw/company/MWNBM001113",
        "NICO": "https://mse.co.mw/company/MWNICO010014",
        "NITL": "https://mse.co.mw/company/MWNITL001117",
        "OMU": "https://mse.co.mw/company/MWOMU001121",            "PCL": "https://mse.co.mw/company/MWPCL001111",
        "STANDARD": "https://mse.co.mw/company/MWSB0001112",
        "SUNBIRD": "https://mse.co.mw/company/MWSUN001119",
        "TNM": "https://mse.co.mw/company/MWTNM001151"
    }

    if company not in pdf_links:
        return jsonify({"error": f"No sample report found for {company}"}), 404

    url = pdf_links[company]
    folder = f'reports/{company}'
    os.makedirs(folder, exist_ok=True)

    try:
        filename = url.split('/')[-1]
        path = os.path.join(folder, filename)
        response = requests.get(url)
        response.raise_for_status()
        with open(path, 'wb') as f:
            f.write(response.content)
        if os.path.getsize(path) < 1000:
            os.remove(path)
            return jsonify({"error": "Downloaded file is too small or corrupt."}), 500

        return jsonify({"message": "Downloaded", "company": company, "file": filename})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ========== FUNDAMENTAL EXTRACT ==========
@app.route('/extract_fundamentals/<company>', methods=['GET'])
def extract_fundamentals(company):
    company = company.upper()
    folder = f'reports/{company}'
    if not os.path.exists(folder):
        return jsonify({"error": "No reports found for this company"}), 404

    files = [f for f in os.listdir(folder) if f.endswith('.pdf')]
    if not files:
        return jsonify({"error": "No PDF files found"}), 404

    pdf_path = os.path.join(folder, sorted(files)[-1])
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        return jsonify({"error": f"Failed to open PDF: {str(e)}"}), 500

    full_text = ""
    for page in doc:
        full_text += page.get_text()

    profit = re.search(r'Net\s+Profit\s*[:\-]?\s*[MK]*\s?([\d,]+\.\d+)', full_text, re.IGNORECASE)
    shares = re.search(r'Number\s+of\s+Shares\s+in\s+Issue\s*[:\-]?\s*([\d,]+)', full_text, re.IGNORECASE)
    dividend = re.search(r'Dividend\s+(?:Paid|Declared)?\s*[:\-]?\s*[MK]*\s?([\d,]+\.\d+)', full_text, re.IGNORECASE)
    book_value = re.search(r'Book\s+Value\s*[:\-]?\s*[MK]*\s?([\d,]+\.\d+)', full_text, re.IGNORECASE)

    try:
        net_profit = float(profit.group(1).replace(',', '')) if profit else None
        shares_out = float(shares.group(1).replace(',', '')) if shares else None
        dividend_paid = float(dividend.group(1).replace(',', '')) if dividend else None
        book_val = float(book_value.group(1).replace(',', '')) if book_value else None
    except:
        return jsonify({"error": "Failed to parse numeric values properly"}), 500

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO fundamentals
            (counter, net_profit, number_of_shares_in_issue, dividend_paid, book_value)
            VALUES (%s, %s, %s, %s, %s)
        ''', (company, net_profit, shares_out, dividend_paid, book_val))
        conn.commit()

    return jsonify({
        "company": company,
        "net_profit": net_profit if net_profit is not None else "Not found",
        "shares_outstanding": shares_out if shares_out is not None else "Not found",
        "dividend_paid": dividend_paid if dividend_paid is not None else "Not found",
        "book_value": book_val if book_val is not None else "Not found"
    })
       
  
# ========== DEBUG TEXT ROUTE ==========
@app.route('/debug_pdf_text/<company>', methods=['GET'])
def debug_pdf_text(company):
    company = company.upper()
    folder = f'reports/{company}'
    if not os.path.exists(folder):
        return jsonify({"error": "No reports found for this company"}), 404

    files = [f for f in os.listdir(folder) if f.endswith('.pdf')]
    if not files:
        return jsonify({"error": "No PDF found"}), 404

    path = os.path.join(folder, files[0])
    try:
        doc = fitz.open(path)
        text = ""
        for page in doc:
            text += page.get_text()
        return text[:10000]
    except Exception as e:
        return jsonify({"error": str(e)}), 500



# =============== ❌❌❌ ADMIN PANEL ❌❌❌ ===============

# ========== Admin Route
@app.route('/admin', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        if request.form['password'] == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('admin_dashboard'))
        return "Oops!! That Key Doesn't Fit the Lock!", 403
    return render_template_string("""
        <h2>StockMate Admin Login</h2>
        <form method="POST">
            <input type="password" name="password" placeholder="Insert Your Key Here"/>
            <button type="submit">Unlock the Vault</button>
        </form>
    """)


# ========== Admin Dashboard Route
@app.route('/admin/dashboard')
def admin_dashboard():
    if not session.get('logged_in'):
        return redirect(url_for('admin_login'))
    try:
        with open('fundamentals.json') as f:
            data = json.load(f)
    except:
        data = {}
    html = "<h2>Company Fundamentals</h2><ul>"
    for k in sorted(data.keys()):
        html += f"<li><strong>{k}</strong> — <a href='/admin/edit/{k}'>Edit</a></li>"
    html += "</ul>"
    return html


# ========== Admin Edit Company Route
@app.route('/admin/edit/<company>', methods=['GET', 'POST'])
def edit_company(company):
    if not session.get('logged_in'):
        return redirect(url_for('admin_login'))
    company = company.upper()
    try:
        with open('fundamentals.json') as f:
            data = json.load(f)
    except:
        data = {}
    if request.method == 'POST':
        data[company] = {
            "net_profit": request.form['net_profit'],
            "number_of_shares_in_issue": request.form['number_of_shares_in_issue'],
            "dividend_paid": request.form['dividend_paid'],
            "book_value": request.form['book_value']
        }
        with open('fundamentals.json', 'w') as f:
            json.dump(data, f, indent=2)
        return redirect(url_for('admin_dashboard'))
    values = data.get(company, {"net_profit":"", "number_of_shares_in_issue":"", "dividend_paid":"", "book_value":""})
    return render_template_string(f"""
        <h2>Edit Fundamentals for {company}</h2>
        <form method="POST">
            Net Profit: <input name="net_profit" value="{values['net_profit']}"/><br>
            Number of Shares Issued: <input name="number_of_shares_in_issue" value="{values['number_of_shares_in_issue']}"/><br>
            Dividend Paid: <input name="dividend_paid" value="{values['dividend_paid']}"/><br>
            Book Value: <input name="book_value" value="{values['book_value']}"/><br>
            <button type="submit">Save</button>
        </form>
        <a href="/admin/dashboard">← Back to dashboard</a>
    """)

    
# ========== SCHEDULER ==========
scheduler = BackgroundScheduler()

def scheduled_scrape():
    print("Scheduled scrape running...")
    data = scrape_mse()
    if data:
        save_data(data)

def shutdown_scheduler(*args):
    scheduler.shutdown(wait=True)
    print("Scheduler shut down Gracefully")


# ========== INIT ==========
if __name__ == '__main__':
    init_db()
    initialize_fundamentals()
    scheduler.add_job(scheduled_scrape, trigger='interval', minutes=5)
    scheduler.start()
    signal.signal(signal.SIGTERM, shutdown_scheduler)
    signal.signal(signal.SIGINT, shutdown_scheduler)
    try:
        app.run(host='0.0.0.0', port=5000, debug=False)
    finally:
        shutdown_scheduler()