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
import sqlite3
import requests
import tempfile
import requests_cache
import secrets 
from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup
from fpdf import FPDF
from dotenv import load_dotenv
from datetime import datetime
from PIL import Image
from flask import Flask, request, jsonify, render_template_string, redirect, url_for, session, send_file
from contextlib import contextmanager


# ========== Load Environment Variables ==========
load_dotenv()


# ========== Enable Request Caching ==========
requests_cache.install_cache('mse_cache', expire_after=300)


# ========== Configuration ==========
DATABASE_PATH = os.getenv('DATABASE_PATH', '/app/database.db')
FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY', 'fallback-secret-key-for-dev-only')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'default-dev-password')


# ========== Validate Environment Variables ==========
required_vars = ['DATABASE_PATH', 'FLASK_SECRET_KEY', 'ADMIN_PASSWORD']
for var in required_vars:
    if not os.getenv(var):
        raise ValueError(f"Environment Variable {var} is not Set")


# ========== FLASK APP ==========
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY


# ========== Database Context Manager ==========
@contextmanager
def get_db_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    try:
        yield conn
    finally:
        conn.close()


# ========== TIMEZONE CONVERTER ==========
def convert_to_local_time(utc_time_str):
    try:
        utc = pytz.utc
        local = pytz.timezone('Africa/Blantyre')  # GMT+2
        utc_dt = datetime.strptime(utc_time_str, '%Y-%m-%d %H:%M:%S')
        local_dt = utc.localize(utc_dt).astimezone(local)
        return local_dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return utc_time_str 
        

# ========== DATABASE INIT ==========
def init_db():
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                counter TEXT,
                last_price TEXT,
                change TEXT,
                volume TEXT,
                turnover TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS fundamentals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                counter TEXT UNIQUE,
                net_profit REAL,
                number_of_shares_in_issue INTEGER,
                dividend_paid REAL,
                book_value REAL
            )
        ''')
        conn.commit()

def initialize_fundamentals():
    fundamentals_data = [
        {
            "counter": "AIRTEL",
            "net_profit": "42714422219.62",
            "number_of_shares_in_issue": "11000000000",
            "dividend_paid": "21875568000.00",
            "book_value": "32120000000.00"
        },
        {
            "counter": "BHL",
            "net_profit": "-1369168339.45",
            "number_of_shares_in_issue": "5878254935",
            "dividend_paid": "0.00",
            "book_value": "65131064679.80"
        },
        {
            "counter": "FDHB",
            "net_profit": "74055922113.91",
            "number_of_shares_in_issue": "6901031250",
            "dividend_paid": "32720549568.75",
            "book_value": "97373550937.50"
        },
        {
            "counter": "FMBCH",
            "net_profit": "118254740000.00",
            "number_of_shares_in_issue": "2458250000",
            "dividend_paid": "8850053988.00",
            "book_value": "329085927500.00"
        },
        {
            "counter": "ICON",           
            "net_profit": "24490000.00",
            "number_of_shares_in_issue": "6680000000",
            "dividend_paid": "1942477200.00",
            "book_value": "146225200000.00"
        },
        {
            "counter": "ILLOVO",
            "net_profit": "22631873664.47",
            "number_of_shares_in_issue": "713444391",
            "dividend_paid": "3578500083.93",
            "book_value": "148781693299.14"
        },
        {
            "counter": "MPICO",
            "net_profit": "8535675173.68",
            "number_of_shares_in_issue": "2298047460",
            "dividend_paid": "987300938.05",
            "book_value": "65195606440.20"
        },
        {
            "counter": "NBM",
            "net_profit": "102283000000.00",
            "number_of_shares_in_issue": "466931738",
            "dividend_paid": "59060764860.77",
            "book_value": "268560458428.08"
        },
        {
            "counter": "NBS",
            "net_profit": "72978138905.20",
            "number_of_shares_in_issue": "2910573356",
            "dividend_paid": "63647021073.80",
            "book_value": "112057074206.00"
        },
        {
            "counter": "NICO",
            "net_profit": "72006217688.65",
            "number_of_shares_in_issue": "1043041096",
            "dividend_paid": "22981533076.39",
            "book_value": "155726035632.8"
        },
        {
            "counter": "NITL",
            "net_profit": "29759480000.00",
            "number_of_shares_in_issue": "135000000",
            "dividend_paid": "1715933700.00",
            "book_value": "73803150000.00"
        },
        {
            "counter": "OMU",
            "net_profit": "2595650000.00",
            "number_of_shares_in_issue": "16977551",
            "dividend_paid": "1404909203.96",
            "book_value": "19469855486.80"
        },
        {
            "counter": "PCL",
            "net_profit": "64673000000.00",
            "number_of_shares_in_issue": "120255820",
            "dividend_paid": "1346858449.67",
            "book_value": "348566304502.80"
        },
        {
            "counter": "STANDARD",
            "net_profit": "86365000000.00",
            "number_of_shares_in_issue": "234668162",
            "dividend_paid": "43881111188.97",
            "book_value": "259843362419.36"
        },
        {
            "counter": "SUNBIRD",
            "net_profit": "10624630000.00",
            "number_of_shares_in_issue": "261582580",
            "dividend_paid": "3396746848.44",
            "book_value": "69889633724.40"
        },
        {
            "counter": "TNM",
            "net_profit": "10060000000.00",
            "number_of_shares_in_issue": "11541200375",
            "dividend_paid": "0.00",
            "book_value": "51819989685.90"
        }
    ]

    with get_db_connection() as conn:
        cursor = conn.cursor()
        for entry in fundamentals_data:
            cursor.execute('''
                INSERT OR IGNORE INTO fundamentals (counter, net_profit, number_of_shares_in_issue, dividend_paid, book_value)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                entry["counter"],
                float(entry["net_profit"].replace(',', '')),
                float(entry["number_of_shares_in_issue"].replace(',', '')),
                float(entry["dividend_paid"].replace(',', '')),
                float(entry["book_value"].replace(',', ''))
            ))
        conn.commit()


# ========== SCRAPE ==========
def scrape_mse():
    url = 'https://www.mse.co.mw/'
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table')
        data = []
        if not table:
            return []
        rows = table.find_all('tr')
        for row in rows[1:]:
            cols = row.find_all('td')
            if len(cols) >= 5:
                data.append({
                    'Counter': cols[0].text.strip(),
                    'Last Price (MK)': cols[1].text.strip(),
                    '% Change': cols[2].text.strip(),
                    'Volume': cols[3].text.strip(),
                    'Turnover (MK)': cols[4].text.strip()
                })
        return data
    except Exception as e:
        print("Scraping Error:", e)
        return []


# ========== SAVE ==========
def save_data(stock_data):
    with get_db_connection() as conn:
        c = conn.cursor()
        for item in stock_data:
            c.execute('''
                SELECT 1 FROM stocks
                WHERE counter = ? AND last_price = ? AND change = ? AND volume = ? AND turnover = ?
                AND timestamp >= datetime('now', '-1 hour')
            ''', (item['Counter'], item['Last Price (MK)'], item['% Change'], item['Volume'], item['Turnover (MK)']))
            if not c.fetchone():
                c.execute('''
                    INSERT INTO stocks (counter, last_price, change, volume, turnover)
                    VALUES (?, ?, ?, ?, ?)
                ''', (item['Counter'], item['Last Price (MK)'], item['% Change'], item['Volume'], item['Turnover (MK)']))
        conn.commit()


# ========== API ROUTES ==========

# ======= Home Route
@app.route('/')
def home():
    return "Hello There! StockMate API is Running!"


# ======= Scrape Route
@app.route('/scrape', methods=['GET'])
def scrape_and_save():
    data = scrape_mse()
    if data:
        save_data(data)
        return jsonify({"message": "Success!! Data Scraped and Saved", "count": len(data)})
    else:
        return jsonify({"error": "Failed to scrape data"}), 500


# ======= Stocks Route
@app.route('/stocks', methods=['GET'])
def get_stocks():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT counter, last_price, change, volume, turnover, timestamp FROM stocks ORDER BY timestamp DESC LIMIT 20')
        rows = cursor.fetchall()
        return jsonify([
            {"counter": r[0], "last_price": r[1], "change": r[2], "volume": r[3], "turnover": r[4], "timestamp": convert_to_local_time(r[5])}
            for r in rows
        ])


# ======= Latest Prices Route
@app.route('/latest_prices', methods=['GET'])
def latest_prices():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT counter, last_price, change, volume, turnover, MAX(timestamp)
            FROM stocks
            GROUP BY counter
        ''')
        rows = cursor.fetchall()
        return jsonify([
            {"counter": r[0], "last_price": r[1], "change": r[2], "volume": r[3], "turnover": r[4], "timestamp": convert_to_local_time(r[5])}
            for r in rows
        ])


# ======= History Route
@app.route('/history/<counter>', methods=['GET'])
def get_history(counter):
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT timestamp, last_price
                FROM stocks
                WHERE counter = ?
                ORDER BY timestamp DESC
                LIMIT 10
            ''', (counter,))
            rows = cursor.fetchall()
        history = [
            {
                "date": convert_to_local_time(row[0]),
                "price": float(str(row[1]).replace(',', '')) if row[1] else None
            } for row in rows
        ]
        return jsonify(history[::-1] if request.args.get('order') == 'asc' else history)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ======= Insert Fundamentals 
@app.route('/insert_fundamentals', methods=['POST'])
def insert_fundamentals():
    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400
    with get_db_connection() as conn:
        c = conn.cursor()
        for entry in data:
            c.execute('''
                INSERT OR REPLACE INTO fundamentals (counter, net_profit, number_of_shares_in_issue, dividend_paid, book_value)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                entry['counter'],
                float(str(entry['net_profit']).replace(',', '')),
                float(str(entry['number_of_shares_in_issue']).replace(',', '')),
                float(str(entry['dividend_paid']).replace(',', '')),
                float(str(entry['book_value']).replace(',', ''))
            ))
        conn.commit()
    return jsonify({"message": "Fundamentals inserted successfully"})


# ======= Fundamentals Route
@app.route('/fundamentals/<counter>', methods=['GET'])
def get_fundamentals(counter):
    counter = counter.upper()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT net_profit, number_of_shares_in_issue, dividend_paid, book_value
                FROM fundamentals WHERE counter = ?
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
                WHERE counter = ?
                ORDER BY timestamp DESC
                LIMIT 1
            ''', (counter,))
            result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Price data not available"}), 404

        price = float(str(result[0]).replace(',', '')) if result[0] else 0
        pe_ratio = price / eps if eps else None
        pb_ratio = price / bvps if bvps else None
        div_yield = (dvps / price) * 100 if price and dvps else None

        return jsonify({
            "eps": f"{eps:.2f}",
            "pe_ratio": f"{pe_ratio:.2f}" if pe_ratio else "N/A",
            "pb_ratio": f"{pb_ratio:.2f}" if pb_ratio else "N/A",
            "div_yield": f"{div_yield:.2f}%" if div_yield else "N/A"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ======= Metrics Route
@app.route('/metrics/<counter>', methods=['GET'])
def stock_metrics(counter):
    counter = counter.upper()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT net_profit, number_of_shares_in_issue, dividend_paid, book_value
                FROM fundamentals WHERE counter = ?
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
                SELECT last_price, change, volume, turnover, timestamp
                FROM stocks
                WHERE counter = ?
                ORDER BY timestamp DESC
                LIMIT 1
            ''', (counter,))
            result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Latest Price data not found"}), 404

        price = float(str(result[0]).replace(',', '')) if result[0] else 0
        pe_ratio = price / eps if eps else None
        pb_ratio = price / bvps if bvps else None
        div_yield = (dvps / price) * 100 if price and dvps else None

        return jsonify({
            "counter": counter,
            "last_price": f"{price:.2f}",
            "change": result[1],
            "volume": result[2],
            "turnover": result[3],
            "timestamp": convert_to_local_time(result[4]),
            "eps": f"{eps:.2f}",
            "pe_ratio": f"{pe_ratio:.2f}" if pe_ratio else "N/A",
            "pb_ratio": f"{pb_ratio:.2f}" if pb_ratio else "N/A",
            "div_yield": f"{div_yield:.2f}%" if div_yield else "N/A"
        })
    except Exception as e:
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
                FROM fundamentals WHERE counter = ?
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
                WHERE counter = ?
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
            VALUES (?, ?, ?, ?, ?)
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