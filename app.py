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
import psycopg2.extras
from PIL import Image
from fpdf import FPDF
from io import BytesIO
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
from psycopg2 import pool, sql
from dotenv import load_dotenv
from contextlib import contextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify, render_template_string, redirect, url_for, session, send_file, Response, abort 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ========== LOAD ENV ===========
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

# ========== LOGGING ==========
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========== FLASK APP ==========
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

# ========== DB POOL ==========
db_pool = pool.SimpleConnectionPool(1, 10, DATABASE_URL)

def get_conn():
    return db_pool.getconn()

def put_conn(conn):
    db_pool.putconn(conn)

@contextmanager
def get_db_connection():
    conn = get_conn()
    try:
        yield conn
    finally:
        put_conn(conn)

# ========== HELPERS ==========
def normalize_counter(counter):
    if not counter:
        return None
    return counter.strip().upper()

def safe_float(val):
    try:
        return float(str(val).replace(',', ''))
    except:
        return None

def safe_int(val):
    try:
        return int(str(val).replace(',', ''))
    except:
        return None

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
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                id SERIAL PRIMARY KEY,
                counter TEXT NOT NULL,
                last_price NUMERIC,
                change NUMERIC,
                volume BIGINT,
                turnover NUMERIC,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fundamentals (
                id SERIAL PRIMARY KEY,
                counter TEXT NOT NULL,
                net_profit NUMERIC,
                number_of_shares BIGINT,
                dividend_paid NUMERIC,               
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
       

# ==================== SCRAPE MSE ====================
def scrape_mse():
    url = "https://www.mse.co.mw/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table", {"class": "table"})
        if not table:
            return []
        data = []
        rows = table.find_all("tr")
        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) >= 5:
                data.append({
                    'Counter': cols[0].text.strip(),
                    'Last Price (MK)': safe_float(cols[1].text),
                    '% Change': safe_float(cols[2].text),
                    'Volume': safe_int(cols[3].text),
                    'Turnover (MK)': safe_float(cols[4].text)
                })
        return data
    except Exception as e:
        logger.error(f"Scraping Error: {e}")
        return []    

# ==================== SAVE ====================
def save_data(data):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for item in data:
            cursor.execute('''
                INSERT INTO stocks (counter, last_price, change, volume, turnover)
                VALUES (%s, %s, %s, %s, %s)
            ''', (item['Counter'], item['Last Price (MK)'], item['% Change'], item['Volume'], item['Turnover (MK)']))
        conn.commit()


# ================= API ROUTES =================
# ======= Home Route
@app.route("/") 
def home():
    return "Hello There! StockMate API is Running!✨️"

# ======= Scrape Route
@app.route("/scrape", methods=["GET"])
def scrape_and_save():
    data = scrape_mse()
    if data:
        save_data(data)
        return jsonify({"message": "Success! Data Scraped and Saved", "scraped": len(data), "inserted": len(data)})
    else:
        return jsonify({"error": "Failed to Scrape Data"}), 500
      
# ======= Stocks Route
@app.route("/stocks", methods=["GET"])
def get_stocks():
    limit = int(request.args.get("limit", 30))
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT counter, last_price, change, volume, turnover, timestamp
            FROM stocks
            ORDER BY timestamp DESC
            LIMIT %s
        """, (limit,))
        rows = cursor.fetchall()
        
        result = []
        for r in rows:
            result.append({
                "counter": r[0],
                "last_price": float(r[1]) if r[1] is not None else None,
                "change": float(r[2]) if r[2] is not None else None,
                "volume": int(r[3]) if r[3] is not None else None,
                "turnover": float(r[4]) if r[4] is not None else None,
                "timestamp": convert_to_local_time(r[5])
            })
        return jsonify(result) 
  
# ======= Latest Prices Route
@app.route("/latest_prices", methods=["GET"])
def latest_prices():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT ON (counter)
                counter, last_price, change, volume, turnover, timestamp
            FROM stocks
            ORDER BY counter, timestamp DESC
        """)
        rows = cursor.fetchall()
       
        result = []
        for r in rows:
            result.append({
                "counter": r[0],
                "last_price": float(r[1]) if r[1] is not None else None,
                "change": float(r[2]) if r[2] is not None else None,
                "volume": int(r[3]) if r[3] is not None else None,
                "turnover": float(r[4]) if r[4] is not None else None,
                "timestamp": convert_to_local_time(r[5])
            })
        return jsonify(result)

# ======= History Route
@app.route("/history/<counter>", methods=["GET"])
def get_history(counter):
    limit = int(request.args.get("limit", 30))
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT timestamp, last_price
            FROM stocks
            WHERE counter = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (counter.upper(), limit))
        rows = cursor.fetchall()
        
        history = [{
            "timestamp": convert_to_local_time(r[0]), 
            "last_price": float(r[1]) if r[1] is not None else None
        } for r in rows]
        return jsonify(list(reversed(history)))
            
# ======= Insert Fundamentals Route
@app.route("/insert_fundamentals", methods=["POST"])
def insert_fundamentals():
    data = request.json
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for entry in data:
            counter = normalize_counter(entry.get("counter"))
            if not counter:
                continue              
            cursor.execute("""
                INSERT INTO fundamentals (counter, net_profit, number_of_shares, dividend_paid)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (counter)
                DO UPDATE SET
                    net_profit = EXCLUDED.net_profit,
                    number_of_shares = EXCLUDED.number_of_shares,
                    dividend_paid = EXCLUDED.dividend_paid,                    
                    timestamp = CURRENT_TIMESTAMP;
            """, (
                counter,
                safe_float(entry.get("net_profit")),
                safe_int(entry.get("number_of_shares")),
                safe_float(entry.get("dividend_paid"))                
            ))
        conn.commit()        
    return jsonify({"message": "Fundamentals inserted/updated Successfully"})

# ======= Fundamentals Route
@app.route("/fundamentals/<counter>", methods=["GET"])
def get_fundamentals(counter):
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT net_profit, number_of_shares, dividend_paid
                FROM fundamentals
                WHERE counter = %s
             """, (counter,))
            row = cursor.fetchone()
        
        if not row:
             return jsonify({"error": "Data not available for this company"}), 404

        net_profit, number_of_shares, dividend_paid = row

        net_profit = safe_float(net_profit) or 0
        number_of_shares = safe_int(number_of_shares) or 0
        dividend_paid = safe_float(dividend_paid) or 0       

        # Fundamentals 
        eps = net_profit / number_of_shares if number_of_shares else 0        
        dvps = dividend_paid / number_of_shares if number_of_shares else 0
        
         
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT last_price FROM stocks
                WHERE counter = %s
                ORDER BY timestamp DESC
                LIMIT 1
                """, (counter,))
            row = cursor.fetchone()
          
            if not row:
                return jsonify({"error": "Price data not available"}), 404
            last_price = safe_float(row[0]) or 0

            # Fundamentals
            pe_ratio = last_price / eps if eps else None
            market_cap = last_price * number_of_shares if number_of_shares else 0
            div_yield = (dvps / last_price) * 100 if last_price and dvps else None

            return jsonify({
                "counter": counter,
                "eps": round(eps, 2),
                "pe_ratio": round(pe_ratio, 2) if pe_ratio is not None else None,
                "market_cap": round(market_cap, 2) if market_cap is not None else None,
                "div_yield_percent": round(div_yield, 2) if div_yield is not None else None,
                "last_price": round(last_price, 2)
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ======= Metrics Route
@app.route("/metrics/<counter>", methods=["GET"])
def stock_metrics(counter):
    counter = normalize_counter(counter)
    if not counter:
        return jsonify({"error": "Invalid counter"}), 400
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT net_profit, number_of_shares, dividend_paid
                FROM fundamentals 
                WHERE counter = %s
            """, (counter,))
            fundamental = cursor.fetchone()
           
        if not fundamental:
            return jsonify({"error": "Fundamentals not available"}), 404

        net_profit, number_of_shares, dividend_paid = fundamental
        
        eps = (safe_float(net_profit) or 0) / (safe_int(number_of_shares) or 1)       
        dvps = (safe_float(dividend_paid) or 0) / (safe_int(number_of_shares) or 1)

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT last_price, change, volume, turnover, timestamp
                FROM stocks 
                WHERE counter = %s
                ORDER BY timestamp DESC 
                LIMIT 1
            """, (counter,))
            latest = cursor.fetchone()
            
        if not latest:
            return jsonify({"error": "Latest price data unavailable"}), 404

        last_price = safe_float(latest[0]) or 0
        pe_ratio = last_price / eps if eps else None  
        market_cap = last_price * number_of_shares if number_of_shares else 0     
        div_yield = (dvps / last_price) * 100 if last_price and dvps else None

        return jsonify({
            "counter": counter,
            "last_price": round(last_price, 2),           
            "eps": round(eps, 2),
            "pe_ratio": round(pe_ratio, 2) if pe_ratio is not None else None,
            "market_cap": round(market_cap, 2) if market_cap is not None else None,
            "div_yield_percent": round(div_yield, 2) if div_yield is not None else None
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
                SELECT net_profit, number_of_shares, dividend_paid 
                FROM fundamentals 
                WHERE counter = %s
            ''', (counter,))
            row = cursor.fetchone()
        if not row:
            return jsonify({"error": "Data not available for this company"}), 404

        net_profit, number_of_shares, dividend_paid = row
        net_profit = safe_float(str(net_profit).replace(',', '')) if net_profit else 0
        number_of_shares = safe_int(str(number_of_shares).replace(',', '')) if number_of_shares else 0
        dividend_paid = safe_float(str(dividend_paid).replace(',', '')) if dividend_paid else 0
        
        # Fundamentals 
        eps = net_profit / number_of_shares if number_of_shares else 0       
        dvps = dividend_paid / number_of_shares if number_of_shares else 0

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT last_price FROM stocks
                WHERE counter = %s
                ORDER BY timestamp DESC 
                LIMIT 1
            ''', (counter,))
            result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Price data not Available"}), 404

        last_price = float(str(result[0]).replace(',', '')) if result[0] else 0
        pe_ratio = last_price / eps if eps else None
        market_cap = last_price * number_of_shares if number_of_shares else 0     
        div_yield = (dvps / last_price) * 100 if last_price and dvps else None

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
        pdf.cell(0, 10, f"Latest Price: MK {last_price:,.2f}" if last_price else "Latest Price: N/A", ln=True)
        pdf.cell(0, 10, f"Net Profit: MK {net_profit:,.2f}" if net_profit else "Net Profit: N/A", ln=True)
        pdf.cell(0, 10, f"Dividend Paid: MK {dividend_paid:,.2f}" if dividend_paid else "Dividend Paid: N/A", ln=True)
        pdf.cell(0, 10, f"Number of Shares: {number_of_shares:,.0f}" if number_of_shares else "Number of Shares: N/A", ln=True)              
        pdf.ln(5)
        pdf.set_font("DejaVu", "B", 16)
        pdf.cell(0, 10, "Key Financial Metrics", ln=True)
        pdf.set_font("DejaVu", "", 12)
        pdf.cell(0, 10, f"Earnings Per Share (EPS): {eps:.2f}" if eps else "Earnings Per Share (EPS): N/A", ln=True)
        pdf.cell(0, 10, f"P/E Ratio: {pe_ratio:.2f}" if pe_ratio else "P/E Ratio: N/A", ln=True)
        pdf.cell(0, 10, f"Dividend Yield: {div_yield:.2f}%" if div_yield else "Dividend Yield: N/A", ln=True)
        pdf.cell(0, 10, f"Market Capitalization: MK {market_cap:,.2f}" if market_cap else "Market Capitalization: N/A", ln=True)
        pdf.ln(10)
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
    if not session.get("logged_in"):
        return redirect(url_for("admin_login"))
    # list fundamentals in DB
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT counter, net_profit, number_of_shares, dividend_paid
            FROM fundamentals 
            ORDER BY counter
        """)
        rows = cursor.fetchall()
        
    html = "<h2>Company Fundamentals (DB)</h2><ul>"
    for r in rows:
        html += f"<li><strong>{r[0]}</strong> — <a href='/admin/edit/{r[0]}'>Edit</a></li>"
    html += "</ul>"
    html += "<p><a href='/admin/add'>Add new company</a></p>"
    return html


# ========== Admin Add Company Route
@app.route("/admin/add", methods=["GET", "POST"])
def admin_add():
    if not session.get("logged_in"):
        return redirect(url_for("admin_login"))
    if request.method == "POST":
        payload = {
            "counter": request.form.get("counter"),
            "net_profit": request.form.get("net_profit"),
            "number_of_shares": request.form.get("number_of_shares"),
            "dividend_paid": request.form.get("dividend_paid")            
        }

        # Use insert_fundamentals logic (single item)
        with get_db_connection() as conn:
            cursor = conn.cursor()
            counter = normalize_counter(payload["counter"])
            if not counter:
                return "Invalid counter", 400
            cursor.execute("""
                INSERT INTO fundamentals (counter, net_profit, number_of_shares, dividend_paid)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (counter) DO UPDATE
                SET net_profit = EXCLUDED.net_profit,
                    number_of_shares = EXCLUDED.number_of_shares,
                    dividend = EXCLUDED.dividend_paid                                                         
            """, (
                counter, 
                safe_float(payload["net_profit"]),
                safe_int(payload["number_of_shares"]),
                safe_float(payload["dividend_paid"])                 
            ))
        conn.commit()
    return redirect(url_for("admin_dashboard"))

    return render_template_string("""
        <h2>Add Company Fundamentals</h2>
        <form method="POST">
            Counter: <input name="counter"/><br/>
            Net Profit: <input name="net_profit"/><br/>
            Number of Shares: <input name="number_of_shares"/><br/>
            Dividend Paid: <input name="dividend_paid"/><br/>           
            <button type="submit">Save</button>
        </form>
        <a href="/admin/dashboard">← Back</a>
    """)


# ========== Admin Edit Counter Route
@app.route("/admin/edit/<counter>", methods=["GET", "POST"])
def edit_counter(counter):
    if not session.get("logged_in"):
        return redirect(url_for("admin_login"))
    counter = normalize_counter(counter)
    if not counter:
        return "Invalid Counter", 400
    if request.method == "POST":
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE fundamentals 
                SET net_profit = %s, number_of_shares = %s, dividend_paid = %s
                WHERE counter = %s
            """, (safe_float(request.form.get("net_profit")), safe_int(request.form.get("number_of_shares")),
                  safe_float(request.form.get("dividend_paid"))
        conn.commit()           
    return redirect(url_for("admin_dashboard"))

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT net_profit, number_of_shares, dividend_paid 
            FROM fundamentals 
            WHERE counter=%s", (counter,))
        row = cursor.fetchone()
        
        values = {"net_profit": "", "number_of_shares": "", "dividend_paid": ""}
        if row:
            values["net_profit"], values["number_of_shares"], values["dividend_paid"] = row
        return render_template_string(f"""
            <h2>Edit Fundamentals for {counter}</h2>
            <form method="POST">
                Net Profit: <input name="net_profit" value="{values['net_profit']}"/><br>
                Number of Shares: <input name="number_of_shares" value="{values['number_of_shares']}"/><br>
                Dividend Paid: <input name="dividend_paid" value="{values['dividend_paid']}"/><br>            
                <button type="submit">Save</button>
            </form>
            <a href="/admin/dashboard"> Back to dashboard</a>
        """)
 
   
# ========== SCHEDULER ==========
scheduler = BackgroundScheduler()

def scheduled_scrape():
    logger.info("Scheduled scrape running...")
    try:
        data = scrape_mse()
        if data:
            save_data(data)
            logger.info("Scheduled scrape Successful")
    except Exception:
        logger.exception("Scheduled scrape Failed")

def shutdown_scheduler(*args):
    try:
        scheduler.shutdown(wait=True)
    except Exception:
        pass
    logger.info("Scheduler shut down Gracefully")


# ========== App Startup ==========
def start_services():
    init_db()
    # Optionally seed fundamentals here by calling initialize_fundamentals_seed([...])
    # Start scheduler
    scheduler.add_job(scheduled_scrape, trigger="interval", minutes=5, next_run_time=datetime.utcnow())
    scheduler.start()
    # trap signals
    signal.signal(signal.SIGINT, lambda *a: shutdown_scheduler())
    signal.signal(signal.SIGTERM, lambda *a: shutdown_scheduler())
    logger.info("Services started.")

if __name__ == "__main__":
    start_services()
    try:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=False)
    finally:
        shutdown_scheduler()
