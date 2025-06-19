# StockMate Flask App

import os
import re
import json
import logging
import sqlite3
import requests
import fitz  # PyMuPDF
from datetime import datetime
from flask import Flask, jsonify, request
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

DB_PATH = 'database.db'
FUNDAMENTALS_PATH = 'data/fundamentals.json'

# Initialize SQLite DB
def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            counter TEXT,
            last_price REAL,
            change REAL,
            volume INTEGER,
            turnover REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

# Helper to parse numbers from strings
def parse_number(value):
    try:
        return float(value.replace(',', '').replace('%', ''))
    except:
        return None

# Scrape MSE website
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

        for row in table.find_all('tr')[1:]:
            cols = row.find_all('td')
            if len(cols) >= 5:
                data.append({
                    'Counter': cols[0].text.strip(),
                    'Last Price': parse_number(cols[1].text.strip()),
                    'Change': parse_number(cols[2].text.strip()),
                    'Volume': int(cols[3].text.strip().replace(',', '') or 0),
                    'Turnover': parse_number(cols[4].text.strip())
                })

        return data
    except Exception as e:
        logging.error(f"Scraping error: {e}")
        return []

# Save scraped data to DB
def save_data(stock_data):
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    for item in stock_data:
        c.execute('''
            SELECT 1 FROM stocks
            WHERE counter = ? AND last_price = ? AND change = ? AND volume = ? AND turnover = ?
            AND timestamp >= datetime('now', '-1 hour')
        ''', (item['Counter'], item['Last Price'], item['Change'], item['Volume'], item['Turnover']))

        if not c.fetchone():
            c.execute('''
                INSERT INTO stocks (counter, last_price, change, volume, turnover)
                VALUES (?, ?, ?, ?, ?)
            ''', (item['Counter'], item['Last Price'], item['Change'], item['Volume'], item['Turnover']))

    conn.commit()
    conn.close()

# API Endpoints
@app.route('/')
def home():
    return "StockMate API is running!"

@app.route('/scrape', methods=['GET'])
def scrape_and_save():
    data = scrape_mse()
    if data:
        save_data(data)
        return jsonify({"message": "Data scraped and saved", "count": len(data)})
    return jsonify({"error": "Failed to scrape data"}), 500

@app.route('/stocks', methods=['GET'])
def get_stocks():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 20))
    offset = (page - 1) * limit

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM stocks ORDER BY timestamp DESC LIMIT ? OFFSET ?', (limit, offset))
    rows = c.fetchall()
    conn.close()

    return jsonify([dict(row) for row in rows])

@app.route('/latest_prices', methods=['GET'])
def latest_prices():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('''
        SELECT counter, last_price, change, volume, turnover, MAX(timestamp)
        FROM stocks GROUP BY counter
    ''')
    rows = c.fetchall()
    conn.close()

    return jsonify([
        {
            "counter": r[0], "last_price": r[1], "change": r[2],
            "volume": r[3], "turnover": r[4], "timestamp": r[5]
        } for r in rows
    ])

@app.route('/price_history/<counter>', methods=['GET'])
def price_history(counter):
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('''SELECT timestamp, last_price FROM stocks WHERE counter = ? ORDER BY timestamp DESC LIMIT 10''', (counter,))
    rows = c.fetchall()
    conn.close()
    return jsonify([{"timestamp": r[0], "price": r[1]} for r in reversed(rows)])

@app.route('/fundamentals/<counter>', methods=['GET'])
def get_fundamentals(counter):
    def clean_number(val):
        try:
            return float(str(val).replace(',', ''))
        except:
            return 0.0

    try:
        with open(FUNDAMENTALS_PATH) as f:
            data = json.load(f)

        company = data.get(counter.upper())
        if not company:
            return jsonify({"error": "Data not available for this company"}), 404

        net_profit = clean_number(company.get("net_profit", 0))
        equity = clean_number(company.get("equity", 0))
        shares_outstanding = clean_number(company.get("shares_outstanding", 0))
        dividend = clean_number(company.get("dividend_paid", 0))
        book_value = clean_number(company.get("book_value", equity))  # fallback to equity

        eps = net_profit / shares_outstanding if shares_outstanding else 0
        bvps = book_value / shares_outstanding if shares_outstanding else 0

        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT last_price FROM stocks WHERE counter = ? ORDER BY timestamp DESC LIMIT 1', (counter,))
        result = cursor.fetchone()
        conn.close()

        if not result:
            return jsonify({"error": "Price data not available"}), 404

        price = float(result[0])
        pe_ratio = price / eps if eps else None
        pb_ratio = price / bvps if bvps else None
        div_yield = (dividend / price) * 100 if price else None
        roe = (net_profit / equity) * 100 if equity else None

        return jsonify({
            "eps": f"{eps:.2f}" if eps else "N/A",
            "pe_ratio": f"{pe_ratio:.2f}" if pe_ratio else "N/A",
            "pb_ratio": f"{pb_ratio:.2f}" if pb_ratio else "N/A",
            "div_yield": f"{div_yield:.2f}%" if div_yield else "N/A",
            "roe": f"{roe:.2f}%" if roe else "N/A"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/history/<counter>', methods=['GET'])
def get_price_history(counter):
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT DATE(timestamp), last_price FROM stocks WHERE counter = ? ORDER BY timestamp ASC', (counter,))
        rows = c.fetchall()
        conn.close()

        history = []
        for row in rows:
            try:
                history.append({"date": row[0], "price": float(row[1])})
            except:
                continue
        return jsonify(history)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/download_sample_reports/<company>', methods=['GET'])
def download_sample_reports(company):
    pdf_links = {
        "NICO": "https://mse.co.mw/wp-content/uploads/2024/04/NICO-Annual-Report-2023.pdf",
        "FMBCH": "https://mse.co.mw/wp-content/uploads/2024/04/FMBCH-FY23.pdf",
        "STAND": "https://mse.co.mw/wp-content/uploads/2024/04/STAND-FY23-Financials.pdf"
    }

    company = company.upper()
    if company not in pdf_links:
        return jsonify({"error": "No sample report found for this company"}), 404

    os.makedirs(f"reports/{company}", exist_ok=True)
    url = pdf_links[company]
    filename = url.split('/')[-1]
    path = os.path.join(f"reports/{company}", filename)

    try:
        res = requests.get(url)
        res.raise_for_status()
        with open(path, 'wb') as f:
            f.write(res.content)

        if os.path.getsize(path) < 1000:
            os.remove(path)
            return jsonify({"error": "Downloaded file is too small or corrupt."}), 500

        return jsonify({"message": "Downloaded", "file": filename})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/extract_fundamentals/<company>', methods=['GET'])
def extract_fundamentals(company):
    company = company.upper()
    folder = f"reports/{company}"
    os.makedirs("data", exist_ok=True)

    if not os.path.exists(folder):
        return jsonify({"error": "No reports found for this company"}), 404

    pdf_files = [f for f in os.listdir(folder) if f.endswith('.pdf')]
    if not pdf_files:
        return jsonify({"error": "No PDF files found"}), 404

    latest_pdf = os.path.join(folder, sorted(pdf_files)[-1])
    try:
        doc = fitz.open(latest_pdf)
        text = "".join([page.get_text() for page in doc])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    fundamentals = {
        "company": company,
        "net_profit": re.search(r'Net Profit.*?([\d,]+\.\d+)', text, re.IGNORECASE).group(1) if re.search(r'Net Profit.*?([\d,]+\.\d+)', text, re.IGNORECASE) else "Not found",
        "equity": re.search(r'Total Equity.*?([\d,]+\.\d+)', text, re.IGNORECASE).group(1) if re.search(r'Total Equity.*?([\d,]+\.\d+)', text, re.IGNORECASE) else "Not found",
        "shares_outstanding": re.search(r'Shares.*?Outstanding.*?([\d,]+)', text, re.IGNORECASE).group(1) if re.search(r'Shares.*?Outstanding.*?([\d,]+)', text, re.IGNORECASE) else "Not found",
        "dividend": re.search(r'Dividend.*?([\d,]+\.\d+)', text, re.IGNORECASE).group(1) if re.search(r'Dividend.*?([\d,]+\.\d+)', text, re.IGNORECASE) else "Not found"
    }

    with open(FUNDAMENTALS_PATH, 'w') as f:
        json.dump({company: fundamentals}, f, indent=2)

    return jsonify(fundamentals)

# Scheduled scraping
scheduler = BackgroundScheduler()
scheduler.add_job(scrape_mse, 'interval', hours=1)
scheduler.start()
atexit.register(lambda: scheduler.shutdown(wait=False))

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)
    
