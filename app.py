#FlaskApp
import os
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify
import sqlite3
from apscheduler.schedulers.background import BackgroundScheduler
import atexit


app = Flask(__name__)

#1 Initialize Database

def init_db():
    conn = sqlite3.connect('database.db')
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
    conn.commit()
    conn.close()



#2 Scrape Data from MSE

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
            if len(cols) >= 3:
                data.append({'Counter': cols[0].text.strip(),
                'Last Price (MK)': cols[1].text.strip(),
                '% Change': cols[2].text.strip(),
                'Volume': cols[3].text.strip(),
                'Turnover (MK)': cols[4].text.strip()})

        return data
    except Exception as e:
        print("Scrapping Error:", e)
        return []


#3 Save to SqLite

def save_data(stock_data):
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    for item in stock_data:
        # Check for recent duplicate (same counter and price in the last hour)
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
    conn.close()



#4 API Routes

@app.route('/')
def home():
    return "StockMate API is running!"

@app.route('/scrape', methods=['GET'])
def scrape_and_save():
    data = scrape_mse()
    if data:
        save_data(data)
        return jsonify({"message": "Data scraped and saved", "count": len(data)})
    else:
        return jsonify({"error": "Failed to scrape data"}), 500

@app.route('/stocks', methods=['GET'])
def get_stocks():
    conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('SELECT counter, last_price, change, volume, turnover, timestamp FROM stocks ORDER BY timestamp DESC LIMIT 20')
    rows = cursor.fetchall()
    conn.close()
    return jsonify([{"counter": r[0], "last_price": r[1], "change": r[2], "volume": r[3], "turnover": r[4], "timestamp": r[5]} for r in rows])

@app.route('/latest_prices', methods=['GET'])
def latest_prices():
    conn = sqlite3.connect('database.db')

    cursor = conn.cursor()
    cursor.execute('''
        SELECT counter, last_price, change, volume, turnover, MAX(timestamp)
        FROM stocks
        GROUP BY counter
    ''')
    rows = cursor.fetchall()
    conn.close()
    return jsonify([
        {
            "counter": r[0],
            "last_price": r[1],
            "change": r[2],
            "volume": r[3],
            "turnover": r[4],
            "timestamp": r[5]
        } for r in rows
    ])
@app.route('/price_history/<counter>', methods=['GET'])
def price_history(counter):
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT timestamp, last_price
        FROM stocks
        WHERE counter = ?
        ORDER BY timestamp DESC
        LIMIT 10
    ''', (counter,))
    rows = cursor.fetchall()
    conn.close()

    return jsonify([
        {"timestamp": row[0], "price": row[1]} for row in reversed(rows)
    ])

@app.route('/fundamentals/<counter>', methods=['GET'])
def get_fundamentals(counter):
    import json
    try:
        with open('fundamentals.json') as f:
            data = json.load(f)

        company = data.get(counter.upper())
        if not company:
            return jsonify({"error": "Data not available for this company"}), 404

        eps = company['net_profit'] / company['shares_outstanding']
        bvps = company['book_value'] / company['shares_outstanding']

        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()
        cursor.execute('SELECT last_price FROM stocks WHERE counter = ? ORDER BY timestamp DESC LIMIT 1', (counter,))
        result = cursor.fetchone()
        conn.close()

        if result:
            price = float(str(result[0]).replace(',', ''))
        else:
            return jsonify({"error": "Price data not available"}), 404

        pe_ratio = price / eps if eps else None
        pb_ratio = price / bvps if bvps else None
        div_yield = (company['dividend'] / price) * 100 if price else None
        roe = (company['net_profit'] / company['equity']) * 100 if company['equity'] else None

        return jsonify({
            "eps": f"{eps:.2f}",
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
        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT DATE(timestamp), last_price
            FROM stocks
            WHERE counter = ?
            ORDER BY timestamp ASC
        ''', (counter,))
        rows = cursor.fetchall()
        conn.close()

        # Clean and format results
        history = []
        for row in rows:
            date_str = row[0]
            try:
                price = float(str(row[1]).replace(',', ''))
                history.append({"date": date_str, "price": price})
            except:
                continue

        return jsonify(history)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/download_reports/<company>', methods=['GET'])
def download_company_reports(company):
    import os, requests
    from bs4 import BeautifulSoup

    company = company.upper()
    base_url = 'https://www.mse.co.mw/announcements/accounts/'
    r = requests.get(base_url)
    soup = BeautifulSoup(r.content, 'html.parser')

    folder = f'reports/{company}'
    os.makedirs(folder, exist_ok=True)
    downloaded = []

    # Find links with PDF filenames
    for a in soup.select('a[href$=".pdf"]'):
        text = a.get_text().upper()
        href = a['href']
        if company in text:
            url = href if href.startswith('http') else f'https://www.mse.co.mw{href}'
            fname = url.split('/')[-1]
            path = os.path.join(folder, fname)
            try:
                pdf = requests.get(url)
                pdf.raise_for_status()
                with open(path, 'wb') as f:
                    f.write(pdf.content)
                downloaded.append(fname)
            except Exception as e:
                print(f"Failed downloading {url}: {e}")

    if not downloaded:
        return jsonify({"message": f"No reports found for {company}."}), 404

    return jsonify({"company": company, "downloaded": downloaded})

# Auto-scraping every hour
def scheduled_scrape():
    print("Scheduled scrape running...")
    data = scrape_mse()
    if data:
        save_data(data)

#Run the App ▶️
if __name__ == '__main__':
    init_db()

    #Start the scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_scrape, trigger='interval', hours=1)
    scheduler.start()

    # Ensure scheduler shuts down when Flask stops
    atexit.register(lambda: scheduler.shutdown(wait=False))

    app.run(host='0.0.0.0', port=5000, debug=True)
