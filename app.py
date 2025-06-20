#StockMate Flask App 

from flask import Flask, jsonify, request, send_file
import os, json, sqlite3, requests, fitz, re
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from bs4 import BeautifulSoup
from fpdf import FPDF

app = Flask(__name__)

# ========== DATABASE INIT ==========
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
    conn = sqlite3.connect('database.db')
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
    conn.close()

# ========== API ROUTES ==========
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
    return jsonify([{"counter": r[0], "last_price": r[1], "change": r[2], "volume": r[3], "turnover": r[4], "timestamp": r[5]} for r in rows])

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

@app.route('/fundamentals/<counter>', methods=['GET'])
def get_fundamentals(counter):
    try:
        with open('fundamentals.json') as f:
            data = json.load(f)

        company = data.get(counter.upper())
        if not company:
            return jsonify({"error": "Data not available for this company"}), 404

        # Parse and clean numerical values
        try:
            net_profit = float(str(company['net_profit']).replace(',', ''))
            equity = float(str(company['equity']).replace(',', ''))
            shares = float(str(company['shares_outstanding']).replace(',', ''))
            dividend = float(str(company['dividend_paid']).replace(',', ''))
        except Exception as e:
            return jsonify({"error": f"Parsing error: {str(e)}"}), 500

        eps = net_profit / shares if shares else 0
        bvps = equity / shares if shares else 0

        # Fetch latest price
        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT last_price FROM stocks
            WHERE counter = ?
            ORDER BY timestamp DESC
            LIMIT 1
        ''', (counter,))
        result = cursor.fetchone()
        conn.close()

        if result:
            price = float(str(result[0]).replace(',', ''))
        else:
            return jsonify({"error": "Price data not available"}), 404

        pe_ratio = price / eps if eps else None
        pb_ratio = price / bvps if bvps else None
        div_yield = (dividend / price) * 100 if price else None
        roe = (net_profit / equity) * 100 if equity else None

        return jsonify({
            "eps": f"{eps:.2f}",
            "pe_ratio": f"{pe_ratio:.2f}" if pe_ratio else "N/A",
            "pb_ratio": f"{pb_ratio:.2f}" if pb_ratio else "N/A",
            "div_yield": f"{div_yield:.2f}%" if div_yield else "N/A",
            "roe": f"{roe:.2f}%" if roe else "N/A"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/metrics/<counter>', methods=['GET'])
def stock_metrics(counter):
    try:
        with open('fundamentals.json') as f:
            data = json.load(f)

        company = data.get(counter.upper())
        if not company:
            return jsonify({"error": "Fundamentals not found"}), 404

        # Parse and clean numbers
        try:
            net_profit = float(str(company['net_profit']).replace(',', ''))
            equity = float(str(company['equity']).replace(',', ''))
            shares = float(str(company['shares_outstanding']).replace(',', ''))
            dividend = float(str(company['dividend_paid']).replace(',', ''))
        except Exception as e:
            return jsonify({"error": f"Parsing error: {str(e)}"}), 500

        eps = net_profit / shares if shares else 0
        bvps = equity / shares if shares else 0

        # Fetch latest stock data
        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT last_price, change, volume, turnover, timestamp
            FROM stocks
            WHERE counter = ?
            ORDER BY timestamp DESC
            LIMIT 1
        ''', (counter,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return jsonify({"error": "Price data not found"}), 404

        price_str = str(row[0]).replace(',', '')
        price = float(price_str) if price_str else 0

        pe = price / eps if eps else None
        pb = price / bvps if bvps else None
        div_yield = (dividend / price) * 100 if price else None
        roe = (net_profit / equity) * 100 if equity else None

        return jsonify({
            "counter": counter.upper(),
            "last_price": f"{price:.2f}",
            "change": row[1],
            "volume": row[2],
            "turnover": row[3],
            "timestamp": row[4],
            "eps": f"{eps:.2f}",
            "pe_ratio": f"{pe:.2f}" if pe else "N/A",
            "pb_ratio": f"{pb:.2f}" if pb else "N/A",
            "div_yield": f"{div_yield:.2f}%" if div_yield else "N/A",
            "roe": f"{roe:.2f}%" if roe else "N/A"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# PDF class: Header and Footer
class PDF(FPDF):
    def header(self):
        # Logo
        self.image("StockMate-logo.png", 10, 8, 30)  # (x, y, width)
        self.set_xy(50, 10)
        self.set_fill_color(0, 102, 204)  # Blue header background
        self.set_text_color(255, 255, 255)
        self.set_font('Arial', 'B', 16)
        self.cell(140, 10, 'StockMate Fundamentals Report', ln=True, align='C', fill=True)
        self.ln(15)
    
    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.set_text_color(120)
        self.cell(0, 10, 'Generated by StockMate.\nCall/WhatsApp: +265888695513 • Email: juanphiri7@gmail.com', 0, 0, 'C')

# PDF class with header and footer
class PDF(FPDF):
    def header(self):
        self.image("StockMate-logo.png", 10, 8, 30)
        self.set_xy(50, 10)
        self.set_fill_color(0, 102, 204)
        self.set_text_color(255, 255, 255)
        self.set_font('Helvetica', 'B', 16)
        self.cell(140, 10, 'StockMate Fundamentals Report', ln=True, align='C', fill=True)
        self.ln(15)

    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(120)
        self.cell(0, 10, 'Generated by StockMate • Call/WhatsApp: +265888695513 • Email: juanphiri7@gmail.com', 0, 0, 'C')

@app.route('/fundamentals_report/<counter>', methods=['GET'])
def fundamentals_report(counter):
    try:
        with open('fundamentals.json') as f:
            data = json.load(f)

        company = data.get(counter.upper())
        if not company:
            return jsonify({"error": "Data not available for this company"}), 404

        net_profit = float(str(company['net_profit']).replace(',', ''))
        equity = float(str(company['equity']).replace(',', ''))
        shares = float(str(company['shares_outstanding']).replace(',', ''))
        dividend = float(str(company['dividend_paid']).replace(',', ''))

        eps = net_profit / shares if shares else 0
        bvps = equity / shares if shares else 0

        # Get latest price
        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT last_price FROM stocks
            WHERE counter = ?
            ORDER BY timestamp DESC LIMIT 1
        ''', (counter,))
        result = cursor.fetchone()
        conn.close()

        if result:
            price = float(str(result[0]).replace(',', ''))
        else:
            return jsonify({"error": "Price data not available"}), 404

        pe_ratio = price / eps if eps else None
        pb_ratio = price / bvps if bvps else None
        div_yield = (dividend / price) * 100 if price else None
        roe = (net_profit / equity) * 100 if equity else None

        # === Create PDF ===
        pdf = PDF()
        pdf.add_page()
        pdf.ln(10)

        pdf.set_text_color(0)
        pdf.set_font("Helvetica", 'B', 14)
        pdf.cell(0, 10, f"{counter.upper()} Snapshot", ln=True)

        pdf.set_font("Helvetica", '', 12)
        pdf.cell(0, 10, f"Net Profit: MK {net_profit:,.2f}", ln=True)
        pdf.cell(0, 10, f"Equity: MK {equity:,.2f}", ln=True)
        pdf.cell(0, 10, f"Shares Outstanding: {shares:,.0f}", ln=True)
        pdf.cell(0, 10, f"Dividend Paid: MK {dividend:,.2f}", ln=True)
        pdf.cell(0, 10, f"Latest Price: MK {price:,.2f}", ln=True)

        pdf.ln(5)
        pdf.set_font("Helvetica", 'B', 14)
        pdf.cell(0, 10, "Key Financial Ratios", ln=True)

        pdf.set_font("Helvetica", '', 12)
        pdf.cell(0, 10, f"• Earnings Per Share (EPS): {eps:.2f}", ln=True)
        pdf.cell(0, 10, f"• P/E Ratio: {pe_ratio:.2f}" if pe_ratio else "• P/E Ratio: N/A", ln=True)
        pdf.cell(0, 10, f"• P/B Ratio: {pb_ratio:.2f}" if pb_ratio else "• P/B Ratio: N/A", ln=True)
        pdf.cell(0, 10, f"• Dividend Yield: {div_yield:.2f}%" if div_yield else "• Dividend Yield: N/A", ln=True)
        pdf.cell(0, 10, f"• Return on Equity (ROE): {roe:.2f}%" if roe else "• ROE: N/A", ln=True)

        pdf.ln(10)
        pdf.set_font("Helvetica", 'I', 10)
        pdf.set_text_color(90)
        pdf.multi_cell(0, 10, "This report was generated based on public financial data collected from the Malawi Stock Exchange.\nAccuracy is not guaranteed. Invest wisely.")

        file_path = f"{counter.upper()}-Fundamentals.pdf"
        pdf.output(file_path)

        return send_file(file_path, as_attachment=True)

    except Exception as e:
        return jsonify({"error": str(e)}), 500            
        
# ========== FINANCIAL REPORTS PDF DOWNLOAD ==========
@app.route('/download_sample_reports/<company>', methods=['GET'])
def download_sample_reports(company):
    company = company.upper()
    pdf_links = {
        "NICO": "https://mse.co.mw/wp-content/uploads/2024/04/NICO-Annual-Report-2023.pdf",
        "FMBCH": "https://mse.co.mw/wp-content/uploads/2024/04/FMBCH-FY23.pdf",
        "STAND": "https://mse.co.mw/wp-content/uploads/2024/04/STAND-FY23-Financials.pdf"
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
    equity = re.search(r'Total\s+Equity\s*[:\-]?\s*[MK]*\s?([\d,]+\.\d+)', full_text, re.IGNORECASE)
    shares = re.search(r'Shares\s+.*?Outstanding\s*[:\-]?\s*([\d,]+)', full_text, re.IGNORECASE)
    dividend = re.search(r'Dividend\s+(?:Paid|Declared)?\s*[:\-]?\s*[MK]*\s?([\d,]+\.\d+)', full_text, re.IGNORECASE)

    data = {
        "company": company,
        "net_profit": profit.group(1) if profit else "Not found",
        "equity": equity.group(1) if equity else "Not found",
        "shares_outstanding": shares.group(1) if shares else "Not found",
        "dividend_paid": dividend.group(1) if dividend else "Not found"
    }

    os.makedirs("data", exist_ok=True)
    with open("data/fundamentals.json", "w") as f:
        json.dump({company: data}, f, indent=2)

    return jsonify(data)

# ========== DEBUG TEXT ROUTE ==========
@app.route('/debug_pdf_text/<company>', methods=['GET'])
def debug_pdf_text(company):
    company = company.upper()
    folder = f'reports/{company}'

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

# ========== SCHEDULER ==========
def scheduled_scrape():
    print("Scheduled scrape running...")
    data = scrape_mse()
    if data:
        save_data(data)

# ========== INIT ==========
if __name__ == '__main__':
    init_db()
    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_scrape, trigger='interval', minutes=10)
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))
    app.run(host='0.0.0.0', port=5000, debug=True)
  
