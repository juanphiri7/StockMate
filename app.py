#StockMate by Juan

# ========== Imports ==========
import os, json, sqlite3, requests, pytz, fitz, re, atexit, qrcode
from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup
from fpdf import FPDF
from datetime import datetime
from PIL import Image
from flask import Flask, request, jsonify, render_template_string, redirect, url_for, session, send_file

# ========== FLASK APP ==========
app = Flask(__name__)
app.secret_key = "your-super-secret-key"

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
    conn = sqlite3.connect('database.db') # Use your PostgreSQL engine later
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
    
    # Fundamentals table
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
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute('SELECT counter, last_price, change, volume, turnover, timestamp FROM stocks ORDER BY timestamp DESC LIMIT 20')
    rows = cursor.fetchall()
    conn.close()
    return jsonify([{"counter": r[0], "last_price": r[1], "change": r[2], "volume": r[3], "turnover": r[4], "timestamp": convert_to_local_time(r[5])} for r in rows])

# ======= Latest Prices Route
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
    
    return jsonify([{"counter": r[0], "last_price": r[1], "change": r[2], "volume": r[3], "turnover": r[4], "timestamp": convert_to_local_time(r[5])} for r in rows])

# ======= Prices History Route
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
        {"timestamp": convert_to_local_time(row[0]), "price": row[1]} for row in reversed(rows)
    ])

# ======= History Route
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

# ==== Fundamentals Data
def insert_fundamentals():
    fundamentals_data = [
        {
            "counter": "AIRTEL",
            "net_profit": "42,714,422,219.62",
            "number_of_shares_in_issue": "11,000,000,000",
            "dividend_paid": "21,875,568,000.00",
            "book_value": "32,120,000,000.00"
        },
        {
            "counter": "BHL",
            "net_profit": "-1,369,168,339.45",
            "number_of_shares_in_issue": "5,878,254,935",
            "dividend_paid": "0.00",
            "book_value": "65,131,064,679.80"
        },
        {
            "counter": "FDHB",
            "net_profit": "74,055,922,113.91",
            "number_of_shares_in_issue": "6,901,031,250",
            "dividend_paid": "32,720,549,568.75",
            "book_value": "97,373,550,937.50"
        },
        {
            "counter": "FMBCH",
            "net_profit": "118,254,740,000.00",
            "number_of_shares_in_issue": "2,458,250,000",
            "dividend_paid": "8,850,053,988.00",
            "book_value": "329,085,927,500.00"
        },
        {
            "counter": "ICON",           
            "net_profit": "24,490,000.00",
            "number_of_shares_in_issue": "6,680,000,000",
            "dividend_paid": "1,942,477,200.00",
            "book_value": "146,225,200,000.00"
        },
        {
            "counter": "ILLOVO",
            "net_profit": "22,631,873,664.47",
            "number_of_shares_in_issue": "713,444,391",
            "dividend_paid": "3,578,500,083.93",
            "book_value": "148,781,693,299.14"
        },
        {
            "counter": "MPICO",
            "net_profit": "8,535,675,173.68",
            "number_of_shares_in_issue": "2,298,047,460",
            "dividend_paid": "987,300,938.05",
            "book_value": "65,195,606,440.20"
        },
        {
            "counter": "NBM",
            "net_profit": "102,283,000,000.00",
            "number_of_shares_in_issue": "466,931,738",
            "dividend_paid": "59,060,764,860.77",
            "book_value": "268,560,458,428.08"
        },
        {
            "counter": "NBS",
            "net_profit": "72,978,138,905.20",
            "number_of_shares_in_issue": "2,910,573,356",
            "dividend_paid": "63,647,021,073.80",
            "book_value": "112,057,074,206.00"
        },
        {
            "counter": "NICO",
            "net_profit": "72,006,217,688.65",
            "number_of_shares_in_issue": "1,043,041,096",
            "dividend_paid": "22,981,533,076.39",
            "book_value": "155,726,035,632.8"
        },
        {
            "counter": "NITL",
            "net_profit": "29,759,480,000.00",
            "number_of_shares_in_issue": "135,000,000",
            "dividend_paid": "1,715,933,700.00",
            "book_value": "73,803,150,000.00"
        },
        {
            "counter": "OMU",
            "net_profit": "2,595,650,000.00",
            "number_of_shares_in_issue": "16,977,551",
            "dividend_paid": "1,404,909,203.96",
            "book_value": "19,469,855,486.80"
        },
        {
            "counter": "PCL",
            "net_profit": "64,673,000,000.00",
            "number_of_shares_in_issue": "120,255,820",
            "dividend_paid": "1,346,858,449.67",
            "book_value": "348,566,304,502.80"
        },
        {
            "counter": "STANDARD",
            "net_profit": "86,365,000,000.00",
            "number_of_shares_in_issue": "234,668,162",
            "dividend_paid": "43,881,111,188.97",
            "book_value": "259,843,362,419.36"
        },
        {
            "counter": "SUNBIRD",
            "net_profit": "10,624,630,000.00",
            "number_of_shares_in_issue": "261,582,580",
            "dividend_paid": "3,396,746,848.44",
            "book_value": "69,889,633,724.40"
        },
        {
            "counter": "TNM",
            "net_profit": "10,060,000,000.00",
            "number_of_shares_in_issue": "11,541,200,375",
            "dividend_paid": "0.00",
            "book_value": "51,819,989,685.90"
        }
    ]

    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    for entry in fundamentals_data:
        cursor.execute('''
            INSERT INTO fundamentals (counter, net_profit, number_of_shares_in_issue, dividend_paid, book_value)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            entry["counter"],
            entry["net_profit"],
            entry["number_of_shares_in_issue"],
            entry["dividend_paid"],
            entry["book_value"]
        ))

    conn.commit()
    conn.close()

# ======= Insert Fundamentals 
@app.route('/insert_fundamentals', methods = ['POST'])
def insert_fundamentals():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()

    for counter, values in data_dict.items():
        c.execute('''
            INSERT INTO fundamentals (counter, net_profit, number_of_shares_in_issue, dividend_paid, book_value)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            counter,
            values['net_profit'],
            values['number_of_shares_in_issue'],
            values['dividend_paid'],
            values['book_value']
        ))
    
    conn.commit()
    conn.close()

# ======= Fundamentals Route
@app.route('/fundamentals/<counter>', methods=['GET'])
def get_fundamentals(counter):
    counter = counter.upper()
    try:
        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT net_profit, number_of_shares_in_issue, dividend_paid, book_value
            FROM fundamentals WHERE counter = ?
        ''', (counter,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return jsonify({"error": "Data not available for this company"}), 404

        net_profit, number_of_shares_in_issue, dividend_paid, book_value = row
        
        # Parse and clean numerical values
        try:
            net_profit = float(str(row['net_profit']).replace(',', ''))
            shares = float(str(row['number_of_shares_in_issue']).replace(',', ''))
            dividend = float(str(row['dividend_paid']).replace(',', ''))
            book_value = float(str(row['book_value']).replace(',', ''))
        except Exception as e:
            return jsonify({"error": f"Parsing error: {str(e)}"}), 500

        eps = net_profit / shares if shares and net_profit else 0
        bvps = book_value / shares if shares and book_value else 0
        dvps = dividend / shares if shares and dividend else 0

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
        div_yield = (dvps / price) * 100 if price else None
        
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
        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT net_profit, number_of_shares_in_issue, dividend_paid, book_value
            FROM fundamentals WHERE counter = ?
        ''', (counter,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return jsonify({"error": "Data not available for this company"}), 404

        net_profit, number_of_shares_in_issue, dividend_paid, book_value = row
        
        # Parse and clean numbers
        try:
            net_profit = float(str(row['net_profit']).replace(',', ''))
            shares = float(str(row['number_of_shares_in_issue']).replace(',', ''))
            dividend = float(str(row['dividend_paid']).replace(',', ''))
            book_value = float(str(row['book_value']).replace(',', ''))
        except Exception as e:
            return jsonify({"error": f"Parsing error: {str(e)}"}), 500

        # Calculate Metrics 
        eps = net_profit / shares if shares and net_profit else 0
        bvps = book_value / shares if shares and book_value else 0
        dvps = dividend / shares if shares and dividend else 0
        
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
        result = cursor.fetchone()
        conn.close()

        if not result:
            return jsonify({"error": "Latest Price data not found"}), 404

        price_str = str(result[0]).replace(',', '')
        price = float(price_str) if price_str else 0
        
        pe_ratio = price / eps if eps else None
        pb_ratio = price / bvps if bvps else None
        div_yield = (dvps / price) * 100 if price else None

        return jsonify({
            "counter": counter.upper(),
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

# 📄 PDF class: Header and Footer using DejaVu font
class PDF(FPDF):
    def header(self):
        self.image("StockMate-logo.png", 10, 8, 30)  # (x, y, width)
        self.set_xy(50, 10)
        self.set_fill_color(0, 102, 204)  # Blue
        self.set_text_color(255, 255, 255)
        self.set_font("DejaVu", "B", 20)
        self.cell(140, 10, "StockMate Fundamentals Report", ln=True, align='C', fill=True)
        self.ln(15)
        # Motto
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

# ======= Fundamentals Report Route
@app.route('/fundamentals_report/<counter>', methods=['GET'])
def fundamentals_report(counter):
    counter = counter.upper()
    try:
        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT net_profit, number_of_shares_in_issue, dividend_paid, book_value
            FROM fundamentals WHERE counter = ?
        ''', (counter,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return jsonify({"error": "Data not available for this company"}), 404

        net_profit, number_of_shares_in_issue, dividend_paid, book_value = row
  
        # Parse numeric data
        net_profit = float(str(row['net_profit']).replace(',', ''))
        shares = float(str(row['number_of_shares_in_issue']).replace(',', ''))
        dividend = float(str(row['dividend_paid']).replace(',', ''))
        book_value = float(str(row.get('book_value', 0)).replace(',', ''))

        #Calculate Metrics
        eps = net_profit / shares if shares and net_profit else 0
        bvps = book_value / shares if shares and book_value else 0
        dvps = dividend / shares if shares and dividend else 0

        # Get latest stock price
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
        div_yield = (dvps / price) * 100 if price else None

        # === Create PDF ===
        pdf = PDF()
        
        pdf.add_font("DejaVu", "", "fonts/DejaVuSans.ttf", uni=True)
        pdf.add_font("DejaVu", "B", "fonts/DejaVuSans-Bold.ttf", uni=True)
        pdf.add_font("DejaVu", "I", "fonts/DejaVuSans-Oblique.ttf", uni=True)
        pdf.add_font("DejaVu", "BI", "fonts/DejaVuSans-BoldOblique.ttf", uni=True)
        
        pdf.add_page()
        pdf.ln(10)
        # ========== Company Logo + Name ==========
        logo_path = f"company_logos/{counter.upper()}.png"
        logo_width = 25
        
        if os.path.exists(logo_path):
            y_start = pdf.get_y()
            pdf.image(logo_path, x=10, y=y_start, w=logo_width)
            pdf.set_xy(10 + logo_width + 10, y_start + 5) 
            pdf.set_font("DejaVu", "B", 16)
            pdf.cell(0, 10, f"{counter} Snapshot", ln=True)
            # Push cursor down so logo and text above don't overlap with content
            pdf.set_y(y_start + logo_width + 5)
        else:
            pdf.set_font("DejaVu", "B", 16)
            pdf.set_text_color(0)
            pdf.cell(0, 10, f"{counter.upper()} Snapshot", ln=True)
            pdf.ln(10)

        # ==== Financial Info ====
        pdf.set_text_color(0)
        pdf.set_font("DejaVu", "", 12)
        pdf.cell(0, 10, f"Latest Price: MK {price:,.2f}" if price else "N/A", ln=True)
        pdf.cell(0, 10, f"Net Profit: MK {net_profit:,.2f}" if net_profit else "N/A", ln=True)
        pdf.cell(0, 10, f"Dividend Paid: MK {dividend:,.2f}" if dividend else "N/A", ln=True)
        pdf.cell(0, 10, f"Number of Shares in Issue: {shares:,.0f}" if shares else "N/A", ln=True)
        pdf.cell(0, 10, f"Book Value: MK {book_value:,.2f}" if book_value else "N/A", ln=True)
        # Metrics 
        pdf.ln(5)
        pdf.set_font("DejaVu", "B", 16)
        pdf.cell(0, 10, "Key Financial Metrics", ln=True)
        pdf.set_font("DejaVu", "", 12)
        pdf.cell(0, 10, f"Earnings Per Share (EPS): {eps:.2f}" if eps else "N/A", ln=True)
        pdf.cell(0, 10, f"P/E Ratio: {pe_ratio:.2f}" if pe_ratio else "N/A", ln=True)
        pdf.cell(0, 10, f"Dividend Yield: {div_yield:.2f}%" if div_yield else "N/A", ln=True)
        pdf.cell(0, 10, f"P/B Ratio: {pb_ratio:.2f}" if pb_ratio else "N/A", ln=True)
        pdf.cell(0, 10, f"Book Value Per Share (BVPS): {bvps:.2f}" if bvps else "N/A", ln=True)
        #Disclaimer
        pdf.ln(12)
        pdf.set_font("DejaVu", "I", 10)
        pdf.set_text_color(90)
        pdf.multi_cell(0, 10, f"Disclaimer: This report is auto-generated based on public financial data from the Malawi Stock Exchange.\nAccuracy is NOT guaranteed. Scan the QR Code to verify {counter.upper()} official data. Invest wisely.")
        pdf.ln(7)
        pdf.set_font("DejaVu", "B", 10)
        pdf.set_text_color(0)
        pdf.cell(0, 10, f"For more information about {counter.upper()}, Scan the QR Code.")
        
        filename = f"{counter.upper()}-Fundamentals-Report.pdf"
       
        # === Generate QR Code for MSE companies ===
        company_urls = {
            "AIRTEL": "https://mse.co.mw/company/MWAIRT001156",
            "BHL": "https://mse.co.mw/company/MWBHL001164",
            "FDH": "https://mse.co.mw/company/MWFDHB001178",
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
            
        qr_url = company_urls.get(counter.upper(), f"https://mse.co.mw")
        qr_img = qrcode.make(qr_url)
        qr_path = f"{counter}_qr.png"
        qr_img.save(qr_path)

        # Insert QR into PDF
        pdf.image(qr_path, x=160, y=240, w=40, h=40)

        os.remove(qr_path)
        pdf.output(filename)

        return send_file(filename, as_attachment=True)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ========== FINANCIAL REPORTS PDF DOWNLOAD ==========
@app.route('/download_sample_reports/<company>', methods=['GET'])
def download_sample_reports(company):
    company = company.upper()
    pdf_links = {
        "AIRTEL": "https://mse.co.mw/company/MWAIRT001156",
        "BHL": "https://mse.co.mw/company/MWBHL001164",
        "FDH": "https://mse.co.mw/company/MWFDHB001178",
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

    # Insert into database
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO fundamentals
        (counter, net_profit, number_of_shares_in_issue, dividend_paid, book_value)
        VALUES (?, ?, ?, ?, ?)
    ''', (company, net_profit, shares_out, dividend_paid, book_val))
    conn.commit()
    conn.close()

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


@app.route('/admin', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        if request.form['password'] == "StockMateAdmin@47":
            session['logged_in'] = True
            return redirect(url_for('admin_dashboard'))
        return r"Oops!! That Key Doesn't Fit the Lock!", 403

    return render_template_string("""
        <h2>StockMate Admin Login</h2>
        <form method="POST">
            <input type="password" name="password" placeholder="Insert Your Key Here"/>
            <button type="submit">Unlock the Vault</button>
        </form>
    """)

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
def scheduled_scrape():
    print("Scheduled scrape running...")
    data = scrape_mse()
    if data:
        save_data(data)

# ========== INIT ==========
if __name__ == '__main__':
    init_db()
    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_scrape, trigger='interval', minutes=5)
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))
    app.run(host='0.0.0.0', port=5000, debug=True)
  
