from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
from datetime import datetime, timedelta
import sys
import io
import os
import psycopg2
from psycopg2 import sql
import pytz
import logging
from typing import List, Dict, Any
import schedule
import threading
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Fix Unicode encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Configuration
PSX_MARKET_OPEN = "09:30"  # PKT (Asia/Karachi)
PSX_MARKET_CLOSE = "15:30"  # PKT (Asia/Karachi)
SCRAPE_INTERVAL_MINUTES = 5  # Run every 5 minutes during market hours
PAKISTAN_TIMEZONE = pytz.timezone('Asia/Karachi')

# Supabase PostgreSQL configuration
SUPABASE_DB_CONFIG = {
    'dbname': os.getenv('SUPABASE_DB_NAME', 'postgres'),
    'user': os.getenv('SUPABASE_DB_USER', 'postgres'),
    'password': os.getenv('SUPABASE_DB_PASSWORD', ''),
    'host': os.getenv('SUPABASE_DB_HOST', 'db.supabase.co'),
    'port': os.getenv('SUPABASE_DB_PORT', '5432'),
    'sslmode': os.getenv('SUPABASE_DB_SSLMODE', 'require')
}

# Get connection string (alternative method)
DATABASE_URL = os.getenv('SUPABASE_DB_URL')

def setup_database():
    """Create database table if it doesn't exist"""
    conn = None
    try:
        if DATABASE_URL:
            conn = psycopg2.connect(DATABASE_URL)
        else:
            conn = psycopg2.connect(**SUPABASE_DB_CONFIG)
        
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'psx_stock_data'
            );
        """)
        
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            logger.info("Creating psx_stock_data table...")
            
            create_table_query = """
            CREATE TABLE psx_stock_data (
                id SERIAL PRIMARY KEY,
                scrape_timestamp TIMESTAMPTZ NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                sector VARCHAR(100),
                listed_in VARCHAR(50),
                ldcp DECIMAL(12, 2),
                open_price DECIMAL(12, 2),
                high_price DECIMAL(12, 2),
                low_price DECIMAL(12, 2),
                current_price DECIMAL(12, 2),
                price_change DECIMAL(12, 2),
                change_percent DECIMAL(8, 2),
                volume BIGINT,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                
                -- Indexes for better query performance
                CONSTRAINT unique_scrape_symbol UNIQUE(scrape_timestamp, symbol)
            );
            
            -- Create indexes for common queries
            CREATE INDEX idx_scrape_timestamp ON psx_stock_data(scrape_timestamp);
            CREATE INDEX idx_symbol ON psx_stock_data(symbol);
            CREATE INDEX idx_sector ON psx_stock_data(sector);
            CREATE INDEX idx_scrape_date ON psx_stock_data(DATE(scrape_timestamp));
            """
            
            cur.execute(create_table_query)
            conn.commit()
            logger.info("Table created successfully")
        else:
            logger.info("Table already exists")
            
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Database setup error: {e}")
        if conn:
            conn.rollback()
        raise

def get_database_connection():
    """Get database connection with retry logic"""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            if DATABASE_URL:
                conn = psycopg2.connect(DATABASE_URL)
            else:
                conn = psycopg2.connect(**SUPABASE_DB_CONFIG)
            
            logger.debug("Database connection established")
            return conn
            
        except Exception as e:
            logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                raise

def setup_driver():
    """Chrome driver setup - Optimized for cloud deployment"""
    logger.info("Setting up Chrome driver for cloud deployment...")
    
    chrome_options = Options()
    
    # Essential options for cloud deployment
    chrome_options.add_argument('--headless=new')  # New headless mode
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    
    # Set window size
    chrome_options.add_argument('--window-size=1920,1080')
    
    # User agent
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Performance optimizations
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-images')
    chrome_options.add_argument('--disable-notifications')
    chrome_options.add_argument('--disable-popup-blocking')
    
    # Additional optimizations for faster loading
    chrome_options.add_argument('--blink-settings=imagesEnabled=false')
    chrome_options.add_argument('--disable-javascript')
    
    # Memory optimizations
    chrome_options.add_argument('--disable-background-timer-throttling')
    chrome_options.add_argument('--disable-backgrounding-occluded-windows')
    chrome_options.add_argument('--disable-renderer-backgrounding')
    
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    
    try:
        # Try with Service first
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logger.info("Chrome driver ready!")
        return driver
    except Exception as e:
        logger.warning(f"Standard driver setup failed: {e}, trying alternative...")
        try:
            # Fallback without Service
            driver = webdriver.Chrome(options=chrome_options)
            logger.info("Chrome driver ready (fallback)!")
            return driver
        except Exception as e2:
            logger.error(f"All driver setup attempts failed: {e2}")
            raise

def extract_correct_psx_data(driver):
    """FIXED: Correct PSX data extraction with ALL columns including SECTOR and LISTED IN"""
    try:
        url = "https://dps.psx.com.pk/market-watch"
        logger.info(f"Loading: {url}")
        
        driver.get(url)
        
        logger.info("Waiting for data to load...")
        # Give initial time for page to load
        time.sleep(3)
        
        wait = WebDriverWait(driver, 20)
        
        # Wait for table to be present
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        logger.info("Table found, extracting data...")
        
        # Additional wait for data to populate
        time.sleep(2)
        
        # Extract with COMPLETE column mapping
        extract_script = """
            let allStocks = [];
            
            let tables = document.querySelectorAll('table');
            let targetTable = null;
            
            // Find the main market watch table
            for (let table of tables) {
                let headers = table.querySelectorAll('th');
                if (headers.length >= 10) {  // PSX table has 11 columns
                    let headerText = Array.from(headers).map(h => h.innerText.trim()).join(' ');
                    if (headerText.includes('Symbol') || headerText.includes('Sector') || headerText.includes('LDCP')) {
                        targetTable = table;
                        break;
                    }
                }
            }
            
            if (!targetTable && tables.length > 0) {
                targetTable = tables[0];
            }
            
            if (!targetTable) return allStocks;
            
            let tbody = targetTable.querySelector('tbody');
            if (!tbody) return allStocks;
            
            let rows = tbody.querySelectorAll('tr');
            
            rows.forEach((row, rowIndex) => {
                let cells = row.querySelectorAll('td');
                
                // CORRECT PSX TABLE STRUCTURE - 11 COLUMNS:
                // 0: Symbol
                // 1: Sector
                // 2: Listed In
                // 3: LDCP (Last Day Close Price)
                // 4: Open
                // 5: High
                // 6: Low
                // 7: Current
                // 8: Change
                // 9: Change%
                // 10: Volume
                
                if (cells.length >= 9) {  // Some rows might have fewer cells
                    let symbol = cells[0]?.innerText.trim() || '';
                    
                    // Skip invalid rows
                    if (!symbol || symbol === '' || symbol.length > 20 || 
                        symbol.includes('Symbol') || symbol.includes('Last') ||
                        symbol === 'PSX' || symbol === 'KSE-100' ||
                        symbol.includes('Open') || symbol.includes('High') ||
                        symbol.includes('Low') || symbol.includes('Volume')) {
                        return;
                    }
                    
                    // COMPLETE COLUMN MAPPING - ALL 11 COLUMNS
                    let stockData = {
                        symbol: symbol,
                        sector: cells[1]?.innerText.trim() || '',           // Column 1 - SECTOR
                        listed_in: cells[2]?.innerText.trim() || '',        // Column 2 - LISTED IN
                        ldcp: cells[3]?.innerText.trim() || '0',           // Column 3 - LDCP
                        open: cells[4]?.innerText.trim() || '0',           // Column 4 - Open
                        high: cells[5]?.innerText.trim() || '0',           // Column 5 - High
                        low: cells[6]?.innerText.trim() || '0',            // Column 6 - Low
                        current: cells[7]?.innerText.trim() || '0',        // Column 7 - Current
                        change: cells[8]?.innerText.trim() || '0',         // Column 8 - Change
                        change_percent: cells[9]?.innerText.trim() || '0', // Column 9 - Change%
                        volume: cells[10]?.innerText.trim() || '0'         // Column 10 - Volume
                    };
                    
                    allStocks.push(stockData);
                }
            });
            
            return allStocks;
        """
        
        raw_data = driver.execute_script(extract_script)
        
        if not raw_data or len(raw_data) == 0:
            logger.warning("JavaScript extraction failed, trying robust manual extraction...")
            return extract_manual_complete(driver)
        
        logger.info(f"Extracted {len(raw_data)} stocks successfully with COMPLETE columns")
        return raw_data
        
    except Exception as e:
        logger.error(f"Error in main extraction: {e}")
        return extract_manual_complete(driver)

def extract_manual_complete(driver):
    """Robust manual extraction with COMPLETE column mapping"""
    logger.info("Using robust manual extraction with ALL columns...")
    
    try:
        stocks_data = []
        
        wait = WebDriverWait(driver, 25)
        
        # Find the main table
        table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        # Get all rows
        rows = table.find_elements(By.TAG_NAME, "tr")
        logger.info(f"Found {len(rows)} total rows")
        
        processed_count = 0
        for i, row in enumerate(rows):
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                
                # Skip rows with too few cells or header rows
                if len(cells) < 9:
                    continue
                
                symbol = cells[0].text.strip()
                
                # Skip invalid symbols
                if (not symbol or len(symbol) > 20 or 
                    any(keyword in symbol for keyword in ['Symbol', 'Last', 'Open', 'High', 'Low', 'Current', 'Change', 'Volume']) or
                    'PSX' in symbol or 'KSE' in symbol):
                    continue
                
                # COMPLETE COLUMN MAPPING FOR PSX - ALL 11 COLUMNS:
                stock = {
                    'symbol': symbol,
                    'sector': cells[1].text.strip() if len(cells) > 1 else '',           # SECTOR
                    'listed_in': cells[2].text.strip() if len(cells) > 2 else '',        # LISTED IN
                    'ldcp': cells[3].text.strip() if len(cells) > 3 else '0',           # LDCP
                    'open': cells[4].text.strip() if len(cells) > 4 else '0',           # Open
                    'high': cells[5].text.strip() if len(cells) > 5 else '0',           # High
                    'low': cells[6].text.strip() if len(cells) > 6 else '0',            # Low
                    'current': cells[7].text.strip() if len(cells) > 7 else '0',        # Current
                    'change': cells[8].text.strip() if len(cells) > 8 else '0',         # Change
                    'change_percent': cells[9].text.strip() if len(cells) > 9 else '0', # Change%
                    'volume': cells[10].text.strip() if len(cells) > 10 else '0'        # Volume
                }
                
                stocks_data.append(stock)
                processed_count += 1
                
                if processed_count % 50 == 0:
                    logger.info(f"   {processed_count} valid stocks processed...")
                
            except Exception as e:
                continue
        
        logger.info(f"Robust extraction: {len(stocks_data)} valid stocks with ALL columns")
        return stocks_data
        
    except Exception as e:
        logger.error(f"Robust extraction error: {e}")
        return []

def clean_numeric_value(value):
    """Clean and convert numeric values properly"""
    if not value or value in ['N/A', '-', '', ' ', 'NAN', 'NULL', 'N.S.', 'N/S', 'n/s']:
        return '0'
    
    try:
        # Remove commas, spaces, percentage signs, and other non-numeric characters except decimal point and minus
        cleaned = str(value).strip()
        cleaned = cleaned.replace(',', '').replace(' ', '').replace('%', '')
        cleaned = cleaned.replace('(', '').replace(')', '').replace('$', '')
        cleaned = cleaned.replace('Rs.', '').replace('PKR', '').replace('Rs', '')
        cleaned = cleaned.replace('--', '').replace('---', '').replace('—', '')
        
        # Handle negative values properly
        if cleaned.startswith('-'):
            cleaned = '-' + cleaned[1:].lstrip()
        
        # If it's empty after cleaning, return 0
        if not cleaned or cleaned == '-':
            return '0'
            
        # Convert to float and back to string to validate
        float_val = float(cleaned)
        return str(float_val)
        
    except:
        return '0'

def validate_stock_data(stock):
    """Validate if stock data looks reasonable"""
    try:
        # Check if symbol is reasonable
        if not stock['symbol'] or len(stock['symbol']) > 20:
            return False
        
        # Check if numeric values are reasonable
        current_price = float(clean_numeric_value(stock['current']))
        if current_price <= 0 or current_price > 100000:  # Assuming no stock price > 100,000
            return False
            
        return True
    except:
        return False

def save_to_supabase(stocks_data: List[Dict[str, Any]], scrape_timestamp: datetime):
    """Save data to Supabase PostgreSQL database"""
    if not stocks_data:
        logger.warning("No data to save")
        return False
    
    conn = None
    cur = None
    saved_count = 0
    
    try:
        # Get database connection
        conn = get_database_connection()
        cur = conn.cursor()
        
        # Prepare insert query with proper column names
        insert_query = """
        INSERT INTO psx_stock_data 
        (scrape_timestamp, symbol, sector, listed_in, ldcp, open_price, high_price, low_price, 
         current_price, price_change, change_percent, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (scrape_timestamp, symbol) DO NOTHING
        """
        
        # Prepare batch data
        batch_data = []
        
        for item in stocks_data:
            if validate_stock_data(item):
                try:
                    # Clean and convert all values
                    ldcp = float(clean_numeric_value(item['ldcp']))
                    open_price = float(clean_numeric_value(item['open']))
                    high_price = float(clean_numeric_value(item['high']))
                    low_price = float(clean_numeric_value(item['low']))
                    current_price = float(clean_numeric_value(item['current']))
                    price_change = float(clean_numeric_value(item['change']))
                    change_percent = float(clean_numeric_value(item['change_percent']))
                    
                    # Convert volume to integer
                    volume_cleaned = clean_numeric_value(item['volume'])
                    volume = int(float(volume_cleaned)) if volume_cleaned != '0' else 0
                    
                    row_data = (
                        scrape_timestamp,           # scrape_timestamp
                        item['symbol'],             # symbol
                        item['sector'],             # sector
                        item['listed_in'],          # listed_in
                        ldcp,                       # ldcp
                        open_price,                 # open_price
                        high_price,                 # high_price
                        low_price,                  # low_price
                        current_price,              # current_price
                        price_change,               # price_change
                        change_percent,             # change_percent
                        volume                      # volume
                    )
                    
                    batch_data.append(row_data)
                    
                except Exception as e:
                    logger.error(f"Error processing stock {item.get('symbol', 'unknown')}: {e}")
                    continue
        
        # Execute batch insert
        if batch_data:
            cur.executemany(insert_query, batch_data)
            conn.commit()
            saved_count = len(batch_data)
            
            logger.info(f"✓ Saved {saved_count} stocks to Supabase at {scrape_timestamp}")
            
            # Log summary statistics
            if saved_count > 0:
                summary_stats = {
                    'symbols': len(set(item[1] for item in batch_data)),
                    'min_price': min(item[8] for item in batch_data),
                    'max_price': max(item[8] for item in batch_data),
                    'total_volume': sum(item[11] for item in batch_data)
                }
                
                logger.info(f"  Summary: {summary_stats['symbols']} unique symbols, "
                          f"Price range: {summary_stats['min_price']:.2f}-{summary_stats['max_price']:.2f}, "
                          f"Total volume: {summary_stats['total_volume']:,}")
        
        return saved_count > 0
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        return False
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def is_market_open(current_time=None):
    """Check if current time is within PSX market hours (Mon-Fri, 9:30-15:30 PKT)"""
    try:
        if current_time is None:
            current_time = datetime.now(PAKISTAN_TIMEZONE)
        else:
            if current_time.tzinfo is None:
                current_time = PAKISTAN_TIMEZONE.localize(current_time)
        
        # Check if it's a weekday (Monday=0, Friday=4)
        if current_time.weekday() >= 5:  # Saturday or Sunday
            logger.info(f"Market closed: Weekend ({current_time.strftime('%A')})")
            return False
        
        # Check if it's a public holiday (you can add specific dates here)
        # Example: if current_time.date() in public_holidays: return False
        
        # Parse market hours
        current_time_only = current_time.time()
        market_open = datetime.strptime(PSX_MARKET_OPEN, "%H:%M").time()
        market_close = datetime.strptime(PSX_MARKET_CLOSE, "%H:%M").time()
        
        # Check if current time is within market hours
        is_open = market_open <= current_time_only <= market_close
        
        if not is_open:
            logger.info(f"Market closed: Outside trading hours "
                       f"({current_time_only.strftime('%H:%M')} not in {PSX_MARKET_OPEN}-{PSX_MARKET_CLOSE})")
        
        return is_open
        
    except Exception as e:
        logger.error(f"Error checking market hours: {e}")
        return False

def get_next_scrape_time():
    """Calculate next scrape time (rounded to next 5-minute interval)"""
    now_pk = datetime.now(PAKISTAN_TIMEZONE)
    
    # Round current minute to next 5-minute interval
    minutes_to_add = (SCRAPE_INTERVAL_MINUTES - (now_pk.minute % SCRAPE_INTERVAL_MINUTES)) % SCRAPE_INTERVAL_MINUTES
    if minutes_to_add == 0:
        minutes_to_add = SCRAPE_INTERVAL_MINUTES
    
    next_time = now_pk + timedelta(minutes=minutes_to_add)
    next_time = next_time.replace(second=0, microsecond=0)
    
    return next_time

def run_scraping_job():
    """Execute one scraping job with timing"""
    start_time = time.time()
    scrape_timestamp = datetime.now(PAKISTAN_TIMEZONE)
    
    logger.info("=" * 60)
    logger.info(f"Starting scraping job at: {scrape_timestamp}")
    logger.info(f"Market status: {'OPEN' if is_market_open(scrape_timestamp) else 'CLOSED'}")
    logger.info("=" * 60)
    
    if not is_market_open(scrape_timestamp):
        logger.info("Skipping scrape - Market is closed")
        return False
    
    driver = None
    success = False
    
    try:
        # Setup and run scraper
        driver = setup_driver()
        stocks_data = extract_correct_psx_data(driver)
        
        if stocks_data and len(stocks_data) > 0:
            logger.info(f"Successfully extracted {len(stocks_data)} stocks")
            
            # Save to Supabase
            success = save_to_supabase(stocks_data, scrape_timestamp)
            
            if success:
                execution_time = time.time() - start_time
                logger.info(f"✓ Scraping job completed successfully in {execution_time:.2f} seconds")
                
                # Display sample of scraped data
                logger.info("Sample of scraped data (first 5 stocks):")
                for i in range(min(5, len(stocks_data))):
                    stock = stocks_data[i]
                    logger.info(f"  {stock['symbol']}: {stock['current']} "
                              f"({stock['change_percent']}%) Vol: {stock['volume']}")
            else:
                logger.error("Failed to save data to Supabase")
        else:
            logger.warning("No data extracted from PSX")
            
    except Exception as e:
        logger.error(f"Scraping error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if driver:
            try:
                driver.quit()
                logger.debug("Browser closed")
            except:
                pass
    
    return success

def schedule_scraper():
    """Main scheduling function that runs continuously"""
    logger.info("=" * 80)
    logger.info("PSX CLOUD SCRAPER - SUPABASE VERSION")
    logger.info("=" * 80)
    logger.info(f"Market Hours: {PSX_MARKET_OPEN} to {PSX_MARKET_CLOSE} PKT (Asia/Karachi)")
    logger.info(f"Schedule: Every {SCRAPE_INTERVAL_MINUTES} minutes during market hours")
    logger.info(f"Days: Monday to Friday (excluding public holidays)")
    logger.info(f"Database: Supabase PostgreSQL")
    logger.info("=" * 80)
    
    # Setup database
    try:
        setup_database()
        logger.info("Database setup completed ✓")
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        logger.error("Continuing without database setup...")
    
    # Track consecutive failures
    consecutive_failures = 0
    max_consecutive_failures = 5
    
    while True:
        try:
            # Get current time in PKT
            now_pk = datetime.now(PAKISTAN_TIMEZONE)
            
            # Run scraping job if market is open
            if is_market_open(now_pk):
                success = run_scraping_job()
                
                if success:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    logger.warning(f"Consecutive failures: {consecutive_failures}")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(f"Too many consecutive failures ({consecutive_failures}). "
                                   f"Waiting 10 minutes before retrying...")
                        time.sleep(600)  # Wait 10 minutes
                        consecutive_failures = 0  # Reset counter
                        continue
                
                # Calculate next run time
                next_run = get_next_scrape_time()
                wait_seconds = (next_run - now_pk).total_seconds()
                
                # Ensure we don't wait negative time
                if wait_seconds > 0:
                    logger.info(f"Next scheduled run at: {next_run.strftime('%H:%M:%S')} PKT "
                              f"(in {wait_seconds:.0f} seconds)")
                    time.sleep(wait_seconds)
                else:
                    # If calculation error, wait 5 minutes
                    logger.warning("Wait time calculation error, waiting 5 minutes...")
                    time.sleep(300)
                    
            else:
                # Market is closed
                logger.info("Market is closed. Checking again in 5 minutes...")
                
                # Wait 5 minutes before checking again
                time.sleep(300)
                
        except KeyboardInterrupt:
            logger.info("Scraper stopped by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error in scheduler: {e}")
            logger.info("Waiting 1 minute before retrying...")
            time.sleep(60)

def test_scraper():
    """Test function for single run"""
    logger.info("Running test scraper (single execution)...")
    success = run_scraping_job()
    
    if success:
        logger.info("Test completed successfully ✓")
    else:
        logger.error("Test failed ✗")
    
    return success

if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--test":
            test_scraper()
        elif sys.argv[1] == "--setup-db":
            setup_database()
        elif sys.argv[1] == "--help":
            print("Usage:")
            print("  python final_fix_scrap.py           # Start scheduled scraper")
            print("  python final_fix_scrap.py --test    # Run single test")
            print("  python final_fix_scrap.py --setup-db # Setup database only")
            print("  python final_fix_scrap.py --help    # Show this help")
    else:
        # Start the scheduled scraper
        schedule_scraper()