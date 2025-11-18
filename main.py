import time
import pandas as pd
from datetime import datetime, time as dt_time
from jugaad_data.nse import NSELive
import requests
import json
import os
from dotenv import load_dotenv
from flask import Flask, jsonify
from threading import Thread
from apscheduler.schedulers.background import BackgroundScheduler
from pytz import timezone

# --- Load Environment Variables ---
load_dotenv()

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Global State & Configuration ---
bot_state = {
    "trade_active": False,
    "position_book": {},
    "pnl_per_lot": 0.0,
    "status_message": "Initializing..."
}

# --- Application & State Configuration ---
STATE_FILE = 'trade_state.json'
POLLING_INTERVAL_SECONDS = int(os.getenv('POLLING_INTERVAL_SECONDS', 8))
MARKET_OPEN_TIME = dt_time.fromisoformat(os.getenv('MARKET_OPEN_TIME', '09:25:00'))
MARKET_CLOSE_TIME = dt_time.fromisoformat(os.getenv('MARKET_CLOSE_TIME', '15:30:00'))
LOG_FILE_NAME = os.getenv('LOG_FILE_NAME', 'paper_trade_log.csv')
INDIA_TZ = timezone('Asia/Kolkata')

# --- Telegram Configuration ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# --- Strategy Parameters ---
PRIMARY_STRATEGY = os.getenv('PRIMARY_STRATEGY', 'IronCondor')
LOT_SIZE = int(os.getenv('LOT_SIZE', 50))
INSTRUMENTS = {
    'SELL_CE_STRIKE': int(os.getenv('SELL_CE_STRIKE', 0)),
    'BUY_CE_STRIKE': int(os.getenv('BUY_CE_STRIKE', 0)),
    'SELL_PE_STRIKE': int(os.getenv('SELL_PE_STRIKE', 0)),
    'BUY_PE_STRIKE': int(os.getenv('BUY_PE_STRIKE', 0)),
}
ENTRY_CONDITIONS = {
    'MIN_SPOT': float(os.getenv('MIN_SPOT', 0)),
    'MAX_SPOT': float(os.getenv('MAX_SPOT', 0)),
}
ENTRY_TIME_START = dt_time.fromisoformat(os.getenv('ENTRY_TIME_START', '09:30:00'))
STOP_LOSS_RANGE = {
    'MIN': float(os.getenv('STOP_LOSS_MIN', 0)),
    'MAX': float(os.getenv('STOP_LOSS_MAX', 0)),
}
PROFIT_TARGET_PER_LOT = float(os.getenv('PROFIT_TARGET_PER_LOT', 0))

# --- Global DataFrame for Logging ---
trade_log_cols = ['timestamp', 'action', 'instrument', 'price', 'pnl', 'commentary']
trade_log = pd.DataFrame({
    'timestamp': pd.Series(dtype='datetime64[ns]'), 
    'action': pd.Series(dtype='object'),
    'instrument': pd.Series(dtype='object'), 
    'price': pd.Series(dtype='float64'),
    'pnl': pd.Series(dtype='float64'), 
    'commentary': pd.Series(dtype='object')
})

# --- Core Functions ---
def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials not found. Skipping notification.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            print(f"Error sending Telegram message: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Exception while sending Telegram message: {e}")

def log_trade(timestamp, action, instrument, price, pnl, commentary):
    global trade_log
    new_log_entry = pd.DataFrame([[timestamp, action, instrument, price, pnl, commentary]], columns=trade_log_cols)
    trade_log = pd.concat([trade_log, new_log_entry], ignore_index=True)

def fetch_live_data(n: NSELive, instruments: dict) -> dict:
    """
    Fetch live data from NSE with proper data structure handling.
    Returns a dictionary with spot price and option LTPs.
    """
    live_data = {
        'spot': None, 
        'sell_ce_ltp': None, 
        'buy_ce_ltp': None, 
        'sell_pe_ltp': None, 
        'buy_pe_ltp': None, 
        'timestamp': datetime.now()
    }
    
    try:
        # Fetch live index data - returns dict with 'name', 'timestamp', 'data' keys
        index_response = n.live_index("NIFTY 50")
        
        # The response structure: {'name': 'NIFTY 50', 'timestamp': '...', 'data': [...]}
        # Extract spot price from the first element in data array
        if isinstance(index_response, dict) and 'data' in index_response:
            if len(index_response['data']) > 0:
                live_data['spot'] = index_response['data'][0].get('lastPrice')
                print(f"Spot Price: {live_data['spot']}")
        else:
            print("API WARNING: Unexpected format for live_index response.")
            print(f"Response: {index_response}")

        # Fetch option chain data
        oc_data = n.index_option_chain("NIFTY")
        
        # Debug: Print structure to understand the format
        if not oc_data:
            print("WARNING: Option chain data is empty")
            return live_data
            
        # The option chain structure: {'records': {'data': [...]}, ...}
        records = oc_data.get('records', {})
        data_list = records.get('data', [])
        
        if not data_list:
            print("WARNING: No data in option chain")
            return live_data
        
        # Helper function to find LTP for a given strike and option type
        def get_ltp(strike, option_type):
            """
            Extract LTP from option chain data.
            Each record contains 'strikePrice', 'CE', and 'PE' keys.
            """
            for record in data_list:
                if record.get('strikePrice') == strike:
                    option_data = record.get(option_type, {})
                    if option_data and isinstance(option_data, dict):
                        ltp = option_data.get('lastPrice')
                        if ltp is not None:
                            return float(ltp)
            return None

        # Fetch LTPs for all instruments
        live_data['sell_ce_ltp'] = get_ltp(instruments['SELL_CE_STRIKE'], 'CE')
        live_data['buy_ce_ltp'] = get_ltp(instruments['BUY_CE_STRIKE'], 'CE')
        live_data['sell_pe_ltp'] = get_ltp(instruments['SELL_PE_STRIKE'], 'PE')
        live_data['buy_pe_ltp'] = get_ltp(instruments['BUY_PE_STRIKE'], 'PE')
        
        # Log which prices were found
        print(f"Fetched prices - SELL_CE: {live_data['sell_ce_ltp']}, "
              f"BUY_CE: {live_data['buy_ce_ltp']}, "
              f"SELL_PE: {live_data['sell_pe_ltp']}, "
              f"BUY_PE: {live_data['buy_pe_ltp']}")

    except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, IndexError, TypeError) as e:
        print(f"API ERROR: {e} at {live_data['timestamp']}. Returning partial/None data.")
        import traceback
        traceback.print_exc()
    
    return live_data

# --- Scheduled Tasks ---
def morning_status_update():
    """Send a morning status update at 9:30 AM IST."""
    now_ist = datetime.now(INDIA_TZ)
    message = (
        f"☀️ *Good Morning!* (IST: {now_ist.strftime('%H:%M:%S')})\n"
        f"Bot Status: `{bot_state['status_message']}`\n"
        f"Awaiting market open and entry conditions."
    )
    send_telegram_message(message)

# --- Web Server Endpoints ---
@app.route('/health')
def health_check():
    """Health check endpoint for Render."""
    return "OK", 200

@app.route('/')
def status_page():
    """Simple status page to view bot state."""
    return jsonify({
        "status": bot_state["status_message"],
        "trade_active": bot_state["trade_active"],
        "pnl_per_lot": bot_state["pnl_per_lot"],
        "position_book": bot_state["position_book"],
        "server_time": datetime.now().isoformat()
    })

# --- Trading Bot Logic ---
def run_trading_bot():
    """Main function for the trading bot, designed to be run in a thread."""
    global trade_log, bot_state

    # --- 1. Initialization & State Restoration ---
    try:
        n = NSELive()
        print("NSELive initialized successfully")
    except Exception as e:
        print(f"Failed to initialize NSELive: {e}. Exiting.")
        send_telegram_message(f"CRITICAL: Failed to initialize NSELive API: {e} 🛑")
        bot_state["status_message"] = f"CRITICAL ERROR: {e}"
        return

    if os.path.exists(STATE_FILE):
        print("Found existing state file. Loading previous trade state.")
        send_telegram_message("🔄 Bot restarted. Loading existing trade state...")
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            bot_state['trade_active'] = state.get('trade_active', False)
            bot_state['position_book'] = state.get('position_book', {})
        if not bot_state['trade_active'] or not bot_state['position_book']:
            print("State file was invalid. Starting fresh.")
            bot_state['trade_active'] = False
            bot_state['position_book'] = {}
    
    if not bot_state['trade_active']:
        print(f"--- Paper Trading Bot Initialized (Strategy: {PRIMARY_STRATEGY}) ---")
    
    # --- 2. Main Trading Loop ---
    try:
        while True:
            current_time = datetime.now().time()
            
            if not (MARKET_OPEN_TIME <= current_time <= MARKET_CLOSE_TIME):
                bot_state["status_message"] = "Market is closed."
                print(bot_state["status_message"])
                if bot_state['trade_active']:
                    exit_message = (
                        f"🛑 *--- TRADE EXITED (Market Closed) ---*\n"
                        f"Final P&L: *₹{bot_state['pnl_per_lot']:.2f}*"
                    )
                    send_telegram_message(exit_message)
                    if os.path.exists(STATE_FILE):
                        os.remove(STATE_FILE)
                    bot_state['trade_active'] = False
                    bot_state['position_book'] = {}
                
                print("Trading loop finished for the day.")
                break
                
            live_data = fetch_live_data(n, INSTRUMENTS)
            
            # Check if we have valid data
            if any(v is None for v in live_data.values()):
                bot_state["status_message"] = "API Error or Missing Data. Skipping iteration."
                print(f"{live_data['timestamp']} - {bot_state['status_message']}")
                time.sleep(POLLING_INTERVAL_SECONDS)
                continue

            # --- Entry Logic ---
            if not bot_state['trade_active']:
                spot = live_data['spot']
                bot_state["status_message"] = f"Waiting for entry. Spot: {spot:.2f}"
                
                if current_time < ENTRY_TIME_START:
                    print(f"Status: {current_time} | Waiting for {ENTRY_TIME_START}. Spot: {spot}")
                    time.sleep(POLLING_INTERVAL_SECONDS)
                    continue
                
                if (ENTRY_CONDITIONS['MIN_SPOT'] <= spot <= ENTRY_CONDITIONS['MAX_SPOT']):
                    bot_state['trade_active'] = True
                    bot_state['position_book'] = {
                        'SELL_CE': live_data['sell_ce_ltp'],
                        'BUY_CE': live_data['buy_ce_ltp'],
                        'SELL_PE': live_data['sell_pe_ltp'],
                        'BUY_PE': live_data['buy_pe_ltp']
                    }
                    
                    # Save state to file
                    with open(STATE_FILE, 'w') as f:
                        json.dump({
                            'trade_active': True,
                            'position_book': bot_state['position_book']
                        }, f)
                    
                    entry_message = (
                        f"🚀 *--- TRADE ENTERED ---*\n"
                        f"Strategy: {PRIMARY_STRATEGY}\n"
                        f"Spot Price: *{spot:.2f}*\n"
                        f"SELL CE {INSTRUMENTS['SELL_CE_STRIKE']}: {live_data['sell_ce_ltp']}\n"
                        f"BUY CE {INSTRUMENTS['BUY_CE_STRIKE']}: {live_data['buy_ce_ltp']}\n"
                        f"SELL PE {INSTRUMENTS['SELL_PE_STRIKE']}: {live_data['sell_pe_ltp']}\n"
                        f"BUY PE {INSTRUMENTS['BUY_PE_STRIKE']}: {live_data['buy_pe_ltp']}"
                    )
                    send_telegram_message(entry_message)
                    log_trade(live_data['timestamp'], 'ENTRY', PRIMARY_STRATEGY, spot, 0, 'Trade entered')

            # --- Monitoring & Exit Logic ---
            if bot_state['trade_active']:
                # Calculate P&L
                entry_book = bot_state['position_book']
                pnl_per_lot = (
                    (entry_book['SELL_CE'] - live_data['sell_ce_ltp']) +
                    (live_data['buy_ce_ltp'] - entry_book['BUY_CE']) +
                    (entry_book['SELL_PE'] - live_data['sell_pe_ltp']) +
                    (live_data['buy_pe_ltp'] - entry_book['BUY_PE'])
                ) * LOT_SIZE
                
                bot_state["pnl_per_lot"] = pnl_per_lot
                bot_state["status_message"] = f"Position active. P&L: {pnl_per_lot:.2f}"
                print(f"{live_data['timestamp']} - {bot_state['status_message']}")
                
                exit_reason = None
                if pnl_per_lot >= PROFIT_TARGET_PER_LOT:
                    exit_reason = "PROFIT_TARGET"
                elif not (STOP_LOSS_RANGE['MIN'] <= live_data['spot'] <= STOP_LOSS_RANGE['MAX']):
                    exit_reason = f"STOP_LOSS (Spot {live_data['spot']} breached range)"
                elif current_time >= MARKET_CLOSE_TIME:
                    exit_reason = "END_OF_DAY"
                    
                if exit_reason:
                    exit_message = (
                        f"🛑 *--- TRADE EXITED ---*\n"
                        f"Reason: {exit_reason}\n"
                        f"Final P&L: *₹{pnl_per_lot:.2f}*"
                    )
                    send_telegram_message(exit_message)
                    log_trade(live_data['timestamp'], 'EXIT', PRIMARY_STRATEGY, 
                             live_data['spot'], pnl_per_lot, exit_reason)
                    
                    if os.path.exists(STATE_FILE):
                        os.remove(STATE_FILE)
                    
                    bot_state['trade_active'] = False
                    bot_state['position_book'] = {}
                    bot_state['pnl_per_lot'] = 0.0
                    break
            
            time.sleep(POLLING_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        bot_state["status_message"] = "Manual override detected. Shutting down."
        print(bot_state["status_message"])
        send_telegram_message("Manual override detected. 🛑 Shutting down.")
    except Exception as e:
        bot_state["status_message"] = f"CRITICAL ERROR: {e}"
        print(f"--- {bot_state['status_message']} ---")
        import traceback
        traceback.print_exc()
        send_telegram_message(f"CRITICAL ERROR: {e} 🛑 Shutting down.")
    
    finally:
        if not trade_log.empty:
            trade_log.to_csv(LOG_FILE_NAME, index=False)
            print(f"Trade log saved to {LOG_FILE_NAME}")
        print("Trading bot thread finished.")

# --- Gunicorn Application Startup ---
send_telegram_message(
    f"✅ *Deployment Successful & Bot Initialized*\n"
    f"Strategy: {PRIMARY_STRATEGY}\n"
    f"Watching NIFTY 50."
)

# Initialize and start the scheduler
scheduler = BackgroundScheduler(timezone=INDIA_TZ)
scheduler.add_job(morning_status_update, 'cron', hour=9, minute=30)
scheduler.start()

# Start the main trading bot logic in a background thread
bot_thread = Thread(target=run_trading_bot, daemon=True)
bot_thread.start()

# --- Main Execution Block (for local development) ---
if __name__ == "__main__":
    port = int(os.getenv('PORT', 10000))
    app.run(host='0.0.0.0', port=port)