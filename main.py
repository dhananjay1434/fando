import time
import pandas as pd
from datetime import datetime, time as dt_time
from jugaad_data.nse import NSELive
import requests
import json
import os
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()

# --- Application & State Configuration ---
STATE_FILE = 'trade_state.json'
POLLING_INTERVAL_SECONDS = int(os.getenv('POLLING_INTERVAL_SECONDS', 8))
MARKET_OPEN_TIME = dt_time.fromisoformat(
    os.getenv('MARKET_OPEN_TIME', '09:25:00'))
MARKET_CLOSE_TIME = dt_time.fromisoformat(
    os.getenv('MARKET_CLOSE_TIME', '15:30:00'))
LOG_FILE_NAME = os.getenv('LOG_FILE_NAME', 'paper_trade_log.csv')

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
ENTRY_TIME_START = dt_time.fromisoformat(
    os.getenv('ENTRY_TIME_START', '09:30:00'))
STOP_LOSS_RANGE = {
    'MIN': float(os.getenv('STOP_LOSS_MIN', 0)),
    'MAX': float(os.getenv('STOP_LOSS_MAX', 0)),
}
PROFIT_TARGET_PER_LOT = float(os.getenv('PROFIT_TARGET_PER_LOT', 0))

# --- Global DataFrame for Logging ---
trade_log_cols = ['timestamp', 'action',
                  'instrument', 'price', 'pnl', 'commentary']
trade_log = pd.DataFrame({
    'timestamp': pd.Series(dtype='datetime64[ns]'), 'action': pd.Series(dtype='object'),
    'instrument': pd.Series(dtype='object'), 'price': pd.Series(dtype='float64'),
    'pnl': pd.Series(dtype='float64'), 'commentary': pd.Series(dtype='object')
})

# --- Core Functions ---


def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials not found. Skipping notification.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID,
               'text': message, 'parse_mode': 'Markdown'}
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            print(
                f"Error sending Telegram message: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Exception while sending Telegram message: {e}")


def log_trade(timestamp, action, instrument, price, pnl, commentary):
    global trade_log
    new_log_entry = pd.DataFrame(
        [[timestamp, action, instrument, price, pnl, commentary]], columns=trade_log_cols)
    trade_log = pd.concat([trade_log, new_log_entry], ignore_index=True)


def fetch_live_data(n: NSELive, instruments: dict) -> dict:
    live_data = {'spot': None, 'sell_ce_ltp': None, 'buy_ce_ltp': None,
                 'sell_pe_ltp': None, 'buy_pe_ltp': None, 'timestamp': datetime.now()}
    target_strikes = set(instruments.values())
    try:
        spot_data = n.live_index("NIFTY 50")
        live_data['spot'] = spot_data['data'][0]['lastPrice']
        oc_data = n.index_option_chain("NIFTY")
        filtered_list = oc_data.get('filtered', {}).get('data', [])
        legs_found = 0
        for item in filtered_list:
            strike = item.get('strikePrice')
            if strike not in target_strikes:
                continue
            if strike == instruments['SELL_CE_STRIKE']:
                live_data['sell_ce_ltp'] = item.get('CE', {}).get('lastPrice')
                legs_found += 1
            elif strike == instruments['BUY_CE_STRIKE']:
                live_data['buy_ce_ltp'] = item.get('CE', {}).get('lastPrice')
                legs_found += 1
            elif strike == instruments['SELL_PE_STRIKE']:
                live_data['sell_pe_ltp'] = item.get('PE', {}).get('lastPrice')
                legs_found += 1
            elif strike == instruments['BUY_PE_STRIKE']:
                live_data['buy_pe_ltp'] = item.get('PE', {}).get('lastPrice')
                legs_found += 1
            if legs_found == 4:
                break
    except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, IndexError, TypeError) as e:
        print(
            f"API ERROR: {e} at {live_data['timestamp']}. Returning partial/None data.")
    return live_data


# --- Main Execution Block ---
if __name__ == "__main__":
    # --- 1. Initialization & State Restoration ---
    try:
        n = NSELive()
    except Exception as e:
        print(f"Failed to initialize NSELive: {e}. Exiting.")
        send_telegram_message(
            f"CRITICAL: Failed to initialize NSELive API: {e} 🛑")
        exit()

    if os.path.exists(STATE_FILE):
        print("Found existing state file. Loading previous trade state.")
        send_telegram_message("🔄 Bot restarted. Loading existing trade state...")
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            trade_active = state.get('trade_active', False)
            position_book = state.get('position_book', {})
        if not trade_active or not position_book:
            print("State file was invalid. Starting fresh.")
            trade_active = False
            position_book = {}
    else:
        trade_active = False
        position_book = {}

    if not trade_active:
        print(
            f"--- Paper Trading Bot Initialized (Strategy: {PRIMARY_STRATEGY}) ---")
        print(
            f"--- Polling every {POLLING_INTERVAL_SECONDS} seconds. LOG: {LOG_FILE_NAME} ---")
        send_telegram_message(
            f"✅ *Trading Bot Initialized*\nStrategy: {PRIMARY_STRATEGY}\nWatching NIFTY 50.")

    # --- 2. Main Trading Loop ---
    try:
        while True:
            current_time = datetime.now().time()

            if not (MARKET_OPEN_TIME <= current_time <= MARKET_CLOSE_TIME):
                print("Market is closed. Ending trading loop.")
                if not trade_active:  # Only send if no trade is active, otherwise exit logic handles it
                    send_telegram_message(
                        "Market is closed. Trading bot shutting down. 👋")
                break

            live_data = fetch_live_data(n, INSTRUMENTS)

            if any(v is None for v in live_data.values()):
                print(
                    f"{live_data['timestamp']} - API Error or Missing Data. Skipping iteration.")
                time.sleep(POLLING_INTERVAL_SECONDS)
                continue

            # --- 2.A. Entry Logic ---
            if not trade_active:
                spot = live_data['spot']
                if current_time < ENTRY_TIME_START:
                    print(
                        f"Status: {current_time} | Waiting for {ENTRY_TIME_START}. Spot: {spot}")
                    time.sleep(POLLING_INTERVAL_SECONDS)
                    continue
                if (ENTRY_CONDITIONS['MIN_SPOT'] <= spot <= ENTRY_CONDITIONS['MAX_SPOT']):
                    trade_active = True
                    position_book = {'SELL_CE': live_data['sell_ce_ltp'], 'BUY_CE': live_data['buy_ce_ltp'],
                                     'SELL_PE': live_data['sell_pe_ltp'], 'BUY_PE': live_data['buy_pe_ltp']}
                    with open(STATE_FILE, 'w') as f:
                        json.dump(
                            {'trade_active': True, 'position_book': position_book}, f)

                    print(
                        f"--- TRADE ENTERED at {live_data['timestamp']} (Spot: {spot}) ---")
                    entry_message = (f"🚀 *--- TRADE ENTERED ---*\n" f"Strategy: {PRIMARY_STRATEGY}\n" f"Spot Price: *{spot:.2f}*\n\n" f"*Positions Entered:*\n" f"- SELL CE {INSTRUMENTS['SELL_CE_STRIKE']} @ {position_book['SELL_CE']:.2f}\n" f"- BUY CE {INSTRUMENTS['BUY_CE_STRIKE']} @ {position_book['BUY_CE']:.2f}\n" f"- SELL PE {INSTRUMENTS['SELL_PE_STRIKE']} @ {position_book['SELL_PE']:.2f}\n" f"- BUY PE {INSTRUMENTS['BUY_PE_STRIKE']} @ {position_book['BUY_PE']:.2f}")
                    send_telegram_message(entry_message)

                    comment = f"Trade Entered. Spot at {spot}"
                    for leg, price in position_book.items():
                        log_trade(live_data['timestamp'], 'ENTER',
                                  f"{leg}_{INSTRUMENTS[f'{leg}_STRIKE']}", price, 0, comment)

            # --- 2.B. Monitoring & Exit Logic ---
            if trade_active:
                pnl_sell_ce = (
                    position_book['SELL_CE'] - live_data['sell_ce_ltp']) * LOT_SIZE
                pnl_buy_ce = (
                    live_data['buy_ce_ltp'] - position_book['BUY_CE']) * LOT_SIZE
                pnl_sell_pe = (
                    position_book['SELL_PE'] - live_data['sell_pe_ltp']) * LOT_SIZE
                pnl_buy_pe = (
                    live_data['buy_pe_ltp'] - position_book['BUY_PE']) * LOT_SIZE
                pnl_per_lot = pnl_sell_ce + pnl_buy_ce + pnl_sell_pe + pnl_buy_pe

                exit_reason, spot = None, live_data['spot']
                if pnl_per_lot >= PROFIT_TARGET_PER_LOT:
                    exit_reason = "PROFIT_TARGET"
                elif not (STOP_LOSS_RANGE['MIN'] <= spot <= STOP_LOSS_RANGE['MAX']):
                    exit_reason = f"STOP_LOSS (Spot {spot} breached range)"
                elif current_time >= MARKET_CLOSE_TIME:
                    exit_reason = "END_OF_DAY"

                if exit_reason:
                    print(
                        f"--- EXIT TRIGGERED: {exit_reason} at {live_data['timestamp']} (P&L: {pnl_per_lot:.2f}) ---")
                    exit_message = (
                        f"🛑 *--- TRADE EXITED ---*\n" f"Reason: {exit_reason}\n" f"Final P&L: *₹{pnl_per_lot:.2f}*\n\n" f"Spot Price: {spot:.2f}")
                    send_telegram_message(exit_message)

                    if os.path.exists(STATE_FILE):
                        os.remove(STATE_FILE)

                    trade_active = False
                    comment = f"Exit: {exit_reason}. Final P&L {pnl_per_lot:.2f}"
                    log_trade(live_data['timestamp'], 'EXIT', f"SELL_CE_{INSTRUMENTS['SELL_CE_STRIKE']}",
                              live_data['sell_ce_ltp'], pnl_sell_ce, comment)
                    log_trade(live_data['timestamp'], 'EXIT', f"BUY_CE_{INSTRUMENTS['BUY_CE_STRIKE']}",
                              live_data['buy_ce_ltp'], pnl_buy_ce, comment)
                    log_trade(live_data['timestamp'], 'EXIT', f"SELL_PE_{INSTRUMENTS['SELL_PE_STRIKE']}",
                              live_data['sell_pe_ltp'], pnl_sell_pe, comment)
                    log_trade(live_data['timestamp'], 'EXIT', f"BUY_PE_{INSTRUMENTS['BUY_PE_STRIKE']}",
                              live_data['buy_pe_ltp'], pnl_buy_pe, comment)
                    break
                else:
                    print(
                        f"{live_data['timestamp']} | Market: {spot:.2f}, P&L: {pnl_per_lot:.2f}, Status: Holding")

            time.sleep(POLLING_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("--- Manual override detected. Terminating loop. ---")
        send_telegram_message("Manual override detected. 🛑 Shutting down.")
    except Exception as e:
        print(f"--- CRITICAL ERROR in main loop: {e} ---")
        send_telegram_message(f"CRITICAL ERROR: {e} 🛑 Shutting down.")

    # --- 3. End of Day: Data Persistence ---
    finally:
        if not trade_log.empty:
            print(f"Trading loop finished. Saving log to {LOG_FILE_NAME}")
            try:
                trade_log.to_csv(LOG_FILE_NAME, index=False)
                print("Log saved successfully.")
            except Exception as e:
                print(f"CRITICAL: Failed to save log file: {e}")
        else:
            print("Trading loop finished. No trades were logged.")
        print("Application terminating.")
