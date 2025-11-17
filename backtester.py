import pandas as pd
import numpy as np
from datetime import date, datetime, time as dt_time
import os

# ==============================================================================
# UPDATED FOR TEST CASE #1
# This script is now configured to run the backtest for the proxy date of
# January 14, 2025, using the corresponding downloaded data.
# ==============================================================================

# --- Part 1: Setup & Initialization ---

# UPDATED: Set the Proxy Date for Test Case #1
PROXY_DATE = date(2025, 1, 14)

# --- Strategy Constants ---
PROFIT_TARGET_PCT = 0.40  # Target 40% of max profit
STOP_LOSS_PCT = 0.80      # Exit if loss exceeds 80% of max profit

# --- Part 2: Mock Data Feed Preparation ---

def prepare_mock_data_feed():
    """
    Reads downloaded CSVs from the 'test_case_1' directory and creates a 
    synthetic intraday feed by linear interpolation.
    """
    print("Preparing mock data feed from downloaded CSVs for Test Case #1...")
    
    try:
        # 1. Use hardcoded Nifty Spot OHLC for the proxy date from user-provided data
        # Data for Jan 14, 2025: OPEN=23248.00, CLOSE=23271.75
        spot_open = 23248.00
        spot_close = 23271.75

        # 2. Read the OPEN and CLOSE from each options CSV within the test_case_1 folder
        options_ohlc = {}
        # UPDATED: Point to the correct sub-directory
        test_case_dir = "test_case_1" 
        leg_files = {
            "sell_ce": os.path.join(test_case_dir, "sell_ce_data.csv"),
            "buy_ce": os.path.join(test_case_dir, "buy_ce_data.csv"),
            "sell_pe": os.path.join(test_case_dir, "sell_pe_data.csv"),
            "buy_pe": os.path.join(test_case_dir, "buy_pe_data.csv"),
        }

        for name, filename in leg_files.items():
            if not os.path.exists(filename):
                raise FileNotFoundError(f"Required data file not found: {filename}")
            
            df = pd.read_csv(filename)
            if df.empty:
                raise ValueError(f"Data file is empty: {filename}")
            
            options_ohlc[f"{name}_open"] = df['OPEN'].iloc[0]
            options_ohlc[f"{name}_close"] = df['CLOSE'].iloc[0]
            print(f"Read {filename}: OPEN={options_ohlc[f'{name}_open']}, CLOSE={options_ohlc[f'{name}_close']}")

        # 3. Create Synthetic Intraday Feed (9:15 to 15:30 -> 376 minutes)
        trading_minutes = 376
        timestamps = pd.to_datetime(pd.date_range(f"{PROXY_DATE} 09:15", f"{PROXY_DATE} 15:30", freq="min"))
        
        mock_feed = pd.DataFrame(index=timestamps)
        mock_feed['spot_price'] = np.linspace(spot_open, spot_close, trading_minutes)
        
        for name, filepath in leg_files.items():
            open_price = options_ohlc[f"{name}_open"]
            close_price = options_ohlc[f"{name}_close"]
            mock_feed[f"{name}_ltp"] = np.linspace(open_price, close_price, trading_minutes)

        print("Mock data feed prepared successfully from real historical data.")
        return mock_feed

    except Exception as e:
        print(f"FATAL: Could not prepare mock data feed due to error: {e}")
        return None

# --- Part 3: Backtesting Logic (Unchanged) ---

def log_trade(log_df, timestamp, event, details):
    """Appends a trade event to the log."""
    print(f"{timestamp} | {event}: {details}")
    new_log = pd.DataFrame([{"timestamp": timestamp, "event": event, "details": details}])
    # Use pd.concat instead of the deprecated append method
    return pd.concat([log_df, new_log], ignore_index=True)

def run_backtest(mock_data_feed):
    """
    Iterates through the mock data feed and applies the trading strategy.
    """
    if mock_data_feed is None:
        print("Backtest cannot run because mock data feed failed to generate.")
        return pd.DataFrame()

    print("\n--- Running Backtest ---")
    trade_log = pd.DataFrame({
        "timestamp": pd.Series(dtype='datetime64[ns]'),
        "event": pd.Series(dtype='object'),
        "details": pd.Series(dtype='object')
    })
    
    trade_active = False
    position_book = {}
    net_credit = 0.0
    pnl = 0.0
    profit_target = 0.0
    stop_loss = 0.0
    entry_time = dt_time(9, 45) # Corrected time object usage

    for timestamp, row in mock_data_feed.iterrows():
        current_time = timestamp.time()
        
        if not trade_active and current_time >= entry_time:
            trade_active = True
            
            position_book = {
                "sell_ce": row['sell_ce_ltp'], "buy_ce": row['buy_ce_ltp'],
                "sell_pe": row['sell_pe_ltp'], "buy_pe": row['buy_pe_ltp'],
            }
            
            net_credit = (position_book['sell_ce'] - position_book['buy_ce']) + \
                         (position_book['sell_pe'] - position_book['buy_pe'])
            
            # Ensure net credit is positive before proceeding
            if net_credit <= 0:
                trade_log = log_trade(trade_log, timestamp, "TRADE REJECTED", f"Negative Net Credit: {net_credit:.2f}")
                break

            details = f"Net Credit: {net_credit:.2f} | Positions: {position_book}"
            trade_log = log_trade(trade_log, timestamp, "ENTRY", details)
            
            profit_target = net_credit * PROFIT_TARGET_PCT
            stop_loss = net_credit * STOP_LOSS_PCT
            trade_log = log_trade(trade_log, timestamp, "TARGETS", f"Profit Target: {profit_target:.2f}, Stop-Loss: {stop_loss:.2f}")

        if trade_active:
            # Mark-to-Market P&L calculation
            mtm_sell_ce = position_book['sell_ce'] - row['sell_ce_ltp']
            mtm_buy_ce = row['buy_ce_ltp'] - position_book['buy_ce']
            mtm_sell_pe = position_book['sell_pe'] - row['sell_pe_ltp']
            mtm_buy_pe = row['buy_pe_ltp'] - position_book['buy_pe']
            
            pnl = (mtm_sell_ce + mtm_buy_ce + mtm_sell_pe + mtm_buy_pe)
            
            exit_reason = None
            if pnl >= profit_target:
                exit_reason = f"Profit Target Hit ({pnl:.2f} >= {profit_target:.2f})"
            elif pnl <= -stop_loss:
                exit_reason = f"Stop-Loss Hit ({pnl:.2f} <= {-stop_loss:.2f})"
            elif current_time >= dt_time(15, 10): # Corrected time object usage
                exit_reason = "End of Day Exit"

            if exit_reason:
                details = f"Exit Reason: {exit_reason} | Final P&L: {pnl:.2f}"
                trade_log = log_trade(trade_log, timestamp, "EXIT", details)
                break
    
    if not any(trade_log['event'] == 'ENTRY'):
        trade_log = log_trade(trade_log, datetime.now(), "NO TRADE", "Entry conditions were not met.")
        
    print("--- Backtest Finished ---")
    return trade_log

# --- Part 4: Main Execution Block ---

if __name__ == "__main__":
    mock_feed = prepare_mock_data_feed()
    final_log = run_backtest(mock_feed)
    
    log_filename = 'backtest_log_test_case_1.csv'
    llm_prompt_filename = 'llm_prompt_test_case_1.txt'

    if not final_log.empty:
        final_log.to_csv(log_filename, index=False)
        print(f"\nBacktest log saved to '{log_filename}'")
        
        pnl_events = final_log[final_log['event'] == 'EXIT']
        if not pnl_events.empty:
            entry_row = final_log[final_log['event'] == 'ENTRY'].iloc[0]
            exit_row = pnl_events.iloc[0]
            pnl_str = exit_row['details'].split('Final P&L: ')[1]
            
            print("\n--- Backtest Summary ---")
            print(f"Proxy Date: {PROXY_DATE}")
            print(f"Entry Time: {entry_row['timestamp']}")
            print(f"Exit Time: {exit_row['timestamp']}")
            print(f"Exit Reason: {exit_row['details'].split('|')[0].strip()}")
            print(f"Total P&L: {pnl_str}")
            print("------------------------")
        else:
            entry_reject = final_log[final_log['event'] == 'TRADE REJECTED']
            if not entry_reject.empty:
                print("\n--- Backtest Summary ---")
                print("Trade was REJECTED. See log for details.")
                print("------------------------")
            else:
                print("\n--- Backtest Summary ---")
                print("No trade was executed or completed.")
                print("------------------------")


{final_log.to_csv(index=False)}