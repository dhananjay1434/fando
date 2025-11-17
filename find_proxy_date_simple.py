
import pandas as pd
from jugaad_data.nse import index_df
from datetime import date, timedelta
import sys

def find_proxy_date_simple():
    """
    A simplified attempt to find a proxy date, with no cache handling.
    """
    print("Attempting to fetch data with a simplified script...")
    to_date = date.today()
    from_date = to_date - timedelta(days=365) 
    
    try:
        nifty_data = index_df(symbol="NIFTY 50", from_date=from_date, to_date=to_date)
        print("Data fetched successfully.")
        nifty_data = nifty_data.sort_index(ascending=True)
        
        nifty_data['OPEN'] = pd.to_numeric(nifty_data['OPEN'])
        nifty_data['HIGH'] = pd.to_numeric(nifty_data['HIGH'])
        nifty_data['LOW'] = pd.to_numeric(nifty_data['LOW'])
        nifty_data['CLOSE'] = pd.to_numeric(nifty_data['CLOSE'])

        # Scan for the pattern from newest to oldest
        for i in range(len(nifty_data) - 1, 0, -1):
            t_minus_1 = nifty_data.iloc[i-1]
            t_day = nifty_data.iloc[i]

            is_breakout = (
                ((t_minus_1['CLOSE'] - t_minus_1['OPEN']) / t_minus_1['OPEN']) * 100 > 0.5 and
                (t_minus_1['CLOSE'] > t_minus_1['HIGH'] * 0.995)
            )

            if is_breakout:
                is_inside_day = (
                    t_day['HIGH'] < t_minus_1['HIGH'] and
                    t_day['LOW'] > t_minus_1['LOW']
                )

                if is_inside_day:
                    proxy_date = t_day.name.date()
                    print(f"SUCCESS: Proxy Date Found: {proxy_date}")
                    return

        print("INFO: No date matching the criteria was found in the last year.")

    except Exception as e:
        print(f"ERROR: The script failed as expected. The error was: {e}")
        sys.exit(1) # Exit with an error code

if __name__ == "__main__":
    find_proxy_date_simple()
