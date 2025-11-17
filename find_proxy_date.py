
import pandas as pd
from jugaad_data import set_cache_location
from jugaad_data.nse import index_df
from datetime import date, timedelta
import os
import shutil

def setup_temp_cache():
    """Sets up a temporary cache directory within the project folder."""
    temp_dir = os.path.join(os.getcwd(), "temp_jugaad_cache")
    os.makedirs(temp_dir, exist_ok=True)
    set_cache_location(temp_dir)
    return temp_dir

def cleanup_temp_cache(temp_dir):
    """Removes the temporary cache directory."""
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

def find_proxy_date():
    """
    Finds a historical date with a breakout day followed by a consolidation day.
    """
    to_date = date.today()
    from_date = to_date - timedelta(days=365) 
    
    try:
        nifty_data = index_df(symbol="NIFTY 50", from_date=from_date, to_date=to_date)
        nifty_data = nifty_data.sort_index(ascending=True)
        
        # Ensure columns are numeric
        nifty_data['OPEN'] = pd.to_numeric(nifty_data['OPEN'])
        nifty_data['HIGH'] = pd.to_numeric(nifty_data['HIGH'])
        nifty_data['LOW'] = pd.to_numeric(nifty_data['LOW'])
        nifty_data['CLOSE'] = pd.to_numeric(nifty_data['CLOSE'])

        # Scan for the pattern from newest to oldest
        for i in range(len(nifty_data) - 1, 0, -1):
            t_minus_1 = nifty_data.iloc[i-1]
            t_day = nifty_data.iloc[i]

            # Criteria for Day T-1 (Breakout)
            is_breakout = (
                ((t_minus_1['CLOSE'] - t_minus_1['OPEN']) / t_minus_1['OPEN']) * 100 > 0.5 and
                (t_minus_1['CLOSE'] > t_minus_1['HIGH'] * 0.995) # Close is within 0.5% of the high
            )

            if is_breakout:
                # Criteria for Day T (Inside Day)
                is_inside_day = (
                    t_day['HIGH'] < t_minus_1['HIGH'] and
                    t_day['LOW'] > t_minus_1['LOW']
                )

                if is_inside_day:
                    proxy_date = t_day.name.date()
                    print(f"Proxy Date Found: {proxy_date}")
                    return

    except Exception as e:
        print(f"An error occurred: {e}")
        return

if __name__ == "__main__":
    temp_cache_dir = None
    try:
        temp_cache_dir = setup_temp_cache()
        find_proxy_date()
    finally:
        if temp_cache_dir:
            cleanup_temp_cache(temp_cache_dir)
            # print(f"Temporary cache directory '{temp_cache_dir}' cleaned up.")
