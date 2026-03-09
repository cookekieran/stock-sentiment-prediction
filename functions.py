from datetime import datetime, timedelta

def get_end_date(start_day, desired_timeframe_in_days=1):
    start_date = datetime.strptime(start_day, "%Y-%m-%d")
    end_date = start_date + timedelta(days=desired_timeframe_in_days)
    return start_date, end_date