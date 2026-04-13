import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from admin import POSTGRES_PASS, ALPHA_VANTAGE_API_KEY
from news_collector_backdated import repair_ticker

ALL_TICKERS = ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "SPY", "QQQ", "GLD", "SLV", "USO"]
day_of_year = datetime.now().timetuple().tm_yday
ticker_index = day_of_year % len(ALL_TICKERS)
CURRENT_TICKER = ALL_TICKERS[ticker_index]

print(f"--- AUTOMATION: Today is day {day_of_year}. Target Ticker: {CURRENT_TICKER} ---")


DB_URL = f'postgresql://postgres:{POSTGRES_PASS}@localhost:5432/stock_market'
engine = create_engine(DB_URL)
CHECKPOINT_FILE = f"checkpoint_{CURRENT_TICKER}.txt"

def get_current_pointer():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            val = f.read().strip()
            return datetime.strptime(val, "%Y%m%dT%H%M")
    return datetime(2023, 1, 1)

def fetch_news_chunk(ticker, start_time, end_time):
    url = (f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT"
           f"&tickers={ticker}&time_from={start_time}&time_to={end_time}"
           f"&sort=EARLIEST&limit=1000&apikey={ALPHA_VANTAGE_API_KEY}")
    try:
        response = requests.get(url)
        data = response.json()
        if "Note" in data or "Information" in data:
            print(f"API Limit reached: {data.get('Note', data.get('Information'))}")
            return None
        if "feed" not in data:
            return pd.DataFrame()

        processed_news = []
        for item in data["feed"]:
            dt_obj = datetime.strptime(item.get("time_published"), "%Y%m%dT%H%M%S")
            processed_news.append({
                "title": item.get("title"),
                "time_published": dt_obj,
                "url": item.get("url"),
                "summary": item.get("summary"),
                "source": item.get("source"),
                "overall_sentiment_score": float(item.get("overall_sentiment_score", 0)),
                "overall_sentiment_label": item.get("overall_sentiment_label"),
                "ticker_sentiment": str(item.get("ticker_sentiment", [])),
                "topics": str(item.get("topics", [])) 
            })
        return pd.DataFrame(processed_news)
    except Exception as e:
        print(f"Request Error: {e}")
        return None


current_pointer = get_current_pointer()
end_goal = datetime.now()

for i in range(25): 
    if current_pointer >= end_goal:
        print(f"Backfill complete!")
        break
    
    # request 90 days at a time
    window_end = current_pointer + timedelta(days=90)
    t_from = current_pointer.strftime("%Y%m%dT%H%M")
    t_to = window_end.strftime("%Y%m%dT%H%M")
    
    print(f"Request {i+1}/25 | {CURRENT_TICKER} | {t_from} to {t_to}")
    
    df = fetch_news_chunk(CURRENT_TICKER, t_from, t_to)
    
    if df is None: break 
    
    if not df.empty:
        try:
            records = df.to_dict(orient='records')

            stmt = text("""
                INSERT INTO news_sentiment 
                (title, time_published, url, summary, source, overall_sentiment_score, overall_sentiment_label, ticker_sentiment, topics)
                VALUES (:title, :time_published, :url, :summary, :source, :overall_sentiment_score, :overall_sentiment_label, :ticker_sentiment, :topics)
                ON CONFLICT (url) DO NOTHING
            """)

            with engine.begin() as conn:
                conn.execute(stmt, records)

            if len(df) >= 1000:
                print(f"1000 API limit hit for {CURRENT_TICKER} | {t_from} → {t_to} | repairing...")

                repair_ticker(
                    CURRENT_TICKER,
                    current_pointer.strftime("%Y-%m-%d"),
                    window_end.strftime("%Y-%m-%d")
                )

            else:
                print(f"Successfully saved {len(df)} articles.")

            current_pointer = window_end

        except Exception as e:
            print(f"Database Error: {e}")
            break

    else:
        print(f"No news found for {CURRENT_TICKER}. Jumping to next window.")
        current_pointer = window_end
    
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(current_pointer.strftime("%Y%m%dT%H%M"))
    
    time.sleep(15) 

print(f"\nSession finished. Current progress for {CURRENT_TICKER}: {current_pointer}")