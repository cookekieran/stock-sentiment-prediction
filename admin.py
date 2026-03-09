import os
from dotenv import load_dotenv

load_dotenv()

ALPACA_API_KEY=os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY=os.getenv("ALPACA_SECRET_CODE")

POSTGRES_PASS=os.getenv("POSTGRES_PASS")

DB_HOST=os.getenv("DB_HOST")
DB_USER=os.getenv("DB_USER")
DB_PASSWORD=os.getenv("DB_PASSWORD")