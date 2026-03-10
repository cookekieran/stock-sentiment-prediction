import os
from dotenv import load_dotenv

load_dotenv()

def get_env_var(key):
    value = os.getenv(key)
    if value is None:
        raise EnvironmentError(f"{key} is not set.")
    return value

ALPACA_API_KEY=get_env_var("ALPACA_API_KEY")
ALPACA_SECRET_KEY=get_env_var("ALPACA_SECRET_CODE")


POSTGRES_PASS=get_env_var("POSTGRES_PASS")

DB_HOST=get_env_var("DB_HOST")
DB_USER=get_env_var("DB_USER")
DB_PASSWORD=get_env_var("DB_PASSWORD")

