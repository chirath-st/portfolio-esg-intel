import sqlite3
import pandas as pd
from pathlib import Path

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR / "portfolio_esg.db"
PRICES_CSV = BASE_DIR / "data" / "prices_raw.csv"
SQL_PATH   = BASE_DIR / "sql" / "create_tables.sql"


def init_db(conn):
    with open(SQL_PATH) as f:
        conn.executescript(f.read())
    conn.commit()

    # DEBUG: show tables created
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table';"
    ).fetchall()
    print("Tables after init_db:", tables)

def load_instruments(conn, df):
    tickers = df["ticker"].unique()
    cur = conn.cursor()
    for t in tickers:
        cur.execute(
            "INSERT OR IGNORE INTO instruments (ticker) VALUES (?)",
            (t,)
        )
    conn.commit()


def load_prices(conn, df):
    cur = conn.cursor()
    tick_map = dict(conn.execute("SELECT ticker, id FROM instruments"))
    rows = [
        # SQLite does not understand pandas Timestamps; store ISO date strings.
        (tick_map[row["ticker"]], row["Date"].strftime("%Y-%m-%d"), row["adj_close"])
        for _, row in df.iterrows()
    ]
    cur.executemany(
        "INSERT OR REPLACE INTO prices (instrument_id, date, adj_close) "
        "VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()


def main():
    print("Using DB:", DB_PATH)
    print("Reading CSV:", PRICES_CSV)
    print("Using SQL:", SQL_PATH)

    df = pd.read_csv(PRICES_CSV, parse_dates=["Date"])
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    load_instruments(conn, df)
    load_prices(conn, df)
    conn.close()


if __name__ == "__main__":
    main()
