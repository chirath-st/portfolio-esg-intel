PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS instruments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS prices (
    instrument_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    adj_close REAL NOT NULL,
    PRIMARY KEY (instrument_id, date),
    FOREIGN KEY (instrument_id) REFERENCES instruments (id) ON DELETE CASCADE
);
