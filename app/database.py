import sqlite3

from app.models import Block, utc_now

SCHEMA = """
CREATE TABLE IF NOT EXISTS chain_state (
 key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS blocks (
 block_number INTEGER PRIMARY KEY, block_hash TEXT NOT NULL, parent_hash TEXT NOT NULL,
 timestamp TEXT NOT NULL, tx_count INTEGER NOT NULL, processed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS launches (
 id INTEGER PRIMARY KEY, tx_hash TEXT NOT NULL, log_index INTEGER NOT NULL,
 block_number INTEGER NOT NULL, block_timestamp TEXT NOT NULL,
 factory_address TEXT NOT NULL, token_address TEXT NOT NULL,
 token_symbol TEXT, token_name TEXT, quote_asset_address TEXT NOT NULL,
 quote_asset_symbol TEXT, quote_asset_name TEXT, creator_address TEXT,
 curve_address TEXT, pair_or_pool_address TEXT, launch_type TEXT NOT NULL,
 is_stock_quote INTEGER NOT NULL CHECK (is_stock_quote IN (0,1)),
 raw_event_name TEXT NOT NULL, launch_config_id TEXT, graduation_threshold TEXT,
 detected_at TEXT NOT NULL, UNIQUE(tx_hash, log_index)
);
CREATE TABLE IF NOT EXISTS stock_assets (
 address TEXT PRIMARY KEY, symbol TEXT, name TEXT, stock_ticker TEXT,
 source TEXT NOT NULL, verified INTEGER NOT NULL CHECK (verified IN (0,1)), updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS launches_block ON launches(block_number);
CREATE INDEX IF NOT EXISTS launches_token ON launches(token_address);
CREATE INDEX IF NOT EXISTS launches_quote ON launches(quote_asset_address);
CREATE INDEX IF NOT EXISTS launches_stock ON launches(is_stock_quote);
CREATE INDEX IF NOT EXISTS launches_detected ON launches(detected_at);
"""


class Database:
    def __init__(self, path, chain_id):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, timeout=10)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(SCHEMA)
        if self.state("chain_id") not in (None, str(chain_id)):
            self.close()
            raise ValueError("Database belongs to another chain")
        with self.conn:
            self.set_state("chain_id", chain_id)

    def close(self):
        self.conn.close()

    def state(self, key):
        row = self.conn.execute("SELECT value FROM chain_state WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set_state(self, key, value):
        self.conn.execute("INSERT INTO chain_state VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET "
                          "value=excluded.value, updated_at=excluded.updated_at", (key, str(value), utc_now()))

    def last(self):
        value = self.state("last_processed_block")
        return int(value) if value is not None else None

    def block_hash(self, number):
        row = self.conn.execute("SELECT block_hash FROM blocks WHERE block_number=?", (number,)).fetchone()
        return row[0] if row else None

    def block(self, number):
        row = self.conn.execute("SELECT block_number,block_hash,parent_hash,timestamp,tx_count "
                                "FROM blocks WHERE block_number=?", (number,)).fetchone()
        return Block(*row) if row else None

    def launch(self, tx_hash, log_index):
        row = self.conn.execute("SELECT * FROM launches WHERE tx_hash=? AND log_index=?",
                                (tx_hash, log_index)).fetchone()
        if row:
            result = dict(row)
            result.pop("id")
            return result

    def is_stock(self, address):
        return self.conn.execute("SELECT 1 FROM stock_assets WHERE address=? AND verified=1", (address,)).fetchone() is not None

    def save_block(self, block, launches, retention):
        # One small SQLite transaction: events and checkpoint succeed together.
        with self.conn:
            previous = {tuple(row[:2]): row[2] for row in self.conn.execute(
                "SELECT tx_hash,log_index,detected_at FROM launches WHERE block_number=?", (block.number,))}
            self.conn.execute("DELETE FROM launches WHERE block_number=?", (block.number,))
            self.conn.execute("INSERT OR REPLACE INTO blocks VALUES(?,?,?,?,?,?)",
                              (block.number, block.hash, block.parent, block.timestamp, block.tx_count, utc_now()))
            for item in launches:
                launch = dict(item)
                launch["detected_at"] = previous.get((launch["tx_hash"], launch["log_index"]), launch["detected_at"])
                fields = ",".join(launch)
                self.conn.execute(f"INSERT INTO launches({fields}) VALUES({','.join('?' for _ in launch)}) "
                                  "ON CONFLICT(tx_hash,log_index) DO NOTHING", tuple(launch.values()))
            self.set_state("last_processed_block", max(block.number, self.last() or 0))
            self.conn.execute("DELETE FROM blocks WHERE block_number < ?", (block.number - retention + 1,))

    def rollback_to(self, number):
        with self.conn:
            self.conn.execute("DELETE FROM launches WHERE block_number>?", (number,))
            self.conn.execute("DELETE FROM blocks WHERE block_number>?", (number,))
            self.set_state("last_processed_block", number)

    def replace_assets(self, assets, source):
        with self.conn:
            self.conn.execute("DELETE FROM stock_assets WHERE source=?", (source,))
            self.conn.executemany("INSERT INTO stock_assets VALUES(?,?,?,?,?,?,?) "
                                  "ON CONFLICT(address) DO UPDATE SET symbol=excluded.symbol,name=excluded.name,"
                                  "stock_ticker=excluded.stock_ticker,source=excluded.source,"
                                  "verified=excluded.verified,updated_at=excluded.updated_at", assets)
            self.conn.execute("UPDATE launches SET is_stock_quote=EXISTS(SELECT 1 FROM stock_assets "
                              "WHERE stock_assets.address=launches.quote_asset_address AND verified=1)")
            self.set_state("stock_assets_synced_at", utc_now())
