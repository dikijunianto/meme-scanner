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

PHASE15_SCHEMA = """
CREATE TABLE IF NOT EXISTS coverage (
 kind TEXT NOT NULL, first INTEGER NOT NULL, last INTEGER NOT NULL CHECK(last>=first),
 PRIMARY KEY(kind,first)
);
CREATE TABLE IF NOT EXISTS rpc_usage (
 minute INTEGER NOT NULL, metric TEXT NOT NULL, count INTEGER NOT NULL,
 PRIMARY KEY(minute,metric)
);
CREATE TABLE IF NOT EXISTS graduations (
 id INTEGER PRIMARY KEY, tx_hash TEXT NOT NULL, log_index INTEGER NOT NULL,
 block_number INTEGER NOT NULL, block_timestamp TEXT NOT NULL,
 factory_address TEXT NOT NULL, token_address TEXT NOT NULL, quote_asset_address TEXT NOT NULL,
 curve_address TEXT NOT NULL, creator_address TEXT NOT NULL,
 pool_id TEXT NOT NULL, pool_manager_address TEXT NOT NULL, currency0 TEXT NOT NULL,
 currency1 TEXT NOT NULL, fee INTEGER NOT NULL, tick_spacing INTEGER NOT NULL, hooks TEXT NOT NULL,
 position_id TEXT NOT NULL, token_amount TEXT NOT NULL, quote_amount TEXT NOT NULL,
 raw_event_name TEXT NOT NULL, detected_at TEXT NOT NULL, UNIQUE(tx_hash,log_index)
);
CREATE INDEX IF NOT EXISTS graduations_block ON graduations(block_number);
CREATE INDEX IF NOT EXISTS graduations_token ON graduations(token_address);
CREATE INDEX IF NOT EXISTS graduations_pool ON graduations(pool_id);
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

    def migrate(self):
        # Explicit transaction includes DDL and state. Never use executescript
        # here: its implicit commit would leave a partially applied migration.
        with self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            for statement in PHASE15_SCHEMA.split(";"):
                if statement.strip():
                    self.conn.execute(statement)
            if self.state("phase15_migrated_at") is None:
                last = self.last()
                if last is not None:
                    self.set_state("historical_checkpoint", last)
                self.set_state("phase15_migrated_at", utc_now())
            if self.state("phase16_migrated_at") is None:
                self.set_state("phase16_migrated_at", utc_now())
                self.set_state("phase16_telemetry_started_at", utc_now())

    def state(self, key):
        row = self.conn.execute("SELECT value FROM chain_state WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set_state(self, key, value):
        self.conn.execute("INSERT INTO chain_state VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET "
                          "value=excluded.value, updated_at=excluded.updated_at", (key, str(value), utc_now()))

    def last(self, key="last_processed_block"):
        value = self.state(key)
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

    def stock_asset(self, address):
        row = self.conn.execute("SELECT symbol,name FROM stock_assets WHERE address=? AND verified=1", (address,)).fetchone()
        return dict(row) if row else None

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

    def record_coverage(self, kind, first, last):
        rows = self.conn.execute("SELECT first,last FROM coverage WHERE kind=? AND first<=? AND last>=?",
                                 (kind, last + 1, first - 1)).fetchall()
        if rows:
            first, last = min(first, min(r[0] for r in rows)), max(last, max(r[1] for r in rows))
            self.conn.execute("DELETE FROM coverage WHERE kind=? AND first<=? AND last>=?", (kind, last + 1, first - 1))
        self.conn.execute("INSERT INTO coverage VALUES(?,?,?)", (kind, first, last))

    def save_range(self, blocks, launches, graduations, first, last, cursor, retention, coverage_first=None):
        """Commit a completely validated range, its events and coverage together."""
        new_launches = new_stock = 0
        with self.conn:
            for b, items, grads in zip(blocks, launches, graduations):
                if b is not None:
                    self.conn.execute("INSERT OR REPLACE INTO blocks VALUES(?,?,?,?,?,?)",
                                      (b.number, b.hash, b.parent, b.timestamp, b.tx_count, utc_now()))
                for table, records in (("launches", items), ("graduations", grads)):
                    for record in records:
                        existing = self.conn.execute(f"SELECT block_number FROM {table} WHERE tx_hash=? AND log_index=?",
                                                     (record["tx_hash"], record["log_index"])).fetchone()
                        if existing and existing[0] != record["block_number"]:
                            raise ValueError("Event identity moved without canonical rollback")
                        fields = ",".join(record)
                        inserted = self.conn.execute(f"INSERT INTO {table}({fields}) VALUES({','.join('?' for _ in record)}) "
                                                     "ON CONFLICT(tx_hash,log_index) DO NOTHING", tuple(record.values())).rowcount
                        if table == "launches" and inserted:
                            new_launches += 1
                            new_stock += record["is_stock_quote"]
            self.record_coverage("live" if cursor == "live_checkpoint" else "backfill", coverage_first or first, last)
            if cursor:
                self.set_state(cursor, max(last, self.last(cursor) or 0))
            # Keep the original historical anchor and any headers needed by live reorg checks.
            ceiling = max(last, self.last("live_checkpoint") or 0)
            self.conn.execute("DELETE FROM blocks WHERE block_number<? AND block_number!=?",
                              (ceiling - retention + 1, self.last("historical_checkpoint") or -1))
        return new_launches, new_stock

    def record_gap(self, kind, first, last):
        if not first <= last:
            return
        with self.conn:
            self.record_coverage(kind, first, last)

    def rollback_to(self, number, cursor="last_processed_block"):
        with self.conn:
            if cursor == "live_checkpoint" and number < int(self.state("live_start_block")):
                raise ValueError("Live rollback cannot cross the preserved historical gap")
            self.conn.execute("DELETE FROM launches WHERE block_number>?", (number,))
            self.conn.execute("DELETE FROM blocks WHERE block_number>?", (number,))
            if cursor == "live_checkpoint":
                self.conn.execute("DELETE FROM graduations WHERE block_number>?", (number,))
                self.conn.execute("DELETE FROM coverage WHERE first>?", (number,))
                self.conn.execute("UPDATE coverage SET last=? WHERE last>?", (number, number))
            self.set_state(cursor, number)

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
