import _bootstrap  # noqa: F401
from app.config import Config
from app.database import Database

if __name__ == "__main__":
    config = Config.load()
    db = Database(config.database, config.chain_id)
    print(f"Database initialized: {config.database}; journal_mode={db.conn.execute('PRAGMA journal_mode').fetchone()[0]}")
    db.close()
