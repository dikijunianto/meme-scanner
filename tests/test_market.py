import asyncio
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock

from eth_abi import encode

from app.config import Config
from app.database import Database
from app.market import MarketResolver, curve_price, v4_price
from app.models import Block, utc_now
from app.telemetry import Telemetry
from test_scanner import FACTORY, QUOTE


class MarketTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); root=Path(self.tmp.name)
        self.config=Config("https://example.invalid",4663,(FACTORY,),"https://api.robinhood.com/rhj/assets",root/"db",root/"log",market_enabled=True)
        self.db=Database(self.config.database,4663); self.db.migrate(); self.telemetry=Telemetry(self.db)
        self.launch=dict(tx_hash="0x"+"11"*32,log_index=0,block_number=1,block_timestamp=utc_now(),factory_address=FACTORY,
                         token_address="0x"+"12"*20,token_symbol="T",token_name="T",quote_asset_address=QUOTE,quote_asset_symbol="Q",quote_asset_name="Q",
                         creator_address="0x"+"13"*20,curve_address="0x"+"14"*20,pair_or_pool_address=None,launch_type="pons-v2",is_stock_quote=1,
                         raw_event_name="TokenLaunched",launch_config_id="1",graduation_threshold="1",detected_at=utc_now())
        self.db.save_block(Block(1,"0x"+"01"*32,"0x"+"00"*32,utc_now(),0),[self.launch],100)

    def tearDown(self): self.db.close(); self.tmp.cleanup()

    def test_curve_and_v4_price_math(self):
        self.assertEqual(Decimal(curve_price(2*10**18,10**18,18)),Decimal(2))
        self.assertEqual(Decimal(v4_price(2**96,True,18,18)),Decimal(1))
        self.assertEqual(Decimal(v4_price(2**96,False,18,18)),Decimal(1))
        self.assertIsNone(v4_price(0,True,18,18))

    def test_targets_idempotent_and_unbiased(self):
        first,long=self.db.schedule_market_targets([self.launch],.05,.05)
        self.db.schedule_market_targets([self.launch],.05,.05)
        count=self.db.conn.execute("select count(*) from outcome_targets").fetchone()[0]
        self.assertEqual(count,6 if long else (2 if first else 1))

    async def test_curve_snapshot_caches_static_calls(self):
        rpc=AsyncMock(); rpc.batch.side_effect=[["0x"+encode(["uint256","uint256"],[2_000_000,1_000_000]).hex(),"0x"+encode(["uint256"],[1_000_000]).hex()],
                                                   ["0x"+encode(["uint8"],[6]).hex()], ["0x"+encode(["uint256"],[1_000_000_000_000_000_000]).hex()]]
        target={"token_address":self.launch["token_address"],"quote_asset_address":QUOTE,"curve_address":self.launch["curve_address"]}
        value=await MarketResolver(self.config,rpc,self.db,self.telemetry).curve(target)
        self.assertEqual(value["market_phase"],"curve")
        self.assertIsNotNone(value["price_quote"])

    async def test_v4_snapshot_uses_state_library_slots(self):
        rpc=AsyncMock(); rpc.batch.side_effect=[["0x"+(2**96).to_bytes(32,"big").hex(),"0x"+(7).to_bytes(32,"big").hex()],
                                                   ["0x"+encode(["uint8"],[18]).hex()], ["0x"+encode(["uint256"],[10**18]).hex()]]
        target={"token_address":self.launch["token_address"],"quote_asset_address":QUOTE,"curve_address":self.launch["curve_address"]}
        grad={"pool_id":"0x"+"33"*32,"pool_manager_address":"0x"+"34"*20,"currency0":self.launch["token_address"]}
        value=await MarketResolver(self.config,rpc,self.db,self.telemetry).v4(target,grad)
        self.assertEqual((value["market_phase"],Decimal(value["price_quote"]),value["v4_active_liquidity"]),("v4",Decimal(1),"7"))

    def test_migration_idempotent(self):
        self.db.migrate(); self.db.migrate()
        self.assertEqual(self.db.conn.execute("pragma integrity_check").fetchone()[0],"ok")


if __name__=="__main__": unittest.main()
