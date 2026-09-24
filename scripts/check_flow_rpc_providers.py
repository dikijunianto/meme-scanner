"""Read-only, redacted chain identity preflight for Phase 2B endpoints."""
import _bootstrap  # noqa: F401
import asyncio
import json

import httpx
from websockets.asyncio.client import connect

from app.flow_providers import FlowProviders


async def check():
    providers=FlowProviders.load()
    results={}
    async with httpx.AsyncClient(timeout=20) as client:
        response=await client.post(providers.http,json={'jsonrpc':'2.0','id':1,'method':'eth_chainId','params':[]})
        response.raise_for_status()
        results['validation_http']=int(response.json()['result'],16)
    for name in ('publicnode','validation'):
        async with connect(providers.ws(name),open_timeout=20,max_size=65536,compression=None) as socket:
            await socket.send(json.dumps({'jsonrpc':'2.0','id':1,'method':'eth_chainId','params':[]}))
            results[name+'_wss']=int(json.loads(await asyncio.wait_for(socket.recv(),20))['result'],16)
    if set(results.values())!={4663}:raise ValueError('Flow provider chain identity mismatch')
    return {'chain_ids':results,'fingerprints':providers.fingerprints()}


if __name__=='__main__':
    try:print(json.dumps(asyncio.run(check()),indent=2))
    except Exception as exc:
        print(json.dumps({'preflight':'failed','error_type':type(exc).__name__}))
        raise SystemExit(1) from None
