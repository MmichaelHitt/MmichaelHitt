import asyncio
import json
import time
import hmac
import hashlib
import base64
import gzip
import ssl
from datetime import datetime
from typing import Dict, Optional, List
from urllib.parse import urlencode
import websockets
import aiohttp

# SSL-контекст без верификации (решает ошибку CERTIFICATE_VERIFY_FAILED на Windows)
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================
HTX_CFG = {
    'api_key': 'afwo04df3f-5ff673dc-1a5fbdc4-88373',
    'secret':  '9443adb6-aca9a178-899220dc-61ed1',
}
OKX_CFG = {
    'api_key':  'c28ccc03-2852-4928-9826-ed8a0aaa5540',
    'secret':   '9102588F22EEABE6422A4E9195C4EF0E',
    'password': 'HGCctc435rfg9(',
}

OKX_SYMBOLS = [
    '1INCH-USDT-SWAP', '2Z-USDT-SWAP', 'A-USDT-SWAP', 'ALGO-USDT-SWAP', 'APT-USDT-SWAP',
    'ARB-USDT-SWAP', 'ASTER-USDT-SWAP', 'AVNT-USDT-SWAP', 'BIGTIME-USDT-SWAP',
    'BLUR-USDT-SWAP', 'BOME-USDT-SWAP', 'CFX-USDT-SWAP', 'CHZ-USDT-SWAP', 'COMP-USDT-SWAP',
    'CORE-USDT-SWAP', 'DOT-USDT-SWAP', 'EIGEN-USDT-SWAP', 'ENA-USDT-SWAP', 'ENS-USDT-SWAP',
    'ENSO-USDT-SWAP', 'ETHFI-USDT-SWAP', 'FIL-USDT-SWAP', 'GALA-USDT-SWAP', 'GIGGLE-USDT-SWAP',
    'GMT-USDT-SWAP', 'GRASS-USDT-SWAP', 'GRT-USDT-SWAP', 'H-USDT-SWAP', 'HBAR-USDT-SWAP',
    'HYPE-USDT-SWAP', 'ICP-USDT-SWAP', 'IMX-USDT-SWAP', 'INJ-USDT-SWAP', 'IP-USDT-SWAP',
    'JUP-USDT-SWAP', 'KAITO-USDT-SWAP', 'LA-USDT-SWAP', 'LDO-USDT-SWAP', 'LIGHT-USDT-SWAP',
    'LINK-USDT-SWAP', 'LIT-USDT-SWAP', 'LPT-USDT-SWAP', 'LTC-USDT-SWAP', 'LUNA-USDT-SWAP',
    'MANA-USDT-SWAP', 'MASK-USDT-SWAP', 'MEME-USDT-SWAP', 'MERL-USDT-SWAP', 'MOODENG-USDT-SWAP',
    'NEAR-USDT-SWAP', 'ONDO-USDT-SWAP', 'OP-USDT-SWAP', 'ORDI-USDT-SWAP', 'PENDLE-USDT-SWAP',
    'PENGU-USDT-SWAP', 'PI-USDT-SWAP', 'PNUT-USDT-SWAP', 'POPCAT-USDT-SWAP', 'POL-USDT-SWAP',
    'PUMP-USDT-SWAP', 'PYTH-USDT-SWAP', 'RESOLV-USDT-SWAP', 'SEI-USDT-SWAP', 'SNX-USDT-SWAP',
    'SPX-USDT-SWAP', 'STRK-USDT-SWAP', 'SUI-USDT-SWAP', 'SUSHI-USDT-SWAP', 'TIA-USDT-SWAP',
    'TON-USDT-SWAP', 'TRB-USDT-SWAP', 'TRUMP-USDT-SWAP', 'TURBO-USDT-SWAP', 'VIRTUAL-USDT-SWAP',
    'W-USDT-SWAP', 'WCT-USDT-SWAP', 'WET-USDT-SWAP', 'WIF-USDT-SWAP', 'WLD-USDT-SWAP',
    'WLFI-USDT-SWAP', 'XAG-USDT-SWAP', 'XLM-USDT-SWAP', 'XPL-USDT-SWAP', 'YB-USDT-SWAP',
    'YFI-USDT-SWAP', 'ZBT-USDT-SWAP', 'ZEN-USDT-SWAP', 'ZK-USDT-SWAP', 'ZKP-USDT-SWAP'
]

SPREAD_THRESHOLD   = 0.004
ORDER_USDT         = 1.0
MIN_PRICE_AGE_SEC  = 300
MAX_PRICE_SYNC_SEC = 150
LEVER              = 10
POSITION_POLL_SEC  = 30
SIGNAL_DELAY_MS    = 500

_HTX_PONG_CACHE: Dict[int, str] = {}
_OKX_PONG_STR = 'pong'

def okx_to_htx(sym: str) -> str:
    return sym.replace('-SWAP', '')


# ============================================================
# HTX
# ============================================================
class HTXTrader:
    ORDER_PATH = '/linear-swap-api/v1/swap_order'
    BASE_URL   = 'https://api.hbdm.vn'
    INFO_URL   = 'https://api.hbdm.vn/linear-swap-api/v1/swap_contract_info'
    POS_URL    = 'https://api.hbdm.vn/linear-swap-api/v1/swap_position_info'
    WS_PUB_URL = 'wss://api.hbdm.com/linear-swap-ws'
    WS_PRV_URL = 'wss://api.hbdm.com/linear-swap-notification'

    def __init__(self, api_key: str, secret: str):
        self.api_key       = api_key
        self.secret        = secret
        self._hmac_key     = secret.encode()
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws_pub        = None
        self.ws_prv        = None
        self.running       = False
        self.prv_authed    = False
        self.price_cache: Dict[str, dict] = {}
        self.order_futures: Dict[str, asyncio.Future] = {}
        self.ws_place_futures: Dict[str, asyncio.Future] = {}
        self.multipliers: Dict[str, float] = {}
        self.symbols: List[str] = []
        self.on_tick       = None
        self._cid_counter  = 0
        self._kyc_required = False
        self._sig_cache_ts = ''
        self._sig_cache_qp: dict = {}
        self._order_url    = self.BASE_URL + self.ORDER_PATH
        self._order_hdrs   = {'Content-Type': 'application/json'}

    async def fetch_multipliers(self, symbols: List[str]):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as s:
            async with s.get(self.INFO_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = json.loads(await r.text())
        if data.get('status') == 'ok':
            for c in data['data']:
                code = c.get('contract_code')
                if code in symbols:
                    self.multipliers[code] = float(c.get('contract_size', 1))
            print(f"✅ [HTX] Мультипликаторы: {len(self.multipliers)} символов")
        else:
            print(f"⚠️  [HTX] Мультипликаторы: {data}")

    def calc_contracts(self, symbol: str, price: float) -> int:
        cs = self.multipliers.get(symbol, 1.0)
        return max(1, round(ORDER_USDT / (price * cs)))

    def _next_cid(self) -> int:
        self._cid_counter = (self._cid_counter + 1) % 1000
        base = int(time.time()) % 2_000_000
        cid  = base * 1000 + self._cid_counter
        return cid if cid > 0 else 1

    async def _warmup_http(self):
        try:
            params_w = {
                'contract_code': 'BTC-USDT', 'client_order_id': 1,
                'direction': 'buy', 'offset': 'open',
                'lever_rate': 1, 'volume': 0,
                'order_price_type': 'opponent_ioc',
            }
            await self._http_post(params_w)
        except Exception:
            pass
        try:
            url = self.BASE_URL + '/linear-swap-ex/market/detail/merged'
            async with self.session.get(
                url, params={'contract_code': 'BTC-USDT'},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                await r.read()
        except Exception:
            pass

    async def connect(self, symbols: List[str]):
        self.symbols = symbols
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=10, ttl_dns_cache=300, keepalive_timeout=60,
                enable_cleanup_closed=True, ssl=False,
            ),
            timeout=aiohttp.ClientTimeout(
                total=2.0, connect=1.0, sock_connect=1.0, sock_read=1.5,
            ))

        self.ws_pub = await websockets.connect(
            self.WS_PUB_URL, ping_interval=20, ping_timeout=10,
            max_size=10**7, compression=None, ssl=_SSL_CTX)
        self.running = True
        asyncio.create_task(self._pub_loop())
        for s in symbols:
            await self.ws_pub.send(json.dumps({'sub': f'market.{s}.bbo', 'id': s}))
        print("✅ [HTX] Public WS")

        self.ws_prv = await websockets.connect(
            self.WS_PRV_URL, ping_interval=20, ping_timeout=10,
            max_size=10**7, compression=None, ssl=_SSL_CTX)
        await self._auth_prv()
        asyncio.create_task(self._prv_loop())
        asyncio.create_task(self._presign_loop())
        for s in symbols:
            await self.ws_prv.send(json.dumps({'op': 'sub', 'topic': f'orders.{s}'}))
        print("✅ [HTX] Private WS")

    async def _auth_prv(self):
        ts     = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        ts_enc = ts.replace(':', '%3A')
        msg    = (f"GET\napi.hbdm.com\n/linear-swap-notification\n"
                  f"AccessKeyId={self.api_key}&SignatureMethod=HmacSHA256"
                  f"&SignatureVersion=2&Timestamp={ts_enc}")
        sig = base64.b64encode(
            hmac.new(self._hmac_key, msg.encode(), hashlib.sha256).digest()).decode()
        await self.ws_prv.send(json.dumps({
            'op': 'auth', 'type': 'api', 'AccessKeyId': self.api_key,
            'SignatureMethod': 'HmacSHA256', 'SignatureVersion': '2',
            'Timestamp': ts, 'Signature': sig,
        }))
        for _ in range(15):
            d = self._decode(await asyncio.wait_for(self.ws_prv.recv(), timeout=5))
            if d and d.get('op') == 'ping':
                await self.ws_prv.send(json.dumps({'op': 'pong', 'ts': d['ts']}))
            elif d and d.get('op') == 'auth':
                if d.get('err-code') == 0:
                    self.prv_authed = True
                    return
                raise Exception(f"HTX auth fail: {d}")
        raise Exception("HTX auth timeout")

    def _decode(self, raw) -> Optional[dict]:
        if isinstance(raw, bytes):
            try:
                raw = gzip.decompress(raw).decode()
            except Exception:
                raw = raw.decode('utf-8', errors='ignore')
        try:
            return json.loads(raw)
        except Exception:
            return None

    async def _pub_loop(self):
        while self.running:
            try:
                while self.running:
                    d = self._decode(await self.ws_pub.recv())
                    if not d:
                        continue
                    if d.get('ping') is not None:
                        ts_val = d['ping']
                        pong = _HTX_PONG_CACHE.get(ts_val)
                        if pong is None:
                            pong = json.dumps({'pong': ts_val})
                            _HTX_PONG_CACHE.clear()
                            _HTX_PONG_CACHE[ts_val] = pong
                        await self.ws_pub.send(pong)
                        continue
                    ch = d.get('ch', '')
                    if 'bbo' in ch and d.get('tick'):
                        try:
                            sym  = ch.split('.')[1]
                            tick = d['tick']
                            raw_bid = tick.get('bid', [0, 0])
                            raw_ask = tick.get('ask', [0, 0])
                            bid     = float(raw_bid[0]) if isinstance(raw_bid, list) else float(raw_bid)
                            ask     = float(raw_ask[0]) if isinstance(raw_ask, list) else float(raw_ask)
                            bid_sz  = float(raw_bid[1]) if isinstance(raw_bid, list) and len(raw_bid) > 1 else float(tick.get('bidSize', 0))
                            ask_sz  = float(raw_ask[1]) if isinstance(raw_ask, list) and len(raw_ask) > 1 else float(tick.get('askSize', 0))
                            if bid > 0 and ask > 0 and bid < ask:
                                prev = self.price_cache.get(sym, {})
                                self.price_cache[sym] = {'bid': bid, 'ask': ask, 'ts': time.time(),
                                                         'bid_sz': bid_sz if bid_sz > 0 else prev.get('bid_sz', 0),
                                                         'ask_sz': ask_sz if ask_sz > 0 else prev.get('ask_sz', 0)}
                                if self.on_tick:
                                    self.on_tick(sym)
                        except Exception:
                            pass
            except websockets.exceptions.ConnectionClosed:
                if not self.running:
                    return
                print("⚠️  [HTX] Pub WS обрыв, переподключение...")
                await asyncio.sleep(2)
                try:
                    self.ws_pub = await websockets.connect(
                        self.WS_PUB_URL, ping_interval=20, ping_timeout=10,
                        max_size=10**7, compression=None, ssl=_SSL_CTX)
                    for s in self.symbols:
                        await self.ws_pub.send(json.dumps({'sub': f'market.{s}.bbo', 'id': s}))
                except Exception as e:
                    print(f"❌ [HTX] Pub reconnect: {e}")
            except Exception as e:
                if self.running:
                    print(f"❌ [HTX] pub loop: {e}")
                    await asyncio.sleep(1)

    async def _prv_loop(self):
        while self.running:
            try:
                while self.running:
                    d = self._decode(await self.ws_prv.recv())
                    if not d:
                        continue
                    op    = d.get('op', '')
                    topic = d.get('topic', '')
                    if op == 'ping':
                        await self.ws_prv.send(json.dumps({'op': 'pong', 'ts': d['ts']}))
                        continue
                    if op == 'place':
                        cid_p = str(d.get('cid', ''))
                        f_p   = self.ws_place_futures.get(cid_p)
                        if f_p and not f_p.done():
                            f_p.set_result(d)
                        self.ws_place_futures.pop(cid_p, None)
                    elif op == 'notify' and 'orders' in topic:
                        cid_raw = d.get('client_order_id', '')
                        if not cid_raw:
                            continue
                        cid = str(cid_raw)
                        f   = self.order_futures.get(cid)
                        if f is not None:
                            if not f.done():
                                f.set_result(d)
                            self.order_futures.pop(cid, None)
                        elif self.order_futures:
                            print(f"⚠️  [HTX] notify cid={cid!r} не в futures")
                    elif op == 'error':
                        print(f"❌ [HTX] prv error: {d.get('err-code')} {d.get('err-msg')}")
                    elif op not in ('auth', 'sub', 'ping', ''):
                        print(f"⚠️  [HTX] prv unknown op={op!r} topic={topic!r}")
            except websockets.exceptions.ConnectionClosed:
                for f in self.order_futures.values():
                    if not f.done():
                        f.set_exception(Exception("HTX prv WS обрыв"))
                self.order_futures.clear()
                for f in self.ws_place_futures.values():
                    if not f.done():
                        f.set_exception(Exception("HTX prv WS обрыв"))
                self.ws_place_futures.clear()
                if not self.running:
                    return
                print("⚠️  [HTX] Prv WS обрыв, переподключение...")
                await asyncio.sleep(2)
                try:
                    self.ws_prv = await websockets.connect(
                        self.WS_PRV_URL, ping_interval=20, ping_timeout=10,
                        max_size=10**7, compression=None, ssl=_SSL_CTX)
                    await self._auth_prv()
                    for s in self.symbols:
                        await self.ws_prv.send(json.dumps({'op': 'sub', 'topic': f'orders.{s}'}))
                except Exception as e:
                    print(f"❌ [HTX] Prv reconnect: {e}")
            except Exception as e:
                if self.running:
                    print(f"❌ [HTX] prv loop: {e}")
                    await asyncio.sleep(1)

    def get_price(self, symbol: str) -> Optional[dict]:
        c = self.price_cache.get(symbol)
        if c and (time.time() - c['ts']) < MIN_PRICE_AGE_SEC:
            return c
        return None

    def _build_sig(self, ts: str) -> dict:
        qp  = {'AccessKeyId': self.api_key, 'SignatureMethod': 'HmacSHA256',
               'SignatureVersion': '2', 'Timestamp': ts}
        qs  = urlencode(sorted(qp.items()))
        qp['Signature'] = base64.b64encode(
            hmac.new(self._hmac_key,
                     f"POST\napi.hbdm.vn\n{self.ORDER_PATH}\n{qs}".encode(),
                     hashlib.sha256).digest()).decode()
        return qp

    def _build_get_sig(self, path: str, ts: str) -> dict:
        host = 'api.hbdm.com'
        qp   = {'AccessKeyId': self.api_key, 'SignatureMethod': 'HmacSHA256',
                'SignatureVersion': '2', 'Timestamp': ts}
        qs   = urlencode(sorted(qp.items()))
        qp['Signature'] = base64.b64encode(
            hmac.new(self._hmac_key,
                     f"GET\n{host}\n{path}\n{qs}".encode(),
                     hashlib.sha256).digest()).decode()
        return qp

    async def _query_order_by_cid(self, contract_code: str, client_order_id: str) -> dict:
        host = 'api.hbdm.com'
        path = '/linear-swap-api/v1/swap_order_info'
        ts   = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        body = json.dumps({'contract_code': contract_code,
                           'client_order_id': int(client_order_id)},
                          separators=(',', ':'))
        qp  = {'AccessKeyId': self.api_key, 'SignatureMethod': 'HmacSHA256',
               'SignatureVersion': '2', 'Timestamp': ts}
        qs  = urlencode(sorted(qp.items()))
        qp['Signature'] = base64.b64encode(
            hmac.new(self._hmac_key,
                     f"POST\n{host}\n{path}\n{qs}".encode(),
                     hashlib.sha256).digest()).decode()
        try:
            async with self.session.post(
                f'https://{host}{path}',
                params=qp, data=body.encode(),
                headers={'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                text = await r.text()
                try:
                    return json.loads(text)
                except Exception:
                    return {'status': 'error', 'err-msg': f'non-JSON: {text[:120]}'}
        except Exception as e:
            return {'status': 'error', 'err-msg': str(e)}

    async def _presign_loop(self):
        """Обновляет REST-подпись каждые 100мс — минимизирует задержку на REST fallback."""
        while self.running:
            ts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
            if ts != self._sig_cache_ts:
                self._sig_cache_ts = ts
                self._sig_cache_qp = self._build_sig(ts)
            await asyncio.sleep(0.1)

    async def _ws_place(self, cid: int, symbol: str, direction: str,
                        offset: str, volume: int) -> dict:
        """Размещает ордер через уже открытый приватный WS (без HTTP overhead)."""
        cid_str   = str(cid)
        place_fut = asyncio.Future()
        self.ws_place_futures[cid_str] = place_fut
        msg = json.dumps({
            'op':               'place',
            'cid':              cid_str,
            'contract_code':    symbol,
            'contract_type':    'swap',
            'client_order_id':  cid,
            'volume':           volume,
            'direction':        direction,
            'offset':           offset,
            'lever_rate':       1,
            'order_price_type': 'opponent_ioc',
        })
        await self.ws_prv.send(msg)
        try:
            return await asyncio.wait_for(place_fut, timeout=1.5)
        except asyncio.TimeoutError:
            self.ws_place_futures.pop(cid_str, None)
            raise

    async def _http_post(self, params: dict) -> dict:
        ts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        if ts != self._sig_cache_ts:
            self._sig_cache_ts = ts
            self._sig_cache_qp = self._build_sig(ts)
        qp   = {**self._sig_cache_qp}
        body = json.dumps(params, separators=(',', ':')).encode()
        async with self.session.post(
            self._order_url, params=qp, data=body, headers=self._order_hdrs
        ) as r:
            return json.loads(await r.read())

    async def open_position(self, symbol: str, side: str, volume: int):
        cid_str = None
        t_start = time.perf_counter()
        try:
            cid        = self._next_cid()
            cid_str    = str(cid)
            notify_fut = asyncio.Future()
            self.order_futures[cid_str] = notify_fut

            # ── Fast path: WS ордер (без HTTP overhead) ───────────────────
            ws_ok       = False
            http_result = None
            try:
                ws_ack = await self._ws_place(cid, symbol, side, 'open', volume)
                err_c  = ws_ack.get('err-code', 0)
                if err_c != 0:
                    self.order_futures.pop(cid_str, None)
                    err_msg = ws_ack.get('err-msg', '')
                    print(f"❌ [HTX] WS отклонён [{err_c}]: {err_msg}")
                    if 'margin' in str(err_msg).lower() or 'Insufficient' in str(err_msg):
                        print(f"\n🛑 СТОП: Недостаточно маржи — бот остановлен!")
                        raise SystemExit(1)
                    if 'kyc' in str(err_msg).lower() or 'identity' in str(err_msg).lower():
                        print(f"\n🛑 HTX требует KYC верификацию [{err_c}]")
                        self._kyc_required = True
                        return False, 0.0, (time.perf_counter()-t_start)*1000, True
                    return False, 0.0, (time.perf_counter()-t_start)*1000, True
                ws_ok  = True
                oid_ws = ws_ack.get('data', {}).get('order_id', '?')
                t_ws   = (time.perf_counter() - t_start) * 1000
                print(f"   [HTX] WS принят id={oid_ws} ({t_ws:.0f}ms), ждём исполнения...")
            except SystemExit:
                raise
            except Exception as ws_err:
                self.ws_place_futures.pop(cid_str, None)
                print(f"   [HTX] WS недоступен ({ws_err.__class__.__name__}), fallback REST...")

            # ── Slow path: REST HTTP ───────────────────────────────────────
            if not ws_ok:
                params = {
                    'contract_code':    symbol,
                    'client_order_id':  cid,
                    'direction':        side,
                    'offset':           'open',
                    'lever_rate':       1,
                    'volume':           volume,
                    'order_price_type': 'opponent_ioc',
                }
                http_task   = asyncio.create_task(self._http_post(params))
                notify_task = asyncio.create_task(asyncio.shield(notify_fut))
                done, pending = await asyncio.wait(
                    [http_task, notify_task], timeout=5.0, return_when=asyncio.FIRST_COMPLETED)
                ws_data_rest = None
                if http_task in done:
                    try: http_result = http_task.result()
                    except Exception as e: print(f"❌ [HTX] HTTP error: {e}")
                if notify_task in done:
                    try: ws_data_rest = notify_task.result()
                    except Exception: pass

                if http_result is not None and http_result.get('status') != 'ok':
                    for t in pending: t.cancel()
                    self.order_futures.pop(cid_str, None)
                    err_code = http_result.get('err-code') or ''
                    err_msg  = http_result.get('err-msg') or str(http_result)
                    print(f"❌ [HTX] REST отклонён [{err_code}]: {err_msg}")
                    if 'margin' in str(err_msg).lower() or 'Insufficient' in str(err_msg):
                        print(f"\n🛑 СТОП: Недостаточно маржи — бот остановлен!")
                        raise SystemExit(1)
                    if 'kyc' in str(err_msg).lower() or 'identity' in str(err_msg).lower():
                        print(f"\n🛑 HTX требует KYC верификацию [{err_code}]")
                        self._kyc_required = True
                        return False, 0.0, (time.perf_counter()-t_start)*1000, True
                    return False, 0.0, (time.perf_counter()-t_start)*1000, True

                if http_result is not None and http_result.get('status') == 'ok':
                    oid = http_result.get('data', {}).get('order_id', '?')
                    print(f"   [HTX] REST принят id={oid} ({(time.perf_counter()-t_start)*1000:.0f}ms)...")

                if ws_data_rest is None:
                    try:
                        elapsed_so_far = time.perf_counter() - t_start
                        ws_data_rest = await asyncio.wait_for(
                            notify_fut, timeout=max(0.05, 0.3 - elapsed_so_far + 0.127))
                    except asyncio.TimeoutError:
                        pass
                for t in pending: t.cancel()
                self.order_futures.pop(cid_str, None)
                return await self._rest_poll_or_parse(
                    ws_data_rest, symbol, side, volume, cid_str, t_start, http_result, 'REST')

            # ── WS path: ждём fill-notify ──────────────────────────────────
            ws_data = None
            try:
                ws_data = await asyncio.wait_for(notify_fut, timeout=2.5)
            except asyncio.TimeoutError:
                pass
            finally:
                self.order_futures.pop(cid_str, None)
            return await self._rest_poll_or_parse(
                ws_data, symbol, side, volume, cid_str, t_start, None, 'WS')

        except SystemExit:
            raise
        except Exception as e:
            if cid_str:
                self.order_futures.pop(cid_str, None)
                self.ws_place_futures.pop(cid_str, None)
            print(f"❌ [HTX] open_position: {e}")
            return False, 0.0, (time.perf_counter()-t_start)*1000, False

    async def _rest_poll_or_parse(self, ws_data, symbol, side, volume,
                                   cid_str, t_start, http_result, mode):
        """Разбирает notify или делает REST-поллинг при отсутствии notify."""
        if ws_data is not None:
            trade_vol  = float(ws_data.get('trade_volume', 0) or 0)
            err_code   = ws_data.get('err_code', 0)
            status     = ws_data.get('status', '')
            order_id   = ws_data.get('order_id', '?')
            if err_code and err_code != 0:
                err_msg_ws = ws_data.get('err_msg', '')
                print(f"❌ [HTX] err={err_code}: {err_msg_ws}")
                if 'margin' in str(err_msg_ws).lower() or 'Insufficient' in str(err_msg_ws):
                    print(f"\n🛑 СТОП: Недостаточно маржи (WS) — бот остановлен!")
                    raise SystemExit(1)
                return False, 0.0, (time.perf_counter()-t_start)*1000, True
            if trade_vol > 0:
                pct        = trade_vol / volume * 100
                fill_price = float(ws_data.get('trade_avg_price', 0) or 0)
                elapsed_ms = (time.perf_counter() - t_start) * 1000
                print(f"✅ [HTX] {side} {symbol} [{mode}] | id={order_id} | "
                      f"filled={trade_vol}/{volume} ({pct:.0f}%) | avg={fill_price} | {elapsed_ms:.0f}ms")
                return True, fill_price, elapsed_ms, False
            reason = "нет ликвидности" if status in (7, '7') else f"status={status}"
            print(f"❌ [HTX] НЕ ИСПОЛНЕН {side} {symbol} | {reason}")
            return False, 0.0, (time.perf_counter()-t_start)*1000, True

        if http_result and http_result.get('status') == 'ok':
            print(f"⏳ [HTX] notify нет — опрашиваем REST...")
            for attempt in range(1, 9):
                if attempt > 1:
                    await asyncio.sleep(0.15 if attempt <= 3 else 0.4)
                rest_data = await self._query_order_by_cid(symbol, cid_str)
                elapsed   = (time.perf_counter() - t_start) * 1000
                if rest_data.get('status') != 'ok': continue
                orders = rest_data.get('data', {})
                if isinstance(orders, list) and orders: orders = orders[0]
                if not isinstance(orders, dict): continue
                trade_vol_r  = float(orders.get('trade_volume',   0) or 0)
                fill_price_r = float(orders.get('trade_avg_price', 0) or 0)
                order_status = orders.get('status', '?')
                print(f"   [HTX] REST попытка {attempt}: status={order_status} "
                      f"filled={trade_vol_r}/{volume} avg={fill_price_r}")
                if trade_vol_r > 0:
                    pct = trade_vol_r / volume * 100
                    print(f"✅ [HTX] {side} {symbol} (REST poll) | "
                          f"filled={trade_vol_r}/{volume} ({pct:.0f}%) | avg={fill_price_r} | {elapsed:.0f}ms")
                    return True, fill_price_r, elapsed, False
                if order_status in (7, '7', 5, '5') and trade_vol_r == 0:
                    return False, 0.0, elapsed, True
                if order_status in (6, '6'):
                    return False, 0.0, elapsed, True
            print(f"❌ [HTX] REST: 8 попыток — статус неизвестен")
        elif mode == 'WS':
            print(f"❌ [HTX] WS notify timeout — ордер неизвестен")
        else:
            print(f"❌ [HTX] notify timeout (HTTP тоже не ответил)")
        return False, 0.0, (time.perf_counter()-t_start)*1000, False

    async def close_position(self, symbol: str, side: str, volume: int) -> bool:
        close_dir = 'sell' if side == 'buy' else 'buy'
        cid_str   = None
        try:
            cid        = self._next_cid()
            cid_str    = str(cid)
            notify_fut = asyncio.Future()
            self.order_futures[cid_str] = notify_fut

            # ── Fast path: WS ─────────────────────────────────────────────
            ws_ok = False
            try:
                ws_ack = await self._ws_place(cid, symbol, close_dir, 'close', volume)
                err_c  = ws_ack.get('err-code', 0)
                if err_c != 0:
                    self.order_futures.pop(cid_str, None)
                    print(f"❌ [HTX] Закрытие WS отклонено [{err_c}]: {ws_ack.get('err-msg','')}")
                    return False
                ws_ok  = True
                oid_ws = ws_ack.get('data', {}).get('order_id', '?')
                print(f"   [HTX] WS закрытие принято id={oid_ws}, ждём...")
            except SystemExit:
                raise
            except Exception as ws_err:
                self.ws_place_futures.pop(cid_str, None)
                print(f"   [HTX] Закрытие WS недоступен ({ws_err.__class__.__name__}), fallback REST...")

            # ── Slow path: REST HTTP ───────────────────────────────────────
            http_result = None
            if not ws_ok:
                params = {
                    'contract_code':    symbol,
                    'client_order_id':  cid,
                    'direction':        close_dir,
                    'offset':           'close',
                    'lever_rate':       1,
                    'volume':           volume,
                    'order_price_type': 'opponent_ioc',
                }
                http_task = asyncio.create_task(self._http_post(params))
                try:
                    http_result = await asyncio.wait_for(asyncio.shield(http_task), timeout=1.5)
                    if http_result.get('status') != 'ok':
                        self.order_futures.pop(cid_str, None)
                        err_msg = http_result.get('err-msg') or str(http_result)
                        print(f"❌ [HTX] Закрытие REST отклонено: {err_msg}")
                        return False
                    oid = http_result.get('data', {}).get('order_id', '?')
                    print(f"   [HTX] Закрытие REST принято id={oid}, ждём...")
                except asyncio.TimeoutError:
                    http_task.add_done_callback(
                        lambda t: t.exception() if not t.cancelled() else None)
                    print(f"   [HTX] Закрытие REST timeout, ждём WS notify...")

            # ── Ожидаем fill notify ────────────────────────────────────────
            ws_data = None
            timeout = 2.5 if ws_ok else 0.3
            try:
                ws_data = await asyncio.wait_for(notify_fut, timeout=timeout)
            except asyncio.TimeoutError:
                pass
            finally:
                self.order_futures.pop(cid_str, None)

            if ws_data is not None and float(ws_data.get('trade_volume', 0) or 0) > 0:
                avg = float(ws_data.get('trade_avg_price', 0) or 0)
                mode = 'WS' if ws_ok else 'REST'
                print(f"✅ [HTX] Закрыто {close_dir} {symbol} [{mode}] | "
                      f"vol={ws_data.get('trade_volume')} avg={avg}")
                return True

            if not ws_ok and http_result and http_result.get('status') == 'ok':
                for attempt in range(1, 9):
                    if attempt > 1:
                        await asyncio.sleep(0.15 if attempt <= 3 else 0.4)
                    rest_d = await self._query_order_by_cid(symbol, cid_str)
                    if rest_d.get('status') != 'ok': continue
                    orders = rest_d.get('data', {})
                    if isinstance(orders, list) and orders: orders = orders[0]
                    if not isinstance(orders, dict): continue
                    trade_vol    = float(orders.get('trade_volume', 0) or 0)
                    order_status = orders.get('status', '?')
                    if trade_vol > 0:
                        avg = float(orders.get('trade_avg_price', 0) or 0)
                        print(f"✅ [HTX] Закрыто {close_dir} {symbol} | vol={trade_vol} avg={avg}")
                        return True
                    if order_status in (7, '7', 5, '5'):
                        print(f"❌ [HTX] Закрытие нет ликвидности (status={order_status})")
                        return False
                    if order_status in (6, '6'):
                        print(f"❌ [HTX] Закрытие без исполнения (status={order_status})")
                        return False
                print(f"❌ [HTX] Закрытие: REST 8 попыток — неизвестен")
            elif ws_ok:
                print(f"❌ [HTX] Закрытие WS notify timeout — ордер неизвестен")
            return False
        except SystemExit:
            raise
        except Exception as e:
            if cid_str:
                self.order_futures.pop(cid_str, None)
                self.ws_place_futures.pop(cid_str, None)
            print(f"❌ [HTX] close_position: {e}")
            return False

    async def get_position_volume(self, symbol: str, side: str) -> float:
        try:
            path     = '/linear-swap-api/v1/swap_position_info'
            host     = 'api.hbdm.com'
            ts       = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
            body_str = json.dumps({'contract_code': symbol}, separators=(',', ':'))
            qp       = {'AccessKeyId': self.api_key, 'SignatureMethod': 'HmacSHA256',
                        'SignatureVersion': '2', 'Timestamp': ts}
            qs       = urlencode(sorted(qp.items()))
            sig      = base64.b64encode(
                hmac.new(self._hmac_key,
                         f"POST\n{host}\n{path}\n{qs}".encode(),
                         hashlib.sha256).digest()).decode()
            qp['Signature'] = sig
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as sess:
                async with sess.post(
                    f'https://{host}{path}', params=qp,
                    data=body_str.encode(),
                    headers={'Content-Type': 'application/json'},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    data = json.loads(await r.read())
            if data.get('status') == 'ok':
                for p in data.get('data', []):
                    if (p.get('contract_code') == symbol
                            and p.get('direction') == side
                            and float(p.get('volume', 0)) > 0):
                        return float(p['volume'])
                return 0.0
            print(f"  ⚠️  [HTX] position_info: {data.get('err-code')} {data.get('err-msg')}")
            return -1.0
        except Exception as e:
            print(f"  ⚠️  [HTX] position_info exception: {type(e).__name__}: {e}")
            return -1.0

    async def close(self):
        self.running = False
        for ws in [self.ws_pub, self.ws_prv]:
            if ws:
                try:
                    await ws.close()
                except Exception:
                    pass
        if self.session and not self.session.closed:
            await self.session.close()


# ============================================================
# OKX
# ============================================================
class OKXTrader:
    REST_URL   = 'https://www.okx.com'
    INFO_URL   = 'https://www.okx.com/api/v5/public/instruments?instType=SWAP'
    WS_PUB_URL = 'wss://ws.okx.com:8443/ws/v5/public'
    WS_PRV_URL = 'wss://ws.okx.com:8443/ws/v5/private'
    _REST_CANDIDATES = [
        'https://www.okx.com',
        'https://aws.okx.com',
        'https://okx.com',
    ]

    def __init__(self, api_key: str, secret: str, password: str):
        self.api_key      = api_key
        self.secret       = secret
        self.password     = password
        self.ws_pub       = None
        self.ws_prv       = None
        self.running      = False
        self.price_cache: Dict[str, dict] = {}
        self.pending: Dict[str, asyncio.Future] = {}
        self.multipliers: Dict[str, float] = {}
        self.on_tick      = None
        self._rid_counter = 0
        self._symbols: List[str] = []
        self._books5_ready: set = set()
        self.pos_mode: str = 'net_mode'
        self.inst_codes: Dict[str, int] = {}

    def _next_rid(self) -> str:
        self._rid_counter = (self._rid_counter + 1) % 99999
        return f"{int(time.time()*1000)}{self._rid_counter:05d}"

    @classmethod
    async def _detect_rest_url(cls) -> str:
        test_path = '/api/v5/public/time'
        for url in cls._REST_CANDIDATES:
            try:
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as s:
                    async with s.get(url + test_path, timeout=aiohttp.ClientTimeout(total=4)) as r:
                        data = await r.json()
                        if data.get('code') == '0':
                            return url
            except Exception:
                pass
        return cls._REST_CANDIDATES[0]

    async def fetch_multipliers(self, symbols: List[str]):
        detected = await OKXTrader._detect_rest_url()
        OKXTrader.REST_URL = detected
        OKXTrader.INFO_URL = detected + '/api/v5/public/instruments?instType=SWAP'
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as s:
            async with s.get(self.INFO_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = json.loads(await r.text())
        if data.get('code') == '0':
            for inst in data['data']:
                iid = inst.get('instId')
                if iid in symbols:
                    self.multipliers[iid] = float(inst.get('ctVal', 1))
                    code = inst.get('instIdCode')
                    if code is not None:
                        self.inst_codes[iid] = int(code)
            print(f"✅ [OKX] Мультипликаторы: {len(self.multipliers)} символов "
                  f"(instIdCode: {len(self.inst_codes)})")
        else:
            print(f"⚠️  [OKX] Мультипликаторы: {data}")

    def calc_contracts(self, symbol: str, price: float) -> int:
        cs = self.multipliers.get(symbol, 1.0)
        return max(1, round(ORDER_USDT / (price * cs)))

    async def set_leverage_all(self, symbols: List[str]):
        print(f"⚙️  [OKX] Плечо {LEVER}x для {len(symbols)} символов...")
        ok = 0
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as sess:
            for sym in symbols:
                if await self._set_leverage_one(sess, sym):
                    ok += 1
                await asyncio.sleep(0.22)
        print(f"⚙️  [OKX] Плечо: {ok}/{len(symbols)}")

    async def _set_leverage_one(self, session: aiohttp.ClientSession, inst_id: str) -> bool:
        try:
            ts   = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            body = json.dumps({'instId': inst_id, 'lever': str(LEVER), 'mgnMode': 'cross'})
            path = '/api/v5/account/set-leverage'
            sig  = base64.b64encode(
                hmac.new(self.secret.encode(),
                         (ts + 'POST' + path + body).encode(),
                         hashlib.sha256).digest()).decode()
            hdrs = {
                'OK-ACCESS-KEY': self.api_key, 'OK-ACCESS-SIGN': sig,
                'OK-ACCESS-TIMESTAMP': ts, 'OK-ACCESS-PASSPHRASE': self.password,
                'Content-Type': 'application/json',
            }
            async with session.post(
                self.REST_URL + path, headers=hdrs, data=body,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                res = await r.json()
                return res.get('code') == '0'
        except Exception:
            return False

    async def connect(self, symbols: List[str]):
        self._symbols = symbols
        if OKXTrader.REST_URL == 'https://www.okx.com':
            detected = await OKXTrader._detect_rest_url()
            OKXTrader.REST_URL = detected
            OKXTrader.INFO_URL = detected + '/api/v5/public/instruments?instType=SWAP'
        try:
            ts_c   = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            path_c = '/api/v5/account/config'
            sig_c  = base64.b64encode(
                hmac.new(self.secret.encode(),
                         (ts_c + 'GET' + path_c).encode(),
                         hashlib.sha256).digest()).decode()
            hdrs_c = {'OK-ACCESS-KEY': self.api_key, 'OK-ACCESS-SIGN': sig_c,
                      'OK-ACCESS-TIMESTAMP': ts_c, 'OK-ACCESS-PASSPHRASE': self.password}
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as sess:
                async with sess.get(self.REST_URL + path_c, headers=hdrs_c,
                                    timeout=aiohttp.ClientTimeout(total=5)) as r:
                    cfg = await r.json()
            if cfg.get('code') == '0':
                self.pos_mode = cfg['data'][0].get('posMode', 'net_mode')
                print(f"ℹ️  [OKX] posMode={self.pos_mode}")
        except Exception as e:
            print(f"⚠️  [OKX] posMode не получен: {e} — используем net_mode")

        print("🔌 [OKX] Connecting...")
        self.ws_pub = await websockets.connect(
            self.WS_PUB_URL, ping_interval=20, ping_timeout=10,
            max_size=10**7, ssl=_SSL_CTX)
        self.ws_prv = await websockets.connect(
            self.WS_PRV_URL, ping_interval=20, ping_timeout=10,
            max_size=10**7, ssl=_SSL_CTX)
        await self._auth()
        self.running = True
        asyncio.create_task(self._pub_loop())
        asyncio.create_task(self._prv_loop())
        for s in symbols:
            await self.ws_pub.send(json.dumps({
                'op': 'subscribe', 'args': [{'channel': 'tickers', 'instId': s}]}))
        for s in symbols:
            await self.ws_pub.send(json.dumps({
                'op': 'subscribe', 'args': [{'channel': 'books5', 'instId': s}]}))
        print("✅ [OKX] Connected!")

    async def _auth(self):
        ts  = str(int(time.time()))
        sig = base64.b64encode(
            hmac.new(self.secret.encode(),
                     (ts + 'GET/users/self/verify').encode(),
                     hashlib.sha256).digest()).decode()
        await self.ws_prv.send(json.dumps({
            'op': 'login',
            'args': [{'apiKey': self.api_key, 'passphrase': self.password,
                      'timestamp': ts, 'sign': sig}]
        }))
        for _ in range(10):
            raw = await asyncio.wait_for(self.ws_prv.recv(), timeout=5)
            r   = json.loads(raw)
            if r.get('event') == 'login':
                if r.get('code') == '0':
                    return
                raise Exception(f"OKX auth failed: {r}")
            if raw == 'ping' or r.get('op') == 'ping':
                await self.ws_prv.send('pong')
        raise Exception("OKX auth timeout")

    async def _pub_loop(self):
        while self.running:
            try:
                while self.running:
                    raw = await self.ws_pub.recv()
                    if raw == 'ping':
                        await self.ws_pub.send(_OKX_PONG_STR)
                        continue
                    d  = json.loads(raw)
                    ch = d.get('arg', {}).get('channel', '')
                    if ch == 'tickers' and d.get('data'):
                        inst    = d['arg']['instId']
                        tick    = d['data'][0]
                        bid     = float(tick.get('bidPx', 0) or 0)
                        ask     = float(tick.get('askPx', 0) or 0)
                        exch_ts = int(tick.get('ts', 0) or 0)
                        ts      = exch_ts / 1000 if exch_ts > 0 else time.time()
                        if bid > 0 and ask > 0 and bid < ask:
                            prev = self.price_cache.get(inst, {})
                            self.price_cache[inst] = {
                                'bid': bid, 'ask': ask, 'ts': ts,
                                'bid_sz':    prev.get('b5_bid_sz', 0),
                                'ask_sz':    prev.get('b5_ask_sz', 0),
                                'b5_bid_sz': prev.get('b5_bid_sz', 0),
                                'b5_ask_sz': prev.get('b5_ask_sz', 0),
                            }
                            if self.on_tick:
                                self.on_tick(inst)
                    elif ch == 'books5' and d.get('data'):
                        try:
                            inst   = d['arg']['instId']
                            book   = d['data'][0]
                            bids_b = book.get('bids', [])
                            asks_b = book.get('asks', [])
                            if bids_b and asks_b:
                                bid_b    = float(bids_b[0][0])
                                ask_b    = float(asks_b[0][0])
                                bid_sz_b = float(bids_b[0][1]) if len(bids_b[0]) > 1 else 0.0
                                ask_sz_b = float(asks_b[0][1]) if len(asks_b[0]) > 1 else 0.0
                                exch_ts_b = int(book.get('ts', 0) or 0)
                                ts_b = exch_ts_b / 1000 if exch_ts_b > 0 else time.time()
                                if bid_b > 0 and ask_b > 0 and bid_b < ask_b:
                                    prev  = self.price_cache.get(inst, {})
                                    b5_bid = bid_sz_b if bid_sz_b > 0 else prev.get('b5_bid_sz', 0)
                                    b5_ask = ask_sz_b if ask_sz_b > 0 else prev.get('b5_ask_sz', 0)
                                    self.price_cache[inst] = {
                                        'bid': bid_b, 'ask': ask_b, 'ts': ts_b,
                                        'bid_sz':    b5_bid,
                                        'ask_sz':    b5_ask,
                                        'b5_bid_sz': b5_bid,
                                        'b5_ask_sz': b5_ask,
                                    }
                                    self._books5_ready.add(inst)
                        except Exception:
                            pass
            except websockets.exceptions.ConnectionClosed:
                if not self.running:
                    return
                print("⚠️  [OKX] Pub WS обрыв, переподключение...")
                self._books5_ready.clear()
                await asyncio.sleep(2)
                try:
                    self.ws_pub = await websockets.connect(
                        self.WS_PUB_URL, ping_interval=20, ping_timeout=10,
                        max_size=10**7, ssl=_SSL_CTX)
                    for s in self._symbols:
                        await self.ws_pub.send(json.dumps({
                            'op': 'subscribe', 'args': [{'channel': 'tickers', 'instId': s}]}))
                    for s in self._symbols:
                        await self.ws_pub.send(json.dumps({
                            'op': 'subscribe', 'args': [{'channel': 'books5', 'instId': s}]}))
                except Exception as e:
                    print(f"❌ [OKX] Pub reconnect: {e}")
            except Exception as e:
                if self.running:
                    print(f"❌ [OKX] pub loop: {e}")
                    await asyncio.sleep(1)

    async def _prv_loop(self):
        while self.running:
            try:
                while self.running:
                    raw = await self.ws_prv.recv()
                    if raw == 'ping':
                        await self.ws_prv.send(_OKX_PONG_STR)
                        continue
                    d   = json.loads(raw)
                    op  = d.get('op', '')
                    rid = d.get('id', '')
                    if op == 'order':
                        if rid in self.pending:
                            f = self.pending.pop(rid)
                            if not f.done():
                                f.set_result(d)
                        else:
                            print(f"⚠️  [OKX] rid={rid!r} не в pending")
                    elif op not in ('login', 'subscribe', 'unsubscribe', ''):
                        chan = d.get('arg', {}).get('channel', '')
                        if chan not in ('orders', 'positions', 'account', ''):
                            print(f"⚠️  [OKX] unknown op={op!r} chan={chan!r}")
            except websockets.exceptions.ConnectionClosed:
                for f in self.pending.values():
                    if not f.done():
                        f.set_exception(Exception("OKX prv WS обрыв"))
                self.pending.clear()
                if not self.running:
                    return
                print("⚠️  [OKX] Prv WS обрыв, переподключение...")
                await asyncio.sleep(2)
                try:
                    self.ws_prv = await websockets.connect(
                        self.WS_PRV_URL, ping_interval=20, ping_timeout=10,
                        max_size=10**7, ssl=_SSL_CTX)
                    await self._auth()
                except Exception as e:
                    print(f"❌ [OKX] Prv reconnect: {e}")
            except Exception as e:
                if self.running:
                    print(f"❌ [OKX] prv loop: {e}")
                    await asyncio.sleep(1)

    def get_price(self, symbol: str) -> Optional[dict]:
        c = self.price_cache.get(symbol)
        if c and (time.time() - c['ts']) < MIN_PRICE_AGE_SEC:
            return c
        return None

    async def open_position(self, symbol: str, side: str, amount: int):
        rid = None
        t_start = time.perf_counter()
        try:
            rid  = self._next_rid()
            code = self.inst_codes.get(symbol)
            if code is not None:
                args = {'instIdCode': code, 'tdMode': 'cross', 'side': side,
                        'ordType': 'market', 'sz': str(amount)}
            else:
                args = {'instId': symbol, 'tdMode': 'cross', 'side': side,
                        'ordType': 'market', 'sz': str(amount)}
            if self.pos_mode == 'long_short_mode':
                args['posSide'] = 'long' if side == 'buy' else 'short'
            f = asyncio.Future()
            self.pending[rid] = f
            await self.ws_prv.send(json.dumps({'id': rid, 'op': 'order', 'args': [args]}))
            try:
                r = await asyncio.wait_for(f, timeout=3)
            except asyncio.TimeoutError:
                self.pending.pop(rid, None)
                print(f"❌ [OKX] Timeout {symbol}")
                return False, 0.0, (time.perf_counter()-t_start)*1000, False
            finally:
                self.pending.pop(rid, None)
            elapsed_ms = (time.perf_counter() - t_start) * 1000
            if r and r.get('code') == '0':
                oid = r.get('data', [{}])[0].get('ordId', 'N/A')
                fill_price = await self._get_order_fill(symbol, oid)
                print(f"✅ [OKX] {side} {symbol} | id={oid} | avg={fill_price if fill_price else '?'} | {elapsed_ms:.0f}ms")
                return True, fill_price, elapsed_ms, False
            else:
                okx_err_msg  = r.get('msg', '') if r else 'empty'
                okx_err_data = (r.get('data') or [{}])[0] if r else {}
                okx_sMsg     = okx_err_data.get('sMsg', '')
                full_msg     = okx_sMsg or okx_err_msg
                print(f"❌ [OKX] {symbol}: {full_msg}")
                if 'margin' in str(full_msg).lower() or 'insufficient' in str(full_msg).lower():
                    print(f"\n🛑 СТОП: Недостаточно маржи OKX — бот остановлен!")
                    raise SystemExit(1)
                return False, 0.0, elapsed_ms, True
        except Exception as e:
            if rid:
                self.pending.pop(rid, None)
            print(f"❌ [OKX] open_position: {e}")
            return False, 0.0, (time.perf_counter()-t_start)*1000, False

    async def _get_order_fill(self, symbol: str, order_id: str, retries: int = 5) -> float:
        path = f'/api/v5/trade/order?instId={symbol}&ordId={order_id}'
        for attempt in range(retries):
            try:
                ts  = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                sig = base64.b64encode(
                    hmac.new(self.secret.encode(),
                             (ts + 'GET' + path).encode(),
                             hashlib.sha256).digest()).decode()
                hdrs = {
                    'OK-ACCESS-KEY': self.api_key, 'OK-ACCESS-SIGN': sig,
                    'OK-ACCESS-TIMESTAMP': ts, 'OK-ACCESS-PASSPHRASE': self.password,
                }
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as sess:
                    async with sess.get(
                        self.REST_URL + path, headers=hdrs,
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp:
                        data = await resp.json()
                if data.get('code') == '0' and data.get('data'):
                    d      = data['data'][0]
                    state  = d.get('state', '')
                    avg_px = float(d.get('avgPx', 0) or 0)
                    if state not in ('filled', 'partially_filled') or avg_px == 0:
                        await asyncio.sleep(0.3)
                        continue
                    return avg_px
            except Exception:
                pass
            await asyncio.sleep(0.3)
        return 0.0

    async def close_position(self, symbol: str, side: str, amount: int) -> bool:
        close_side = 'sell' if side == 'buy' else 'buy'
        rid = None
        try:
            rid  = self._next_rid()
            code = self.inst_codes.get(symbol)
            if code is not None:
                args = {'instIdCode': code, 'tdMode': 'cross', 'side': close_side,
                        'ordType': 'market', 'sz': str(amount), 'reduceOnly': True}
            else:
                args = {'instId': symbol, 'tdMode': 'cross', 'side': close_side,
                        'ordType': 'market', 'sz': str(amount), 'reduceOnly': True}
            if self.pos_mode == 'long_short_mode':
                args['posSide'] = 'short' if close_side == 'buy' else 'long'
                args.pop('reduceOnly', None)
            f = asyncio.Future()
            self.pending[rid] = f
            await self.ws_prv.send(json.dumps({'id': rid, 'op': 'order', 'args': [args]}))
            try:
                r = await asyncio.wait_for(f, timeout=3)
            except asyncio.TimeoutError:
                self.pending.pop(rid, None)
                print(f"❌ [OKX] Закрытие timeout {symbol}")
                return False
            finally:
                self.pending.pop(rid, None)
            if r and r.get('code') == '0':
                oid = r.get('data', [{}])[0].get('ordId', '?')
                print(f"✅ [OKX] Закрыто {close_side} {symbol} | id={oid}")
                return True
            else:
                print(f"❌ [OKX] Закрытие: {r.get('msg', r) if r else 'empty'}")
                return False
        except Exception as e:
            if rid:
                self.pending.pop(rid, None)
            print(f"❌ [OKX] close_position: {e}")
            return False

    async def get_position_volume(self, symbol: str, side: str) -> float:
        try:
            ts   = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            path = f'/api/v5/account/positions?instType=SWAP&instId={symbol}'
            sig  = base64.b64encode(
                hmac.new(self.secret.encode(),
                         (ts + 'GET' + path).encode(),
                         hashlib.sha256).digest()).decode()
            hdrs = {
                'OK-ACCESS-KEY': self.api_key, 'OK-ACCESS-SIGN': sig,
                'OK-ACCESS-TIMESTAMP': ts, 'OK-ACCESS-PASSPHRASE': self.password,
            }
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as sess:
                async with sess.get(
                    self.REST_URL + path, headers=hdrs,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    data = json.loads(await r.read())
            if data.get('code') == '0':
                for p in data.get('data', []):
                    pos = float(p.get('pos', 0))
                    if pos != 0:
                        return abs(pos)
                return 0.0
            return -1.0
        except Exception:
            return -1.0

    async def close(self):
        self.running = False
        for ws in [self.ws_pub, self.ws_prv]:
            if ws:
                try:
                    await ws.close()
                except Exception:
                    pass


# ============================================================
# МОНИТОР
# ============================================================
class ArbitrageMonitor:
    MAX_VALID_SPREAD = 0.05

    def __init__(self, htx: HTXTrader, okx: OKXTrader):
        self.htx     = htx
        self.okx     = okx
        self.running = False
        self.executing_coins: set = set()
        self.positions: Dict[str, dict] = {}
        self.MAX_PARALLEL = 2
        self.okx_blacklist: set = set()
        self._sym_index: Dict[str, tuple] = {}
        self._trade_counter = 0

    def _calc_spread(self, p_htx: dict, p_okx: dict):
        s_htx = (p_okx['bid'] - p_htx['ask']) / p_htx['ask']
        s_okx = (p_htx['bid'] - p_okx['ask']) / p_okx['ask']
        best  = s_htx if s_htx >= s_okx else s_okx
        if best > self.MAX_VALID_SPREAD:
            return -1.0, 'invalid'
        if s_htx >= s_okx:
            return s_htx, 'htx_cheap'
        return s_okx, 'okx_cheap'

    def _on_tick(self, updated_sym: str):
        pair = self._sym_index.get(updated_sym)
        if not pair:
            return
        htx_sym, okx_sym = pair
        coin = htx_sym.replace('-USDT', '')
        if coin in self.executing_coins or coin in self.positions or coin in self.okx_blacklist:
            return
        if len(self.positions) + len(self.executing_coins) >= self.MAX_PARALLEL:
            return
        p_htx = self.htx.get_price(htx_sym)
        p_okx = self.okx.get_price(okx_sym)
        if not p_htx or not p_okx:
            return
        if okx_sym not in self.okx._books5_ready:
            return
        age_diff = abs(p_htx['ts'] - p_okx['ts'])
        if age_diff > MAX_PRICE_SYNC_SEC:
            return
        spread_pct, direction = self._calc_spread(p_htx, p_okx)
        if direction == 'invalid' or spread_pct < SPREAD_THRESHOLD:
            return
        if spread_pct > 0.20:
            ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"\r⚠️  [{ts}] АНОМАЛЬНЫЙ СПРЕД {spread_pct*100:.2f}% на {coin} — пропущено")
            return
        min_liq    = ORDER_USDT * 10
        okx_cs_sym = self.okx.multipliers.get(okx_sym, 1.0)
        htx_bid_usd = p_htx.get('bid_sz', 0) * p_htx['bid']
        htx_ask_usd = p_htx.get('ask_sz', 0) * p_htx['ask']
        okx_bid_usd = p_okx.get('bid_sz', 0) * p_okx['bid'] * okx_cs_sym
        okx_ask_usd = p_okx.get('ask_sz', 0) * p_okx['ask'] * okx_cs_sym
        if direction == 'htx_cheap':
            trade_htx_usd, trade_okx_usd = htx_ask_usd, okx_bid_usd
            opp_htx_usd,   opp_okx_usd   = htx_bid_usd, okx_ask_usd
        else:
            trade_htx_usd, trade_okx_usd = htx_bid_usd, okx_ask_usd
            opp_htx_usd,   opp_okx_usd   = htx_ask_usd, okx_bid_usd
        if trade_htx_usd < min_liq or trade_okx_usd < min_liq:
            return
        max_opp = 5
        if (opp_htx_usd > trade_htx_usd * max_opp or
                opp_okx_usd > trade_okx_usd * max_opp):
            return
        self.executing_coins.add(coin)
        opp = {
            'htx_sym': htx_sym, 'okx_sym': okx_sym, 'coin': coin,
            'spread_pct': spread_pct, 'direction': direction,
            'p_htx': p_htx, 'p_okx': p_okx,
        }
        try:
            if SIGNAL_DELAY_MS > 0:
                asyncio.get_running_loop().create_task(self._delayed_execute(opp))
            else:
                asyncio.get_running_loop().create_task(self._execute(opp))
        except RuntimeError:
            self.executing_coins.discard(coin)

    async def _delayed_execute(self, opp: dict):
        coin        = opp['coin']
        htx_sym     = opp['htx_sym']
        okx_sym     = opp['okx_sym']
        orig_spread = opp['spread_pct']
        orig_dir    = opp['direction']
        ts_wait     = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"\r⏱️  [{ts_wait}] {coin}: ждём {SIGNAL_DELAY_MS}мс (спред {orig_spread*100:.4f}%)")
        await asyncio.sleep(SIGNAL_DELAY_MS / 1000)
        p_htx = self.htx.get_price(htx_sym)
        p_okx = self.okx.get_price(okx_sym)
        if not p_htx or not p_okx:
            ts_skip = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"\r❌ [{ts_skip}] {coin}: нет цены после задержки — пропущен")
            self.executing_coins.discard(coin)
            return
        new_spread, new_dir = self._calc_spread(p_htx, p_okx)
        ts_check = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        if new_dir == 'invalid' or new_spread < SPREAD_THRESHOLD or new_dir != orig_dir:
            print(f"\r❌ [{ts_check}] {coin}: сигнал исчез ({new_spread*100:.4f}%) — пропущен")
            self.executing_coins.discard(coin)
            return
        delta = (new_spread - orig_spread) * 100
        sign  = f"+{delta:.4f}%" if delta >= 0 else f"{delta:.4f}%"
        print(f"\r✅ [{ts_check}] {coin}: сигнал подтверждён {new_spread*100:.4f}% ({sign}) — входим")
        opp = dict(opp)
        opp['p_htx']      = p_htx
        opp['p_okx']      = p_okx
        opp['spread_pct'] = new_spread
        opp['direction']  = new_dir
        await self._execute_inner(opp)

    async def _execute(self, opp: dict):
        coin = opp.get('coin', opp['htx_sym'].replace('-USDT', ''))
        try:
            await self._execute_inner(opp)
        except Exception as e:
            import traceback
            print(f"❌ [МОНИТОР] Ошибка: {e}")
            traceback.print_exc()
        finally:
            self.executing_coins.discard(coin)

    async def _execute_inner(self, opp: dict):
        htx_sym    = opp['htx_sym']
        okx_sym    = opp['okx_sym']
        spread_pct = opp['spread_pct']
        direction  = opp['direction']
        p_htx      = opp['p_htx']
        p_okx      = opp['p_okx']
        ts         = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        coin       = opp.get('coin', htx_sym.replace('-USDT', ''))

        if direction == 'htx_cheap':
            buy_exch, sell_exch   = 'HTX', 'OKX'
            buy_sym,  sell_sym    = htx_sym, okx_sym
            entry_buy, entry_sell = p_htx['ask'], p_okx['bid']
            buy_price             = p_htx['ask']
            htx_side, okx_side   = 'buy', 'sell'
        else:
            buy_exch, sell_exch   = 'OKX', 'HTX'
            buy_sym,  sell_sym    = okx_sym, htx_sym
            entry_buy, entry_sell = p_okx['ask'], p_htx['bid']
            buy_price             = p_okx['ask']
            htx_side, okx_side   = 'sell', 'buy'

        htx_cs = self.htx.multipliers.get(htx_sym, 1.0)
        okx_cs = self.okx.multipliers.get(okx_sym, 1.0)
        coins   = ORDER_USDT / buy_price
        vol_htx = max(1, round(coins / htx_cs))
        vol_okx = max(1, round(coins / okx_cs))
        real_coins_htx = vol_htx * htx_cs
        real_coins_okx = vol_okx * okx_cs
        if abs(real_coins_htx - real_coins_okx) > htx_cs:
            target  = min(real_coins_htx, real_coins_okx)
            vol_htx = max(1, round(target / htx_cs))
            vol_okx = max(1, round(target / okx_cs))

        post_ticks: list = []
        _capture_active = True

        async def _capture_ticks():
            seen_ts = set()
            while _capture_active:
                ph = self.htx.price_cache.get(htx_sym)
                po = self.okx.price_cache.get(okx_sym)
                if ph and po:
                    key = (round(ph['bid'], 8), round(po['bid'], 8))
                    if key not in seen_ts:
                        seen_ts.add(key)
                        okx_cs_t = self.okx.multipliers.get(okx_sym, 1.0)
                        if direction == 'htx_cheap':
                            sprd = (po['bid'] - ph['ask']) / ph['ask'] * 100
                        else:
                            sprd = (ph['bid'] - po['ask']) / po['ask'] * 100
                        post_ticks.append({
                            'ts':          datetime.now().strftime('%H:%M:%S.%f')[:-3],
                            'htx_bid':     ph['bid'], 'htx_ask':  ph['ask'],
                            'htx_bid_usd': round(ph.get('bid_sz', 0)*ph['bid'], 0),
                            'htx_ask_usd': round(ph.get('ask_sz', 0)*ph['ask'], 0),
                            'okx_bid':     po['bid'], 'okx_ask':  po['ask'],
                            'okx_bid_usd': round(po.get('bid_sz', 0)*po['bid']*okx_cs_t, 0),
                            'okx_ask_usd': round(po.get('ask_sz', 0)*po['ask']*okx_cs_t, 0),
                            'spread':      round(sprd, 4),
                        })
                await asyncio.sleep(0.05)

        capture_task = asyncio.get_running_loop().create_task(_capture_ticks())
        t0 = time.perf_counter()

        if direction == 'htx_cheap':
            exec_task = asyncio.gather(
                self.htx.open_position(htx_sym, 'buy',  vol_htx),
                self.okx.open_position(okx_sym, 'sell', vol_okx),
                return_exceptions=True,
            )
        else:
            exec_task = asyncio.gather(
                self.htx.open_position(htx_sym, 'sell', vol_htx),
                self.okx.open_position(okx_sym, 'buy',  vol_okx),
                return_exceptions=True,
            )

        sep = '─' * 46
        print(f"\n{'🔥' * 23}")
        print(f"  [{ts}]  АРБИТРАЖ #{self._trade_counter + 1}  |  {coin}")
        print(f"  {sep}")
        print(f"  Спред:    {spread_pct * 100:.4f}%")
        print(f"  Покупка:  {buy_sym} @ {buy_exch}  ask={entry_buy}")
        print(f"  Продажа:  {sell_sym} @ {sell_exch} bid={entry_sell}")
        htx_bid_usd = p_htx.get('bid_sz', 0) * p_htx['bid']
        htx_ask_usd = p_htx.get('ask_sz', 0) * p_htx['ask']
        okx_bid_usd = p_okx.get('bid_sz', 0) * p_okx['bid'] * okx_cs
        okx_ask_usd = p_okx.get('ask_sz', 0) * p_okx['ask'] * okx_cs
        def _fmt_usd(v): return f"${v:,.0f}" if v >= 1 else "?"
        print(f"  HTX:      bid={p_htx['bid']} ({_fmt_usd(htx_bid_usd)})  ask={p_htx['ask']} ({_fmt_usd(htx_ask_usd)})")
        print(f"  OKX:      bid={p_okx['bid']} ({_fmt_usd(okx_bid_usd)})  ask={p_okx['ask']} ({_fmt_usd(okx_ask_usd)})")
        print(f"  Объём:    HTX {vol_htx}x{htx_cs} | OKX {vol_okx}x{okx_cs}")
        print(f"  {sep}")

        raw     = await exec_task
        elapsed = (time.perf_counter() - t0) * 1000

        async def _delayed_stop():
            nonlocal _capture_active
            await asyncio.sleep(3.0)
            _capture_active = False
            capture_task.cancel()
        asyncio.get_running_loop().create_task(_delayed_stop())
        await asyncio.sleep(0.1)

        r_htx, r_okx = raw[0], raw[1]
        kyc_stop = getattr(self.htx, '_kyc_required', False)
        if isinstance(r_htx, Exception):
            print(f"❌ [HTX] Исключение: {r_htx}")
            r_htx = (False, 0.0, 0.0, False)
        if isinstance(r_okx, Exception):
            print(f"❌ [OKX] Исключение: {r_okx}")
            r_okx = (False, 0.0, 0.0, False)

        htx_ok, htx_fill, htx_ms, htx_rejected = r_htx
        okx_ok, okx_fill, okx_ms, okx_rejected = r_okx

        if okx_rejected and not okx_ok:
            self.okx_blacklist.add(coin)
            print(f"🚫 [{coin}] добавлен в blacklist OKX")

        both_ok  = htx_ok and okx_ok
        hedge_ok = False

        if not both_ok and (htx_ok or okx_ok):
            if not htx_ok:
                failed_exch = 'HTX'; failed_sym = htx_sym
                failed_vol  = vol_htx; failed_side = htx_side
                open_exch   = 'OKX'; open_sym = okx_sym
                open_vol    = vol_okx; open_side = okx_side
            else:
                failed_exch = 'OKX'; failed_sym = okx_sym
                failed_vol  = vol_okx; failed_side = okx_side
                open_exch   = 'HTX'; open_sym = htx_sym
                open_vol    = vol_htx; open_side = htx_side

            htx_status_unknown = (not htx_ok and not htx_rejected)
            okx_perm_rejected  = (not okx_ok and okx_rejected and failed_exch == 'OKX')

            if htx_status_unknown:
                print(f"\n🚨 HTX статус неизвестен. Сразу закрываем {open_exch}...")
                retry_ok = False
            elif okx_perm_rejected:
                print(f"\n🚫 OKX отклонил {okx_sym} навсегда — пропускаем retry")
                retry_ok = False
            else:
                print(f"\n⚠️  ЧАСТИЧНОЕ: {failed_exch} отклонён. Retry...")
                retry_ok = False
                for attempt in range(1, 3):
                    print(f"   🔄 Retry {attempt}/2: {failed_exch} {failed_side} {failed_sym} vol={failed_vol}")
                    if not htx_ok:
                        r_ok, r_fill, r_ms, r_rej = await self.htx.open_position(
                            failed_sym, failed_side, failed_vol)
                    else:
                        r_ok, r_fill, r_ms, r_rej = await self.okx.open_position(
                            failed_sym, failed_side, failed_vol)
                    if r_ok:
                        print(f"   ✅ Retry {attempt} успешен ({r_ms:.0f}ms)")
                        htx_ok  = htx_ok  or (not htx_ok and r_ok)
                        okx_ok  = okx_ok  or (not okx_ok and r_ok)
                        both_ok = True; retry_ok = True
                        break
                    if not r_rej:
                        print(f"   ⚠️  Retry {attempt} — статус неизвестен, стоп")
                        break

            if not retry_ok and not both_ok:
                print(f"\n🚨 Закрываем {open_exch} ({open_side} {open_sym} vol={open_vol})...")
                for attempt in range(1, 4):
                    if open_exch == 'OKX':
                        hedge_ok = await self.okx.close_position(open_sym, open_side, open_vol)
                    else:
                        hedge_ok = await self.htx.close_position(open_sym, open_side, open_vol)
                    if hedge_ok:
                        print(f"   ✅ Закрыто (попытка {attempt})")
                        break
                    print(f"   ❌ Попытка {attempt} неудачна, ждём 1с...")
                    await asyncio.sleep(1.0)
                else:
                    print(f"   🚨 НЕ ЗАКРЫТО ПОСЛЕ 3 ПОПЫТОК — ЗАКРЫТЬ ВРУЧНУЮ!")

        if both_ok:
            status = "✅  ОБЕ СТОРОНЫ ИСПОЛНЕНЫ"
        elif not (htx_ok or okx_ok):
            status = "❌  НЕ ИСПОЛНЕНО"
        elif hedge_ok:
            status = (f"⚠️  ЧАСТИЧНОЕ — ЗАКРЫТО ХЕДЖЕМ "
                      f"(HTX={'✅' if htx_ok else '❌'} OKX={'✅' if okx_ok else '❌'})")
        else:
            status = (f"🚨 ЧАСТИЧНОЕ — ЗАКРЫТЬ ВРУЧНУЮ! "
                      f"HTX={'✅' if htx_ok else '❌'} OKX={'✅' if okx_ok else '❌'}")

        if direction == 'htx_cheap':
            real_buy_price  = htx_fill if htx_fill > 0 else entry_buy
            real_sell_price = okx_fill if okx_fill > 0 else entry_sell
        else:
            real_buy_price  = okx_fill if okx_fill > 0 else entry_buy
            real_sell_price = htx_fill if htx_fill > 0 else entry_sell

        real_spread_pct = ((real_sell_price - real_buy_price) / real_buy_price * 100
                           if real_buy_price > 0 else 0.0)

        print(f"  Результат: {status}")
        print(f"  {sep}")
        print(f"  Цена покупки:  {real_buy_price:.6g}  ({buy_exch})")
        print(f"  Цена продажи:  {real_sell_price:.6g}  ({sell_exch})")
        print(f"  Спред факт.:   {real_spread_pct:+.4f}%  (план: {spread_pct*100:.4f}%)")
        print(f"  Время HTX:     {htx_ms:.0f} ms")
        print(f"  Время OKX:     {okx_ms:.0f} ms")
        print(f"  Время итого:   {elapsed:.1f} ms")
        print(f"{'🔥' * 23}\n")

        if post_ticks:
            sample = post_ticks[0]
            px_w   = max(len(str(sample['htx_bid'])), len(str(sample['okx_bid'])), 8) + 2
            print(f"  📈 Тики после сигнала ({len(post_ticks)} шт.)")
            hdr = (f"  {'Время':<14} "
                   f"{'HTX bid':>{px_w}} {'($)':>7}  {'HTX ask':>{px_w}} {'($)':>7}   "
                   f"{'OKX bid':>{px_w}} {'($)':>7}  {'OKX ask':>{px_w}} {'($)':>7}  "
                   f"{'Спред%':>8}")
            print(f"  {'─'*len(hdr.rstrip())}")
            print(hdr)
            print(f"  {'─'*len(hdr.rstrip())}")
            for i, tk in enumerate(post_ticks[:20]):
                marker   = ' ◀СИГНАЛ' if i == 0 else ''
                sprd_col = f"{tk['spread']:+.4f}%"
                sprd_str = sprd_col if tk['spread'] >= spread_pct * 100 * 0.5 else f"({sprd_col})"
                print(f"  {tk['ts']:<14} "
                      f"{tk['htx_bid']:>{px_w}} {int(tk['htx_bid_usd']):>6}$  "
                      f"{tk['htx_ask']:>{px_w}} {int(tk['htx_ask_usd']):>6}$   "
                      f"{tk['okx_bid']:>{px_w}} {int(tk['okx_bid_usd']):>6}$  "
                      f"{tk['okx_ask']:>{px_w}} {int(tk['okx_ask_usd']):>6}$  "
                      f"{sprd_str:>9}{marker}")
            if len(post_ticks) > 20:
                print(f"  ... (+{len(post_ticks)-20} тиков)")
            print()

        self._trade_counter += 1
        entry = {
            'trade':           self._trade_counter,
            'ts':              ts,
            'coin':            coin,
            'direction':       f"{buy_exch}→{sell_exch}",
            'plan_buy_price':  entry_buy,
            'plan_sell_price': entry_sell,
            'plan_spread_pct': round(spread_pct * 100, 4),
            'fill_buy_price':  real_buy_price,
            'fill_sell_price': real_sell_price,
            'fill_spread_pct': round(real_spread_pct, 4),
            'htx_bid': p_htx['bid'], 'htx_ask': p_htx['ask'],
            'okx_bid': p_okx['bid'], 'okx_ask': p_okx['ask'],
            'vol_htx': vol_htx,      'vol_okx': vol_okx,
            'htx_ok':  htx_ok,       'okx_ok':  okx_ok,
            'fee_estimate_pct': 0.095,
            'net_spread_pct':   round(real_spread_pct - 0.095, 4),
            'est_pnl_usdt':     round(ORDER_USDT * LEVER * (real_spread_pct - 0.095) / 100, 4),
            'htx_ms':   round(htx_ms, 1),
            'okx_ms':   round(okx_ms, 1),
            'total_ms': round(elapsed, 1),
        }
        fname = f"arb_log_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]}.json"
        try:
            with open(fname, 'w', encoding='utf-8') as f:
                json.dump(entry, f, ensure_ascii=False, indent=2)
            print(f"  💾 Лог: {fname}\n")
        except Exception:
            pass

        if kyc_stop:
            print(f"\n🛑 СТОП: HTX требует KYC верификацию — бот остановлен!")
            self.running = False
            raise SystemExit(1)

        if both_ok:
            self.positions[coin] = {
                'coin':      coin,
                'htx_sym':   htx_sym,
                'okx_sym':   okx_sym,
                'htx_side':  htx_side,
                'okx_side':  okx_side,
                'vol_htx':   vol_htx,
                'vol_okx':   vol_okx,
                'open_ts':   time.time(),
                'trade_num': self._trade_counter,
            }
            htx_dir = 'LONG' if htx_side == 'buy' else 'SHORT'
            okx_dir = 'SHORT' if okx_side == 'sell' else 'LONG'
            active  = len(self.positions)
            print(f"⏳ ПОЗИЦИИ ОТКРЫТЫ [{active}/2] — жду закрытия вручную")
            print(f"   HTX  {htx_dir}  {htx_sym} × {vol_htx} контрактов")
            print(f"   OKX  {okx_dir}  {okx_sym} × {vol_okx} контрактов")
            print(f"   Проверка каждые {POSITION_POLL_SEC}с.\n")
            asyncio.get_running_loop().create_task(self._poll_until_closed(coin))

    async def _poll_until_closed(self, coin: str):
        pos = self.positions.get(coin)
        if not pos:
            return
        htx_sym       = pos['htx_sym']
        okx_sym       = pos['okx_sym']
        htx_side      = pos['htx_side']
        okx_side      = pos['okx_side']
        trade_n       = pos['trade_num']
        check_n       = 0
        htx_err_count = 0
        okx_err_count = 0
        MAX_ERR       = 5

        while self.running and coin in self.positions:
            await asyncio.sleep(POSITION_POLL_SEC)
            check_n += 1
            htx_vol, okx_vol = await asyncio.gather(
                self.htx.get_position_volume(htx_sym, htx_side),
                self.okx.get_position_volume(okx_sym, okx_side),
            )
            if htx_vol < 0: htx_err_count += 1
            else:           htx_err_count = 0
            if okx_vol < 0: okx_err_count += 1
            else:           okx_err_count = 0

            def fmt(vol: float, err_count: int) -> str:
                if vol < 0:  return f"ERR({err_count}/{MAX_ERR})"
                if vol == 0: return "CLOSED  "
                return f"open {vol:.0f}ct"

            open_min  = (time.time() - pos['open_ts']) / 60
            stat_line = (
                f"  ⏳ #{trade_n} | +{open_min:.0f}min"
                f" | HTX: {fmt(htx_vol, htx_err_count)}"
                f" | OKX: {fmt(okx_vol, okx_err_count)}"
                f"          "
            )
            print("\r" + stat_line, end='', flush=True)

            htx_closed = (htx_vol == 0.0)
            okx_closed = (okx_vol == 0.0)
            if htx_closed and okx_closed:
                open_min2 = (time.time() - pos['open_ts']) / 60
                print(f"\n{'='*50}")
                print(f"✅ Обе позиции закрыты! (держали {open_min2:.1f} мин)")
                print("🔁 Возобновляем мониторинг спреда...")
                print(f"{'='*50}\n")
                self.positions.pop(coin, None)
                return
        self.positions.pop(coin, None)

    async def run(self, symbol_pairs: List[tuple]):
        self.running = True
        for htx_sym, okx_sym in symbol_pairs:
            self._sym_index[htx_sym] = (htx_sym, okx_sym)
            self._sym_index[okx_sym] = (htx_sym, okx_sym)
        self.htx.on_tick = self._on_tick
        self.okx.on_tick = self._on_tick
        print(f"📡 Мониторинг {len(symbol_pairs)} монет | Порог: {SPREAD_THRESHOLD*100:.2f}%")
        print(f"   Бот работает непрерывно. Остановка: Ctrl+C\n")
        while self.running:
            await asyncio.sleep(1)


# ============================================================
# ПРОВЕРКА ОТКРЫТЫХ ПОЗИЦИЙ ПРИ СТАРТЕ
# ============================================================
async def check_open_positions_at_start(htx: HTXTrader, okx: OKXTrader,
                                         htx_symbols: list, okx_symbols: list):
    print("🔍 Проверка открытых позиций при старте...")
    found = []
    try:
        path      = '/linear-swap-api/v1/swap_position_info'
        ts        = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        body_str2 = '{}'
        qp_check  = {'AccessKeyId': htx.api_key, 'SignatureMethod': 'HmacSHA256',
                     'SignatureVersion': '2', 'Timestamp': ts}
        qs2  = urlencode(sorted(qp_check.items()))
        sig2 = base64.b64encode(
            hmac.new(htx._hmac_key,
                     f"POST\napi.hbdm.vn\n{path}\n{qs2}".encode(),
                     hashlib.sha256).digest()).decode()
        qp_check['Signature'] = sig2
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as sess:
            async with sess.post(
                f'https://api.hbdm.vn{path}', params=qp_check,
                data=body_str2.encode(),
                headers={'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                data = json.loads(await r.read())
        if data.get('status') == 'ok':
            for p in data.get('data', []):
                if p.get('contract_code') in htx_symbols and float(p.get('volume', 0)) > 0:
                    found.append(f"HTX {p['contract_code']} {p['direction']} {p['volume']}")
    except Exception as e:
        print(f"  ⚠️  HTX проверка не удалась: {e}")

    try:
        ts   = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        path = '/api/v5/account/positions?instType=SWAP'
        sig  = base64.b64encode(
            hmac.new(okx.secret.encode(),
                     (ts + 'GET' + path).encode(),
                     hashlib.sha256).digest()).decode()
        hdrs = {
            'OK-ACCESS-KEY': okx.api_key, 'OK-ACCESS-SIGN': sig,
            'OK-ACCESS-TIMESTAMP': ts, 'OK-ACCESS-PASSPHRASE': okx.password,
        }
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as sess:
            async with sess.get(okx.REST_URL + path, headers=hdrs,
                                timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = json.loads(await r.read())
        if data.get('code') == '0':
            for p in data.get('data', []):
                if p.get('instId') in okx_symbols and float(p.get('pos', 0)) != 0:
                    found.append(f"OKX {p['instId']} pos={p['pos']}")
    except Exception as e:
        print(f"  ⚠️  OKX проверка не удалась: {e}")

    if found:
        print(f"\n🚨 Найдены открытые позиции ({len(found)} шт.):")
        for item in found:
            print(f"   {item}")
        print("\nПродолжить запуск? (yes/no): ", end='')
        ans = input().strip().lower()
        if ans != 'yes':
            raise SystemExit("Остановлено. Закройте позиции и перезапустите.")
    else:
        print("✅ Открытых позиций нет — старт безопасен\n")


# ============================================================
# MAIN
# ============================================================
async def main():
    symbol_pairs = [(okx_to_htx(s), s) for s in OKX_SYMBOLS]
    htx_symbols  = [p[0] for p in symbol_pairs]
    okx_symbols  = [p[1] for p in symbol_pairs]

    print(f"""
╔══════════════════════════════════════════════════════════╗
║   ⚡  ARBITRAGE BOT  |  HTX × OKX  ⚡                  ║
╠══════════════════════════════════════════════════════════╣
║  Монет: {len(symbol_pairs):<3} | Порог: {SPREAD_THRESHOLD*100:.2f}% | Объём: ${ORDER_USDT:.0f}/сторону     ║
║  Плечо: {LEVER}x  | Возраст: {MIN_PRICE_AGE_SEC:.0f}s | Синхр.: {MAX_PRICE_SYNC_SEC:.1f}s             ║
╚══════════════════════════════════════════════════════════╝
""")

    htx = HTXTrader(HTX_CFG['api_key'], HTX_CFG['secret'])
    okx = OKXTrader(OKX_CFG['api_key'], OKX_CFG['secret'], OKX_CFG['password'])

    print("📊 Мультипликаторы контрактов...")
    await asyncio.gather(
        htx.fetch_multipliers(htx_symbols),
        okx.fetch_multipliers(okx_symbols),
    )

    print("🔌 Подключение...")
    try:
        await asyncio.gather(
            htx.connect(htx_symbols),
            okx.connect(okx_symbols),
        )

        print("⏳ Установка плеча + прогрев кеша цен...")
        await asyncio.gather(
            okx.set_leverage_all(okx_symbols),
            htx._warmup_http(),
        )
        print("🔥 [HTX] POST endpoint прогрет — первый ордер будет быстрым")

        await check_open_positions_at_start(htx, okx, htx_symbols, okx_symbols)

        monitor = ArbitrageMonitor(htx, okx)
        await monitor.run(symbol_pairs)

    except KeyboardInterrupt:
        print("\n⚠️  Остановка по Ctrl+C...")
    finally:
        await asyncio.sleep(1.0)
        print("\n🔍 Проверка открытых позиций перед выходом...")
        try:
            closed_any = False
            htx_pos_data = {}
            try:
                path     = '/linear-swap-api/v1/swap_position_info'
                host_pos = 'api.hbdm.vn'
                ts_p     = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
                body_p   = '{}'
                qp_p     = {'AccessKeyId': htx.api_key, 'SignatureMethod': 'HmacSHA256',
                            'SignatureVersion': '2', 'Timestamp': ts_p}
                qs_p     = urlencode(sorted(qp_p.items()))
                sig_p    = base64.b64encode(
                    hmac.new(htx._hmac_key,
                             f"POST\n{host_pos}\n{path}\n{qs_p}".encode(),
                             hashlib.sha256).digest()).decode()
                qp_p['Signature'] = sig_p
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as sess:
                    async with sess.post(
                        f'https://{host_pos}{path}', params=qp_p,
                        data=body_p.encode(),
                        headers={'Content-Type': 'application/json'},
                        timeout=aiohttp.ClientTimeout(total=6)
                    ) as r:
                        htx_pos_data = json.loads(await r.read())
            except Exception as e:
                print(f"  ⚠️  HTX позиции не получены: {e}")

            htx_positions = []
            if htx_pos_data.get('status') == 'ok':
                for p in htx_pos_data.get('data', []):
                    vol = float(p.get('volume', 0))
                    if vol > 0 and p.get('contract_code') in htx_symbols:
                        htx_positions.append({
                            'symbol': p['contract_code'],
                            'side':   p['direction'],
                            'vol':    int(vol),
                        })

            okx_positions = []
            try:
                ts_o   = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                path_o = '/api/v5/account/positions?instType=SWAP'
                sig_o  = base64.b64encode(
                    hmac.new(okx.secret.encode(),
                             (ts_o + 'GET' + path_o).encode(),
                             hashlib.sha256).digest()).decode()
                hdrs_o = {
                    'OK-ACCESS-KEY': okx.api_key, 'OK-ACCESS-SIGN': sig_o,
                    'OK-ACCESS-TIMESTAMP': ts_o, 'OK-ACCESS-PASSPHRASE': okx.password,
                }
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as sess:
                    async with sess.get(
                        okx.REST_URL + path_o, headers=hdrs_o,
                        timeout=aiohttp.ClientTimeout(total=6)
                    ) as r:
                        okx_data = json.loads(await r.read())
                if okx_data.get('code') == '0':
                    for p in okx_data.get('data', []):
                        pos = float(p.get('pos', 0))
                        if pos != 0 and p.get('instId') in okx_symbols:
                            side = 'buy' if pos > 0 else 'sell'
                            okx_positions.append({
                                'symbol': p['instId'],
                                'side':   side,
                                'vol':    int(abs(pos)),
                            })
            except Exception as e:
                print(f"  ⚠️  OKX позиции не получены: {e}")

            total = len(htx_positions) + len(okx_positions)
            if total == 0:
                print("  ✅ Открытых позиций нет")
            else:
                print(f"  🚨 Найдено {total} открытых позиций — закрываем...")
                for p in htx_positions:
                    print(f"     HTX {p['side']} {p['symbol']} vol={p['vol']} → закрываем...")
                    ok = await htx.close_position(p['symbol'], p['side'], p['vol'])
                    print(f"     {'✅' if ok else '❌'} HTX {p['symbol']}")
                    closed_any = True
                for p in okx_positions:
                    print(f"     OKX {p['side']} {p['symbol']} vol={p['vol']} → закрываем...")
                    ok = await okx.close_position(p['symbol'], p['side'], p['vol'])
                    print(f"     {'✅' if ok else '❌'} OKX {p['symbol']}")
                    closed_any = True
                if closed_any:
                    print("  ✅ Все позиции закрыты")
        except Exception as e:
            print(f"  ❌ Ошибка при закрытии позиций: {e}")

        print("🔌 Закрытие соединений...")
        await asyncio.gather(htx.close(), okx.close())
        print("✅ Готово.")


if __name__ == '__main__':
    if input("Запустить арбитраж-бот? (yes/no): ").strip().lower() == 'yes':
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("\n⚠️  Прерван")
        except SystemExit as e:
            print(f"\n{e}")
        except Exception as e:
            import traceback
            print(f"\n❌ {e}")
            traceback.print_exc()
