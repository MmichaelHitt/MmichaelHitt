import asyncio
import websockets
import json
import time
import gzip
import hmac
import hashlib
import base64
import os
import traceback
from datetime import datetime, timezone
from typing import Dict, Callable, Optional, List
from urllib.parse import urlencode
import logging
import aiohttp
import ssl

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HTXFutureWebSocketFixed:
    """WebSocket client for HTX linear futures"""

    WS_ENDPOINTS = [
        "wss://api.hbdm.com/linear-swap-ws",
        "wss://api.btcgateway.pro/linear-swap-ws",
        "wss://api.hbdm.vn/linear-swap-ws",
    ]

    def __init__(self, api_key: Optional[str] = None, secret_key: Optional[str] = None):
        self.api_key = api_key
        self.secret_key = secret_key

        self.ws = None
        self.current_endpoint = 0
        self.price_cache: Dict[str, Dict] = {}
        self.price_callbacks: Dict[str, Callable] = {}
        self.contract_multipliers: Dict[str, float] = {}

        self.message_count = 0
        self.ping_count = 0
        self.last_ping_time = 0

        self.start_time = time.time()
        self.reconnect_count = 0

        self.latency_history = []

    def get_endpoint(self):
        endpoint = self.WS_ENDPOINTS[self.current_endpoint]
        self.current_endpoint = (self.current_endpoint + 1) % len(self.WS_ENDPOINTS)
        return endpoint

    async def get_contract_multipliers(self, symbols: list):
        """Fetch contract multipliers for all symbols"""
        cache_file = 'multipliers_cache.json'

        try:
            if os.path.exists(cache_file):
                try:
                    cache_age = time.time() - os.path.getmtime(cache_file)
                except OSError as e:
                    logger.warning(f"Cache file removed, refetching: {e}")
                    cache_age = float('inf')

                if cache_age < 86400:
                    with open(cache_file, 'r') as f:
                        cached_data = json.load(f)
                        if 'htx' in cached_data:
                            for symbol, mult in cached_data['htx'].items():
                                self.contract_multipliers[symbol] = mult
                            logger.info(f"Loaded HTX multipliers from cache (age: {cache_age/3600:.1f}h)")
                            return
                else:
                    logger.info(f"Cache older than 24h ({cache_age/3600:.1f}h), refetching...")
                    try:
                        os.remove(cache_file)
                    except OSError:
                        pass
        except Exception as e:
            logger.warning(f"Could not load multipliers cache: {e}")

        try:
            url = "https://api.hbdm.com/linear-swap-api/v1/swap_contract_info"
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    text = await resp.text()
                    data = json.loads(text)

                    if data.get('status') == 'ok':
                        logger.info(f"HTX Multipliers:")
                        htx_multipliers = {}
                        for contract in data.get('data', []):
                            symbol = contract.get('contract_code')
                            multiplier = contract.get('contract_size', 1)
                            if symbol in symbols:
                                self.contract_multipliers[symbol] = multiplier
                                htx_multipliers[symbol] = multiplier
                                logger.info(f"   {symbol}: multiplier = {multiplier}")

                        try:
                            cache_data = {'htx': htx_multipliers}
                            with open(cache_file, 'w') as f:
                                json.dump(cache_data, f)
                            logger.debug(f"HTX multipliers saved to cache")
                        except Exception as e:
                            logger.warning(f"Could not save cache: {e}")
                    else:
                        logger.warning(f"Could not fetch HTX multipliers")
        except Exception as e:
            logger.error(f"Error fetching HTX multipliers: {e}")

    async def _handle_message(self, message: bytes):
        try:
            is_gzipped = False
            if len(message) >= 2:
                is_gzipped = (message[0] == 0x1f and message[1] == 0x8b)

            if is_gzipped:
                try:
                    message = gzip.decompress(message).decode('utf-8', errors='ignore')
                except Exception:
                    message = message.decode('utf-8', errors='ignore')
            else:
                message = message.decode('utf-8', errors='ignore')

            if self.message_count < 10:
                logger.debug(f"Message {self.message_count}: {message[:150]}")

            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                return None

            if 'ping' in data:
                self.ping_count += 1
                pong_time = time.time()
                self.last_ping_time = pong_time
                return json.dumps({'pong': data['ping']}, separators=(',', ':'))

            elif 'op' in data and data['op'] == 'ping':
                self.ping_count += 1
                pong_time = time.time()
                self.last_ping_time = pong_time
                return json.dumps({"op": "pong", "ts": data['ts']}, separators=(',', ':'))

            if 'op' in data and data['op'] == 'sub':
                return None

            if 'ch' in data and 'tick' in data:
                ch = data['ch']
                tick = data['tick']
                ts = data.get('ts', int(time.time() * 1000))

                client_ts = int(time.time() * 1000)
                latency = client_ts - ts

                self.message_count += 1
                self.latency_history.append(latency)

                if len(self.latency_history) > 1000:
                    self.latency_history = self.latency_history[-1000:]

                if '.bbo' in ch:
                    symbol = ch.split('.')[1]

                    bid = tick.get('bid', [0])[0] if isinstance(tick.get('bid'), list) else tick.get('bid', 0)
                    ask = tick.get('ask', [0])[0] if isinstance(tick.get('ask'), list) else tick.get('ask', 0)
                    bid_size = tick.get('bid', [0, 0])[1] if isinstance(tick.get('bid'), list) else 0
                    ask_size = tick.get('ask', [0, 0])[1] if isinstance(tick.get('ask'), list) else 0

                    spread = ask - bid
                    spread_pct = (spread / bid) * 100 if bid > 0 else 0
                    mid_price = (bid + ask) * 0.5

                    self.price_cache[symbol] = {
                        'bid': float(bid),
                        'ask': float(ask),
                        'mid_price': mid_price,
                        'bid_size': float(bid_size),
                        'ask_size': float(ask_size),
                        'spread': spread,
                        'spread_pct': spread_pct,
                        'timestamp': client_ts,
                        'server_ts': ts,
                        'latency_ms': latency,
                        'type': 'linear_bbo',
                        'exchange': 'HTX'
                    }

                    if symbol in self.price_callbacks:
                        callback = self.price_callbacks[symbol]
                        if asyncio.iscoroutinefunction(callback):
                            asyncio.create_task(callback(symbol, self.price_cache[symbol]))
                        else:
                            callback(symbol, self.price_cache[symbol])

                elif '.ticker' in ch:
                    symbol = ch.split('.')[1]

                    last_price = tick.get('last_price', 0)
                    volume = tick.get('vol', 0)
                    amount = tick.get('amount', 0)

                    self.price_cache[symbol] = {
                        'last': float(last_price),
                        'volume': float(volume),
                        'amount': float(amount),
                        'timestamp': client_ts,
                        'latency_ms': latency,
                        'type': 'linear_ticker',
                        'exchange': 'HTX'
                    }

            return None

        except Exception:
            return None

    async def connect(self, symbols: list):
        endpoint = self.get_endpoint()

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        try:
            async with websockets.connect(
                    endpoint,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    compression=None,
                    max_size=2 * 1024 * 1024,
                    max_queue=16,
                    ssl=ssl_context,
            ) as websocket:

                self.ws = websocket

                for symbol in symbols:
                    sub_bbo = {
                        "sub": f"market.{symbol}.bbo",
                        "id": f"bbo_{int(time.time())}_{symbol}"
                    }
                    await websocket.send(json.dumps(sub_bbo))
                    await asyncio.sleep(0.1)

                while True:
                    try:
                        message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=25
                        )

                        response = await self._handle_message(message)

                        if response:
                            await websocket.send(response)

                    except asyncio.TimeoutError:
                        ping_msg = {"op": "ping", "ts": int(time.time() * 1000)}
                        await websocket.send(json.dumps(ping_msg))
                        continue

                    except websockets.exceptions.ConnectionClosed:
                        self.reconnect_count += 1
                        break

        except Exception:
            return False

        return True

    async def run(self, symbols: list):
        while True:
            try:
                success = await self.connect(symbols)

                if not success:
                    wait_time = min(2 ** self.reconnect_count, 30)
                    await asyncio.sleep(wait_time)
                else:
                    self.reconnect_count = 0

            except KeyboardInterrupt:
                break
            except Exception:
                await asyncio.sleep(5)

    async def place_market_order(self, symbol: str, side: str, volume: float, leverage: int = 10) -> dict:
        """Open a position on HTX"""
        if not self.api_key or self.api_key == "YOUR_HTX_ACCESS_KEY":
            logger.warning(f"HTX API keys not set, skipping order")
            return {"error": "API key not configured"}

        try:
            actual_volume = volume
            if symbol in self.contract_multipliers:
                multiplier = self.contract_multipliers[symbol]
                if multiplier == 0:
                    logger.warning(f"HTX {symbol}: multiplier = 0, using original volume")
                elif multiplier != 1.0:
                    actual_volume = volume / multiplier
                    logger.debug(f"HTX {symbol}: volume {volume} / multiplier {multiplier} = {actual_volume}")

            htx_contracts = int(actual_volume)
            if htx_contracts < 1:
                logger.error(
                    f"HTX {symbol}: 0 contracts (tokens={actual_volume:.4f}, mult={self.contract_multipliers.get(symbol,1)}). "
                    f"Increase position_usdt."
                )
                return {"error": "zero contracts"}

            logger.info(f"Sending HTX order: {side.upper()} {htx_contracts} contracts {symbol}")

            endpoint = '/linear-swap-api/v1/swap_order'
            order_params = {
                'contract_code': symbol,
                'client_order_id': int(time.time() * 1000000),
                'direction': side,
                'offset': 'open',
                'lever_rate': leverage,
                'volume': htx_contracts,
                'order_price_type': 'optimal_20'
            }

            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
            sign_params = {
                'AccessKeyId': self.api_key,
                'SignatureMethod': 'HmacSHA256',
                'SignatureVersion': '2',
                'Timestamp': timestamp,
            }
            sign_params.update(order_params)

            sorted_params = sorted(sign_params.items())
            query_string = urlencode(sorted_params)

            # BUG FIX: sign string must use actual newlines between components
            sign_str = f"POST\napi.hbdm.com\n{endpoint}\n{query_string}"
            signature = hmac.new(
                self.secret_key.encode('utf-8'),
                sign_str.encode('utf-8'),
                hashlib.sha256
            ).digest()
            signature_b64 = base64.b64encode(signature).decode()

            sign_params['Signature'] = signature_b64

            url = f"https://api.hbdm.com{endpoint}"
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }

            try:
                connector = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.post(
                        url,
                        params=sign_params,
                        json=order_params,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=5, connect=3)
                    ) as resp:
                        text = await resp.text()
                        result = json.loads(text)

                        if result.get('status') == 'ok':
                            logger.info(f"HTX {side.upper()} OK: {result.get('data')}")
                            return result
                        elif 'Verification failure' in text or 'fail' in text.lower():
                            logger.warning(f"HTX verification failed, trying alternative method...")
                            return await self._place_order_alt(symbol, side, int(volume))
                        else:
                            logger.error(f"HTX Error: {result.get('err_msg', result)}")
                            return result

            except Exception as e:
                logger.warning(f"Method 1 failed, trying method 2: {e}")
                return await self._place_order_alt(symbol, side, int(volume))

        except Exception as e:
            logger.error(f"HTX Order Error: {e}")
            traceback.print_exc()
            return {"error": str(e)}

    async def _place_order_alt(self, symbol: str, side: str, volume: int) -> dict:
        """Alternative order method (POST request with different auth placement)"""
        try:
            logger.info(f"Attempt 2: POST request for {side.upper()} {volume} {symbol}")

            endpoint = '/linear-swap-api/v1/swap_order'

            params = {
                'contract_code': symbol,
                'client_order_id': int(time.time() * 1000000),
                'direction': side,
                'offset': 'open',
                'lever_rate': 10,
                'volume': volume,
                'order_price_type': 'optimal_20'
            }

            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
            sign_params = {
                'AccessKeyId': self.api_key,
                'SignatureMethod': 'HmacSHA256',
                'SignatureVersion': '2',
                'Timestamp': timestamp,
            }
            sign_params.update(params)

            sorted_params = sorted(sign_params.items())
            query_string = urlencode(sorted_params)

            # BUG FIX: sign string uses POST (order placement is always POST)
            sign_str = f"POST\napi.hbdm.com\n{endpoint}\n{query_string}"
            signature = hmac.new(
                self.secret_key.encode('utf-8'),
                sign_str.encode('utf-8'),
                hashlib.sha256
            ).digest()
            signature_b64 = base64.b64encode(signature).decode()

            sign_params['Signature'] = signature_b64

            url = f"https://api.hbdm.com{endpoint}"
            headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(
                    url,
                    params=sign_params,
                    json=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    text = await resp.text()
                    result = json.loads(text)

                    if result.get('status') == 'ok':
                        logger.info(f"HTX {side.upper()} OK (method 2): {result.get('data')}")
                        return result
                    else:
                        logger.error(f"HTX Error (method 2): {result.get('err_msg', result)}")
                        return result

        except Exception as e:
            logger.error(f"HTX Order Error (method 2): {e}")
            return {"error": str(e)}


class OKXFuturesWebSocket:
    """WebSocket client for OKX futures"""

    WS_ENDPOINTS = {
        'public': [
            "wss://wseea.okx.com:8443/ws/v5/public",
            "wss://ws.okx.com:8443/ws/v5/public",
        ]
    }

    REST_ENDPOINTS = [
        "https://www.okx.com",
        "https://eea.okx.com",
    ]

    def __init__(self, api_key: Optional[str] = None, secret_key: Optional[str] = None,
                 passphrase: Optional[str] = None):

        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

        self.ws_public = None
        self.ws_private = None
        self.authenticated = False

        self.price_cache: Dict[str, Dict] = {}
        self.positions_cache: Dict[str, Dict] = {}
        self.orders_cache: Dict[str, Dict] = {}
        self.price_callbacks: Dict[str, Callable] = {}
        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.contract_multipliers: Dict[str, float] = {}

        self.message_count = 0
        self.ping_count = 0
        self.last_ping_time = 0

        self.start_time = time.time()
        self.reconnect_count = 0
        self.current_endpoint_idx = 0

        self.latency_history = []

    def _get_timestamp(self) -> str:
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    async def get_contract_multipliers(self, symbols: list):
        """Fetch contract multipliers for all symbols"""
        try:
            url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json()

                    if data.get('code') == '0':
                        logger.info(f"OKX Multipliers:")
                        for instrument in data.get('data', []):
                            inst_id = instrument.get('instId')
                            multiplier = float(instrument.get('ctVal', 1))

                            symbol = inst_id.replace('-SWAP', '')

                            if symbol in symbols or inst_id in symbols:
                                self.contract_multipliers[inst_id] = multiplier
                                logger.info(f"   {inst_id}: multiplier = {multiplier}")
                    else:
                        logger.warning(f"Could not fetch OKX multipliers")
        except Exception as e:
            logger.error(f"Error fetching OKX multipliers: {e}")

    def _sign_message(self, timestamp: str, message: str) -> str:
        if not self.secret_key:
            return ""

        payload = timestamp + "GET" + "/users/self/verify"
        signature = hmac.new(
            self.secret_key.encode(),
            payload.encode(),
            hashlib.sha256
        ).digest()
        return base64.b64encode(signature).decode()

    async def _send_rest_request(self, method: str, path: str, data: Optional[dict] = None) -> dict:
        if not self.api_key or not self.secret_key:
            return {"error": "API credentials not configured"}

        timestamp = self._get_timestamp()

        headers = {
            'OK-ACCESS-KEY': self.api_key,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.passphrase or '',
            'Content-Type': 'application/json'
        }

        if method == 'GET':
            request_path = path
            if data:
                request_path += '?' + urlencode(data)
            body = ''
        else:
            body = json.dumps(data) if data else ''
            request_path = path

        message = timestamp + method + request_path + body
        signature = hmac.new(
            self.secret_key.encode() if self.secret_key else b'',
            message.encode(),
            hashlib.sha256
        ).digest()
        signature_b64 = base64.b64encode(signature).decode()
        headers['OK-ACCESS-SIGN'] = signature_b64

        url = self.REST_ENDPOINTS[0] + request_path

        try:
            async with aiohttp.ClientSession() as session:
                if method == 'GET':
                    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        return await resp.json()
                else:
                    async with session.post(url, headers=headers, json=data, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        return await resp.json()
        except Exception as e:
            logger.error(f"OKX API Error: {e}")
            return {}

    async def _handle_public_message(self, message):
        try:
            if isinstance(message, bytes):
                message = message.decode('utf-8')

            data = json.loads(message)

            if data.get('event') == 'ping':
                self.ping_count += 1
                self.last_ping_time = time.time()
                return json.dumps({"event": "pong"})

            if data.get('event') in ('subscribe', 'error', 'data'):
                return None

            if data.get('data'):
                arg = data.get('arg', {})
                channel = arg.get('channel')
                inst_id = arg.get('instId')

                if not channel or not inst_id:
                    return None

                tick_data = data['data']
                if isinstance(tick_data, list):
                    tick_data = tick_data[0]

                ts = int(data.get('ts', int(time.time() * 1000)))
                client_ts = int(time.time() * 1000)
                latency = client_ts - ts

                self.message_count += 1
                self.latency_history.append(latency)

                if len(self.latency_history) > 1000:
                    self.latency_history = self.latency_history[-1000:]

                if channel == 'tickers':
                    last_price = float(tick_data.get('last', 0))
                    bid = float(tick_data.get('bidPx', 0))
                    ask = float(tick_data.get('askPx', 0))
                    bid_sz = float(tick_data.get('bidSz', 0))
                    ask_sz = float(tick_data.get('askSz', 0))
                    vol_24h = float(tick_data.get('vol24h', 0))

                    mid_price = (bid + ask) * 0.5 if (bid + ask) > 0 else 0
                    spread = ask - bid
                    spread_pct = (spread / bid) * 100 if bid > 0 else 0

                    self.price_cache[inst_id] = {
                        'last': last_price,
                        'bid': bid,
                        'ask': ask,
                        'mid_price': mid_price,
                        'bid_size': bid_sz,
                        'ask_size': ask_sz,
                        'spread': spread,
                        'spread_pct': spread_pct,
                        'vol_24h': vol_24h,
                        'timestamp': client_ts,
                        'server_ts': ts,
                        'latency_ms': latency,
                        'type': 'okx_ticker',
                        'exchange': 'OKX'
                    }

                    if inst_id in self.price_callbacks:
                        callback = self.price_callbacks[inst_id]
                        if asyncio.iscoroutinefunction(callback):
                            asyncio.create_task(callback(inst_id, self.price_cache[inst_id]))
                        else:
                            callback(inst_id, self.price_cache[inst_id])

            return None

        except Exception:
            return None

    async def _handle_private_message(self, message):
        try:
            if isinstance(message, bytes):
                message = message.decode('utf-8')

            data = json.loads(message)

            if data.get('event') == 'ping':
                self.ping_count += 1
                return json.dumps({"event": "pong"})

            if data.get('event') == 'login':
                return None

            if data.get('data'):
                arg = data.get('arg', {})
                channel = arg.get('channel')

                if channel == 'orders':
                    for order in data['data']:
                        order_id = order.get('ordId')
                        self.orders_cache[order_id] = {
                            'ordId': order_id,
                            'clOrdId': order.get('clOrdId'),
                            'instId': order.get('instId'),
                            'state': order.get('state'),
                            'side': order.get('side'),
                            'ordType': order.get('ordType'),
                            'px': float(order.get('px', 0)),
                            'sz': float(order.get('sz', 0)),
                            'accFillSz': float(order.get('accFillSz', 0)),
                            'avgPx': float(order.get('avgPx', 0)),
                            'fee': float(order.get('fee', 0)),
                            'timestamp': int(order.get('uTime', 0))
                        }

                elif channel == 'positions':
                    for pos in data['data']:
                        inst_id = pos.get('instId')
                        self.positions_cache[inst_id] = {
                            'instId': inst_id,
                            'posSide': pos.get('posSide'),
                            'pos': float(pos.get('pos', 0)),
                            'avgPx': float(pos.get('avgPx', 0)),
                            'unrealPnl': float(pos.get('unrealPnl', 0)),
                            'realizedPnl': float(pos.get('realizedPnl', 0)),
                            'markPx': float(pos.get('markPx', 0)),
                            'leverage': float(pos.get('lever', 0)),
                            'mgnRatio': float(pos.get('mgnRatio', 0)),
                            'timestamp': int(pos.get('uTime', 0))
                        }

            return None

        except Exception:
            return None

    async def connect_public(self, instruments: List[str]):

        endpoint_idx = 0
        while endpoint_idx < len(self.WS_ENDPOINTS['public']):
            endpoint = self.WS_ENDPOINTS['public'][endpoint_idx]

            try:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

                async with websockets.connect(
                        endpoint,
                        ping_interval=25,
                        ping_timeout=10,
                        close_timeout=5,
                        ssl=ssl_context,
                ) as websocket:

                    self.ws_public = websocket

                    if not self.ws_private and self.api_key and self.api_key != "YOUR_OKX_API_KEY":
                        try:
                            await self._connect_private_ws()
                        except Exception as e:
                            logger.warning(f"OKX private WebSocket not connected: {e}")

                    for inst_id in instruments:
                        try:
                            sub_ticker = {
                                "op": "subscribe",
                                "args": [{"channel": "tickers", "instId": inst_id}]
                            }
                            await websocket.send(json.dumps(sub_ticker))
                            await asyncio.sleep(0.01)
                        except Exception:
                            pass

                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=30)
                            response = await self._handle_public_message(message)
                            if response:
                                try:
                                    await websocket.send(response)
                                except Exception:
                                    pass

                        except asyncio.TimeoutError:
                            try:
                                await websocket.send(json.dumps({"op": "ping"}))
                            except Exception:
                                break
                            continue

                        except websockets.exceptions.ConnectionClosed:
                            self.reconnect_count += 1
                            break
                        except Exception:
                            break

                    endpoint_idx += 1

            except (asyncio.TimeoutError, websockets.exceptions.WebSocketException, Exception):
                endpoint_idx += 1
                await asyncio.sleep(1)

        return False

    async def _connect_private_ws(self):
        """Connect private WebSocket for OKX orders"""
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            self.ws_private = await websockets.connect(
                'wss://ws.okx.com:8443/ws/v5/private',
                ping_interval=25,
                ping_timeout=10,
                close_timeout=5,
                ssl=ssl_context,
            )

            timestamp = str(int(time.time()))
            sign_str = timestamp + 'GET' + '/users/self/verify'
            signature = base64.b64encode(
                hmac.new(
                    self.secret_key.encode('utf-8'),
                    sign_str.encode('utf-8'),
                    hashlib.sha256
                ).digest()
            ).decode()

            auth_msg = {
                'op': 'login',
                'args': [{
                    'apiKey': self.api_key,
                    'passphrase': self.passphrase,
                    'timestamp': timestamp,
                    'sign': signature
                }]
            }

            await self.ws_private.send(json.dumps(auth_msg))

            response = await asyncio.wait_for(self.ws_private.recv(), timeout=5)
            result = json.loads(response)

            if result.get('event') == 'login' and result.get('code') == '0':
                self.authenticated = True
                logger.info(f"OKX private WebSocket connected and authenticated")
                asyncio.create_task(self._handle_private_ws_messages())
            else:
                logger.error(f"OKX authentication failed: {result}")
                if self.ws_private:
                    await self.ws_private.close()
                    self.ws_private = None

        except Exception as e:
            logger.error(f"OKX private WebSocket error: {e}")
            self.ws_private = None
            self.authenticated = False

    async def _handle_private_ws_messages(self):
        """Handle messages from OKX private WebSocket"""
        try:
            while self.ws_private and self.authenticated:
                try:
                    message = await asyncio.wait_for(self.ws_private.recv(), timeout=30)
                    data = json.loads(message)

                    logger.debug(f"OKX private WS: {data}")

                    if data.get('op') == 'ping':
                        await self.ws_private.send(json.dumps({'op': 'pong'}))
                        continue

                    if data.get('op') == 'order':
                        req_id = data.get('id')
                        logger.debug(f"OKX order response: ID={req_id}, code={data.get('code')}")

                        if req_id and req_id in self.pending_requests:
                            future = self.pending_requests.pop(req_id)
                            if not future.done():
                                future.set_result(data)
                        else:
                            logger.warning(f"OKX: unknown order ID={req_id}")

                except asyncio.TimeoutError:
                    await self.ws_private.send(json.dumps({'op': 'ping'}))
                    continue
                except json.JSONDecodeError as e:
                    logger.warning(f"OKX JSON error: {e}")
                    continue
                except Exception as e:
                    logger.warning(f"OKX WebSocket error: {e}")
                    break
        except Exception as e:
            logger.error(f"OKX private WebSocket ended: {e}")
            self.authenticated = False

    async def set_leverage(self, inst_id: str, leverage: int = 10) -> dict:
        """Set leverage for an instrument on OKX"""
        try:
            logger.debug(f"Setting leverage {leverage}x for {inst_id} on OKX")
            data = {
                "instId": inst_id,
                "lever": str(leverage),
                "mgnMode": "cross"
            }
            response = await self._send_rest_request('POST', '/api/v5/account/set-leverage', data)
            if response.get('code') == '0':
                logger.debug(f"Leverage {leverage}x set for {inst_id}")
                return response
            else:
                logger.warning(f"Could not set leverage: {response}")
                return response
        except Exception as e:
            logger.error(f"Error setting leverage: {e}")
            return {"error": str(e)}

    async def place_market_order(self, inst_id: str, side: str, contract_qty: float) -> dict:
        """Place a market order on OKX via WebSocket"""
        if not self.api_key or not self.secret_key or self.api_key == "YOUR_OKX_API_KEY":
            logger.warning(f"OKX API credentials not configured, skipping order for {inst_id}")
            return {"error": "OKX API credentials not configured"}

        try:
            actual_qty = contract_qty
            if inst_id in self.contract_multipliers:
                multiplier = self.contract_multipliers[inst_id]
                if multiplier == 0:
                    logger.warning(f"OKX {inst_id}: multiplier = 0, using original volume")
                elif multiplier != 1.0:
                    actual_qty = contract_qty / multiplier
                    logger.debug(f"OKX {inst_id}: volume {contract_qty} / multiplier {multiplier} = {actual_qty}")

            logger.info(f"OKX Order (WebSocket): {side.upper()} {actual_qty} {inst_id}")

            if not self.ws_private or not self.authenticated:
                logger.warning(f"OKX private WebSocket not connected, trying REST fallback")
                return await self._place_order_rest(inst_id, side, actual_qty)

            req_id = str(int(time.time() * 1000000))
            contract_qty_int = int(actual_qty)
            if contract_qty_int < 1:
                logger.error(
                    f"OKX {inst_id}: 0 contracts (tokens={actual_qty:.4f}, mult={self.contract_multipliers.get(inst_id,1)}). "
                    f"Increase position_usdt."
                )
                return {"error": "zero contracts"}

            order_msg = {
                'id': req_id,
                'op': 'order',
                'args': [{
                    'instId': inst_id,
                    'tdMode': 'cross',
                    'side': side,
                    'posSide': 'long' if side == 'buy' else 'short',
                    'ordType': 'market',
                    'sz': str(contract_qty_int)
                }]
            }

            logger.debug(f"Sending OKX order: {order_msg}")

            future = asyncio.Future()
            self.pending_requests[req_id] = future

            await self.ws_private.send(json.dumps(order_msg))

            result = await asyncio.wait_for(future, timeout=5)

            logger.debug(f"OKX response: {result}")

            if result.get('code') == '0':
                order_id = result.get('data', [{}])[0].get('ordId', 'N/A')
                logger.info(f"OKX {side.upper()} OK: ID={order_id}")
                return result
            else:
                s_data = result.get('data', [{}])[0] if result.get('data') else {}
                s_code = s_data.get('sCode', result.get('code', ''))
                s_msg = s_data.get('sMsg', '') or result.get('msg', '') or str(result)
                logger.error(f"OKX WebSocket [{s_code}]: {s_msg} — switching to REST")
                return await self._place_order_rest(inst_id, side, actual_qty)

        except asyncio.TimeoutError:
            logger.warning(f"OKX WebSocket timeout, trying REST fallback")
            return await self._place_order_rest(inst_id, side, actual_qty)
        except Exception as e:
            logger.error(f"OKX Order Error: {e}")
            traceback.print_exc()
            return {"error": str(e)}

    async def _place_order_rest(self, inst_id: str, side: str, actual_qty: float) -> dict:
        """REST fallback — actual_qty is already the contract count (after multiplier division)"""
        try:
            contracts = max(1, int(actual_qty))
            logger.info(f"OKX Order (REST fallback): {side.upper()} {contracts} contracts {inst_id}")

            order_data = {
                "instId": inst_id,
                "tdMode": "cross",
                "side": side,
                "ordType": "market",
                "sz": str(contracts),
                "clOrdId": f"okx_{int(time.time() * 1000)}",
                "posSide": "long" if side == "buy" else "short",
            }

            response = await self._send_rest_request('POST', '/api/v5/trade/order', order_data)

            if response.get('code') == '0':
                logger.info(f"OKX {side.upper()} OK (REST): {response.get('msg')}")
                return response
            else:
                logger.error(f"OKX REST Error: {response.get('msg', response)}")
                return response
        except Exception as e:
            logger.error(f"OKX REST Order Error: {e}")
            return {"error": str(e)}

    async def run(self, instruments: List[str]):
        while True:
            try:
                success = await self.connect_public(instruments)

                if not success:
                    wait_time = min(2 ** self.reconnect_count, 30)
                    await asyncio.sleep(wait_time)
                    self.reconnect_count += 1
                else:
                    self.reconnect_count = 0

            except KeyboardInterrupt:
                break
            except Exception:
                await asyncio.sleep(5)


class TradeManager:
    """Real trade manager with arbitrage logic"""

    def __init__(self, htx_bot, okx_bot):
        self.htx_bot = htx_bot
        self.okx_bot = okx_bot
        self.active_trades: Dict[str, dict] = {}
        self.opening_trades: set = set()

        self.position_usdt = 0.5
        self.leverage = 10

        self.active_htx_trades = 0
        self.active_okx_trades = 0
        self.max_trades_per_exchange = 2

        self.total_open_trades = 0
        self.closing_trades: set = set()
        self.failed_symbols: Dict[str, float] = {}
        self.fail_cooldown = 30.0

        self.trades_lock = None

    async def initialize_async_components(self):
        """Initialize async components (Lock, etc.)"""
        self.trades_lock = asyncio.Lock()
        logger.debug("Async components initialized")

    async def _auto_close_position(self, symbol: str, delay_seconds: int):
        """Automatically close a position after N seconds"""
        try:
            await asyncio.sleep(delay_seconds)

            if symbol not in self.active_trades:
                return

            trade = self.active_trades[symbol]
            okx_symbol = symbol + '-SWAP'

            logger.info(f"Auto-closing: {symbol}")

            if trade['direction'] == "LONG HTX / SHORT OKX":
                await self.htx_bot.place_market_order(symbol, 'sell', trade['qty'], self.leverage)
                await asyncio.sleep(0.5)
                await self.okx_bot.place_market_order(okx_symbol, 'buy', trade['qty'])
            else:
                await self.htx_bot.place_market_order(symbol, 'buy', trade['qty'], self.leverage)
                await asyncio.sleep(0.5)
                await self.okx_bot.place_market_order(okx_symbol, 'sell', trade['qty'])

            logger.info(f"Position {symbol} closed automatically")

            if symbol in self.active_trades:
                del self.active_trades[symbol]
                # BUG FIX: decrement counters when auto-closing
                self.active_htx_trades = max(0, self.active_htx_trades - 1)
                self.active_okx_trades = max(0, self.active_okx_trades - 1)
                self.total_open_trades = max(0, self.total_open_trades - 1)

        except Exception as e:
            logger.error(f"Error during auto-close {symbol}: {e}")

    async def _monitor_okx_connection(self):
        """Monitor OKX WebSocket and reconnect if disconnected"""
        while True:
            try:
                await asyncio.sleep(5)

                if not self.okx_bot.ws_private or not self.okx_bot.authenticated:
                    logger.error("CRITICAL: OKX WebSocket disconnected! Attempting reconnect...")

                    try:
                        await self.okx_bot._connect_private_ws()
                        logger.info("OKX WebSocket reconnected")
                    except Exception as e:
                        logger.error(f"Could not reconnect OKX: {e}")

            except Exception as e:
                logger.error(f"Error in WebSocket monitor: {e}")
                await asyncio.sleep(5)

    def calculate_position_size(self, symbol: str, price: float) -> float:
        """Calculate position size in contracts"""
        contract_qty = (self.position_usdt * self.leverage) / price
        return round(contract_qty, 4)

    async def process_symbol(self, symbol: str):
        """Process a symbol for arbitrage with real trades"""

        if self.trades_lock is None:
            logger.error("CRITICAL: trades_lock not initialized! "
                         "Call initialize_async_components() first!")
            return

        okx_symbol = symbol + '-SWAP'

        htx_price = self.htx_bot.price_cache.get(symbol)
        okx_price = self.okx_bot.price_cache.get(okx_symbol)

        if not htx_price or not okx_price:
            return

        if htx_price.get('type') != 'linear_bbo' or okx_price.get('type') != 'okx_ticker':
            return

        htx_mid = htx_price.get('mid_price', 0)
        okx_mid = okx_price.get('mid_price', 0)

        if htx_mid <= 0 or okx_mid <= 0:
            return

        spread_percent = abs((okx_mid - htx_mid) / htx_mid) * 100

        if symbol in self.active_trades:
            if spread_percent <= 0.1:
                if spread_percent < 0.05:
                    logger.debug(f"{symbol}: spread {spread_percent:.4f}% too narrow, wait for 0.05-0.1%")
                    return

                async with self.trades_lock:
                    if symbol in self.closing_trades:
                        return
                    self.closing_trades.add(symbol)

                trade = self.active_trades[symbol]
                profit_spread = trade['entry_spread'] - spread_percent
                hold_time = time.time() - trade['entry_time']
                direction = trade['direction']
                qty = trade['qty']

                logger.info(
                    f"CLOSE: {symbol} | Spread: {spread_percent:.4f}% | Profit spread: {profit_spread:.4f}% | Hold: {hold_time:.1f}s"
                )

                try:
                    if direction == "LONG HTX / SHORT OKX":
                        results = await asyncio.wait_for(
                            asyncio.gather(
                                self.htx_bot.place_market_order(symbol, 'sell', qty, self.leverage),
                                self.okx_bot.place_market_order(okx_symbol, 'buy', qty),
                                return_exceptions=True
                            ),
                            timeout=5.0
                        )
                    else:
                        results = await asyncio.wait_for(
                            asyncio.gather(
                                self.htx_bot.place_market_order(symbol, 'buy', qty, self.leverage),
                                self.okx_bot.place_market_order(okx_symbol, 'sell', qty),
                                return_exceptions=True
                            ),
                            timeout=5.0
                        )

                    if isinstance(results[0], Exception) or (isinstance(results[0], dict) and results[0].get('status') != 'ok'):
                        logger.error(f"HTX close error {symbol}: {results[0]}")
                    if isinstance(results[1], Exception) or (isinstance(results[1], dict) and results[1].get('code') not in ('0', None)):
                        logger.error(f"OKX close error {symbol}: {results[1]}")

                    logger.info(f"Close orders sent for {symbol}")
                except asyncio.TimeoutError:
                    logger.error(f"TIMEOUT closing {symbol} (5s)")
                except Exception as e:
                    logger.error(f"Error closing {symbol}: {e}")
                finally:
                    if symbol in self.active_trades:
                        del self.active_trades[symbol]
                    self.closing_trades.discard(symbol)
                    self.active_htx_trades = max(0, self.active_htx_trades - 1)
                    self.active_okx_trades = max(0, self.active_okx_trades - 1)
                    self.total_open_trades = max(0, self.total_open_trades - 1)
            else:
                logger.debug(f"{symbol}: position open (spread {spread_percent:.4f}%), waiting for <= 0.1%")

            return

        if spread_percent >= 1.0:
            fail_ts = self.failed_symbols.get(symbol)
            if fail_ts and time.time() - fail_ts < self.fail_cooldown:
                return

            async with self.trades_lock:
                if symbol in self.opening_trades:
                    logger.debug(f"{symbol}: position already opening, skipping")
                    return

                if self.active_htx_trades >= self.max_trades_per_exchange:
                    logger.debug(f"HTX: max {self.max_trades_per_exchange} trades open, skipping")
                    return

                if self.active_okx_trades >= self.max_trades_per_exchange:
                    logger.debug(f"OKX: max {self.max_trades_per_exchange} trades open, skipping")
                    return

                if self.total_open_trades >= 4:
                    logger.debug(f"Max 4 trades open ({self.total_open_trades}), skipping")
                    return

                htx_price_fresh = self.htx_bot.price_cache.get(symbol)
                okx_price_fresh = self.okx_bot.price_cache.get(okx_symbol)

                if not htx_price_fresh or not okx_price_fresh:
                    return

                htx_mid_fresh = htx_price_fresh.get('mid_price', 0)
                okx_mid_fresh = okx_price_fresh.get('mid_price', 0)

                if htx_mid_fresh <= 0 or okx_mid_fresh <= 0:
                    return

                spread_percent_fresh = abs((okx_mid_fresh - htx_mid_fresh) / htx_mid_fresh) * 100

                if not (0 <= spread_percent_fresh <= 1000):
                    logger.warning(f"{symbol}: spread {spread_percent_fresh}% invalid")
                    return

                if spread_percent_fresh < 1.0:
                    logger.debug(f"{symbol}: spread dropped to {spread_percent_fresh:.4f}%, no longer profitable")
                    return

                htx_qty = self.calculate_position_size(symbol, htx_mid_fresh)
                okx_qty = self.calculate_position_size(symbol, okx_mid_fresh)
                qty = min(htx_qty, okx_qty)

                if qty <= 0:
                    logger.warning(f"Position size {symbol} <= 0 ({qty}), skipping")
                    return

                htx_mult = self.htx_bot.contract_multipliers.get(symbol, 1.0) or 1.0
                okx_mult = self.okx_bot.contract_multipliers.get(okx_symbol, 1.0) or 1.0
                if int(qty / htx_mult) < 1 or int(qty / okx_mult) < 1:
                    logger.debug(
                        f"{symbol}: min contract not reached "
                        f"(HTX={qty/htx_mult:.3f}, OKX={qty/okx_mult:.3f} contracts). "
                        f"Increase position_usdt (current={self.position_usdt})."
                    )
                    return

                direction = "LONG HTX / SHORT OKX" if okx_mid_fresh > htx_mid_fresh else "SHORT HTX / LONG OKX"

                self.active_trades[symbol] = {
                    'entry_spread': spread_percent_fresh,
                    'entry_time': time.time(),
                    'htx_price': htx_mid_fresh,
                    'okx_price': okx_mid_fresh,
                    'qty': qty,
                    'direction': direction,
                    'status': 'opening'
                }

                self.opening_trades.add(symbol)
                self.active_htx_trades += 1
                self.active_okx_trades += 1
                self.total_open_trades += 1

            trade = self.active_trades[symbol]
            direction = trade['direction']
            qty = trade['qty']

            spread_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            position_value = qty * htx_mid_fresh

            logger.info(f"OPEN: {symbol} | Spread: {spread_percent_fresh:.4f}%")
            logger.info(f"   Time: {spread_time}")
            logger.info(f"   Volume: {qty:.0f} contracts (~{position_value:.2f} USDT)")
            if direction == "LONG HTX / SHORT OKX":
                logger.info(f"   BUY {qty} on HTX @ {htx_mid_fresh}")
                logger.info(f"   SELL {qty} on OKX @ {okx_mid_fresh}")
            else:
                logger.info(f"   SELL {qty} on HTX @ {htx_mid_fresh}")
                logger.info(f"   BUY {qty} on OKX @ {okx_mid_fresh}")

            order_start = time.time()
            try:
                if direction == "LONG HTX / SHORT OKX":
                    results = await asyncio.wait_for(
                        asyncio.gather(
                            self.htx_bot.place_market_order(symbol, 'buy', qty, self.leverage),
                            self.okx_bot.place_market_order(okx_symbol, 'sell', qty),
                            return_exceptions=True
                        ),
                        timeout=5.0
                    )
                else:
                    results = await asyncio.wait_for(
                        asyncio.gather(
                            self.htx_bot.place_market_order(symbol, 'sell', qty, self.leverage),
                            self.okx_bot.place_market_order(okx_symbol, 'buy', qty),
                            return_exceptions=True
                        ),
                        timeout=5.0
                    )

                htx_result = results[0]
                okx_result = results[1]

                htx_failed = isinstance(htx_result, Exception) or (
                    isinstance(htx_result, dict) and htx_result.get('status') != 'ok'
                )
                okx_failed = isinstance(okx_result, Exception) or (
                    isinstance(okx_result, dict) and okx_result.get('code') not in ('0', None)
                )

                if htx_failed:
                    logger.error(f"HTX ERROR: {htx_result}")
                    self.failed_symbols[symbol] = time.time()
                    async with self.trades_lock:
                        if symbol in self.active_trades:
                            del self.active_trades[symbol]
                        self.opening_trades.discard(symbol)
                        self.active_htx_trades = max(0, self.active_htx_trades - 1)
                        self.active_okx_trades = max(0, self.active_okx_trades - 1)
                        self.total_open_trades = max(0, self.total_open_trades - 1)
                    if not okx_failed:
                        logger.warning(f"OKX succeeded but HTX failed - positions desynchronized!")
                    return

                if okx_failed:
                    logger.error(f"OKX ERROR: {okx_result}")
                    logger.warning(f"HTX succeeded but OKX failed - positions desynchronized!")
                    self.failed_symbols[symbol] = time.time()
                    async with self.trades_lock:
                        if symbol in self.active_trades:
                            del self.active_trades[symbol]
                        self.opening_trades.discard(symbol)
                        self.active_htx_trades = max(0, self.active_htx_trades - 1)
                        self.active_okx_trades = max(0, self.active_okx_trades - 1)
                        self.total_open_trades = max(0, self.total_open_trades - 1)
                    return

            except asyncio.TimeoutError:
                logger.error(f"TIMEOUT sending orders {symbol} (5s)")
                self.failed_symbols[symbol] = time.time()
                async with self.trades_lock:
                    if symbol in self.active_trades:
                        del self.active_trades[symbol]
                    self.opening_trades.discard(symbol)
                    self.active_htx_trades = max(0, self.active_htx_trades - 1)
                    self.active_okx_trades = max(0, self.active_okx_trades - 1)
                    self.total_open_trades = max(0, self.total_open_trades - 1)
                return
            except Exception as e:
                logger.error(f"CRITICAL ERROR sending orders: {e}")
                self.failed_symbols[symbol] = time.time()
                async with self.trades_lock:
                    if symbol in self.active_trades:
                        del self.active_trades[symbol]
                    self.opening_trades.discard(symbol)
                    self.active_htx_trades = max(0, self.active_htx_trades - 1)
                    self.active_okx_trades = max(0, self.active_okx_trades - 1)
                    self.total_open_trades = max(0, self.total_open_trades - 1)
                return

            order_time_ms = (time.time() - order_start) * 1000
            logger.info(f"   Confirmed in {order_time_ms:.0f}ms")

            async with self.trades_lock:
                if symbol in self.active_trades:
                    self.active_trades[symbol]['status'] = 'opened'
                    self.opening_trades.discard(symbol)


async def main():
    """Start the combined client with real trading"""

    # Fill in your API keys here
    HTX_API_KEY = "YOUR_HTX_ACCESS_KEY"
    HTX_SECRET_KEY = "YOUR_HTX_SECRET_KEY"

    OKX_API_KEY = "YOUR_OKX_API_KEY"
    OKX_SECRET_KEY = "YOUR_OKX_SECRET_KEY"
    OKX_PASSPHRASE = "YOUR_OKX_PASSPHRASE"

    # Or use environment variables (recommended for production):
    # HTX_API_KEY = os.getenv('HTX_API_KEY', 'YOUR_HTX_ACCESS_KEY')
    # HTX_SECRET_KEY = os.getenv('HTX_SECRET_KEY', 'YOUR_HTX_SECRET_KEY')
    # OKX_API_KEY = os.getenv('OKX_API_KEY', 'YOUR_OKX_API_KEY')
    # OKX_SECRET_KEY = os.getenv('OKX_SECRET_KEY', 'YOUR_OKX_SECRET_KEY')
    # OKX_PASSPHRASE = os.getenv('OKX_PASSPHRASE', 'YOUR_OKX_PASSPHRASE')

    if HTX_API_KEY == "YOUR_HTX_ACCESS_KEY":
        logger.error("HTX API keys not set! Edit the script and add your keys.")
        return

    if OKX_API_KEY == "YOUR_OKX_API_KEY":
        logger.warning("OKX API keys not set! OKX orders will not work.")

    if OKX_API_KEY != "YOUR_OKX_API_KEY" and len(OKX_API_KEY) < 20:
        logger.error(f"OKX_API_KEY too short: {len(OKX_API_KEY)} chars")
        return

    htx_bot = HTXFutureWebSocketFixed(api_key=HTX_API_KEY, secret_key=HTX_SECRET_KEY)
    okx_bot = OKXFuturesWebSocket(api_key=OKX_API_KEY, secret_key=OKX_SECRET_KEY, passphrase=OKX_PASSPHRASE)
    trade_manager = TradeManager(htx_bot, okx_bot)

    await trade_manager.initialize_async_components()

    symbols_htx = [
        'GALA-USDT', 'GIGGLE-USDT', 'GMT-USDT', 'GRASS-USDT', 'GRT-USDT', 'H-USDT',
        'HBAR-USDT', 'HYPE-USDT', 'ICP-USDT', 'IMX-USDT', 'INJ-USDT', 'IP-USDT',
        'JUP-USDT', 'KAITO-USDT', 'KGEN-USDT', 'LA-USDT', 'LDO-USDT', 'LIGHT-USDT',
        'LINK-USDT', 'LIT-USDT', 'LPT-USDT', 'LTC-USDT', 'LUNA-USDT', 'MANA-USDT',
        'MASK-USDT', 'MEME-USDT', 'MERL-USDT', 'MOODENG-USDT', 'MOVE-USDT', 'NEAR-USDT',
        'OKB-USDT', 'ONDO-USDT', 'OP-USDT', 'ORDI-USDT', 'PENDLE-USDT', 'PENGU-USDT'
    ]

    # BUG FIX: define symbols_okx once from symbols_htx to avoid desync
    symbols_okx = [sym.replace('-USDT', '-USDT-SWAP') for sym in symbols_htx]

    logger.info("Fetching contract multipliers...")
    await htx_bot.get_contract_multipliers(symbols_htx)
    await okx_bot.get_contract_multipliers(symbols_okx)
    logger.info("Multipliers fetched, starting trading...")

    async def htx_on_price(symbol: str, data: dict):
        await trade_manager.process_symbol(symbol)

    async def okx_on_price(inst_id: str, data: dict):
        pass

    for symbol in symbols_htx:
        htx_bot.price_callbacks[symbol] = htx_on_price

    for inst_id in symbols_okx:
        okx_bot.price_callbacks[inst_id] = okx_on_price

    logger.info(f"Starting REAL arbitrage between HTX and OKX...")
    logger.info(f"Tracking {len(symbols_htx)} pairs")
    logger.info(f"Position size: {trade_manager.position_usdt} USDT")
    logger.info(f"Leverage: {trade_manager.leverage}x")
    logger.info(f"Open at spread >= 1.0%")
    logger.info(f"Close at spread <= 0.1%")
    logger.info(f"WARNING: REAL TRADES - MARKET ORDERS")

    async def safe_run_htx():
        try:
            await htx_bot.run(symbols_htx)
        except Exception as e:
            logger.error(f"CRITICAL HTX ERROR: {e}")
            raise

    async def safe_run_okx():
        try:
            await okx_bot.run(symbols_okx)
        except Exception as e:
            logger.error(f"CRITICAL OKX ERROR: {e}")
            raise

    htx_task = asyncio.create_task(safe_run_htx())
    okx_task = asyncio.create_task(safe_run_okx())
    monitor_task = asyncio.create_task(trade_manager._monitor_okx_connection())

    await asyncio.gather(htx_task, okx_task, monitor_task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down")
