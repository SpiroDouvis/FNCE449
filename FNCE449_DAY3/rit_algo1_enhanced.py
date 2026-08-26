"""
RIT ALGO1 — enhanced cross-exchange arbitrage bot.

Improvements over the starter algorithm:
  1. One GET /v1/securities per scan instead of two GET /v1/securities/book calls
     (also yields position, trading_fee, max_trade_size for free).
  2. No sleep(1). The loop runs as fast as the rate limiter allows.
  3. Fee-aware trigger: trades only when the spread clears BOTH commissions.
  4. Size to the top of book (and to the gross limit), never a blind 1000 lots.
  5. Both legs fire concurrently on separate keep-alive connections.
  6. Marketable limit orders: any remainder resting after 1s unfilled is cancelled.
  7. Partial-fill reconciliation: any naked residual is flattened immediately.
  8. Generic pair discovery (CRZY_M/CRZY_A, TAME_M/TAME_A, ... ) from the ticker list.
  9. Per-security 429 cooldowns using the wait / Retry-After values.
 10. Nothing raises out of the main loop; the bot survives transient API errors.
 11. End-of-case flatten so no inventory is carried into the scoring snapshot.

Usage:  python rit_algo1_enhanced.py
Stop:   CTRL+C (graceful — cancels resting orders and flattens)
"""

import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

try:                                    # marginally faster JSON parsing if present
    import orjson
    def _loads(raw): return orjson.loads(raw)
except ImportError:
    import json
    def _loads(raw): return json.loads(raw)


# ----------------------------------------------------------------------------
# CONFIG — check the API key and port against your RIT client before running
# ----------------------------------------------------------------------------
API_KEY = {'X-API-Key': 'FLI7E73K'}
BASE = 'http://localhost:9999/v1'

START_TICK = 5          # don't trade during the opening auction noise
STOP_TICK = 295         # stop opening new positions
FLATTEN_TICK = 290      # start unwinding inventory from here

EDGE_BUFFER = 0.00      # extra $/share required beyond commissions. Raise to
                        # 0.01-0.02 if you're getting picked off on the 2nd leg.
MAX_ORDER_SIZE = 5000   # hard cap per leg regardless of what the book shows
MIN_ORDER_SIZE = 50     # below this the fixed costs aren't worth the API call

USE_LIMIT_ORDERS = True # marketable limits (safe) vs MARKET (always fills)
CANCEL_GRACE = 1.0      # seconds a resting limit remainder gets before we cancel it
CANCEL_POLL = 0.2       # how often we check the order while waiting out the grace period
CASE_POLL_EVERY = 25    # scans between /v1/case refreshes
LIMITS_POLL_EVERY = 40  # scans between /v1/limits refreshes
GROSS_UTILISATION = 0.9 # only use this fraction of the gross limit

VERBOSE = False         # True prints every trade live (slower — costs latency)


# ----------------------------------------------------------------------------
# Plumbing
# ----------------------------------------------------------------------------
shutdown = False
_T0 = time.monotonic()
_local = threading.local()
_log = []
_cooldown = {}          # ticker -> monotonic timestamp until which it's throttled
_global_cooldown = 0.0


def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True


def log(msg):
    """Buffered logging — printing inside the hot loop is expensive."""
    entry = '[%7.2fs] %s' % (time.monotonic() - _T0, msg)
    _log.append(entry)
    if VERBOSE:
        print(entry)


def session():
    """One keep-alive Session per thread so concurrent legs don't contend."""
    s = getattr(_local, 'session', None)
    if s is None:
        s = requests.Session()
        s.headers.update(API_KEY)
        s.mount('http://', requests.adapters.HTTPAdapter(
            pool_connections=4, pool_maxsize=8, max_retries=0))
        _local.session = s
    return s


def api(method, path, params=None, ticker=None):
    """
    Single entry point for every API call.

    Returns the decoded body on success, or None on any failure. Never raises —
    a crash at tick 40 means zero P&L for the remaining 250 ticks.
    429s set a cooldown (per-security when we know which security) using the
    wait / Retry-After the API hands back, rather than blind-retrying.
    """
    global _global_cooldown
    try:
        resp = session().request(method, BASE + path, params=params, timeout=2.0)
    except requests.RequestException as exc:
        log('NETWORK %s %s: %s' % (method, path, exc))
        return None

    if resp.status_code == 200:
        try:
            return _loads(resp.content)
        except ValueError:
            return None

    if resp.status_code == 429:
        wait = 0.2
        try:
            wait = float(_loads(resp.content).get('wait', wait))
        except Exception:
            try:
                wait = float(resp.headers.get('Retry-After', wait))
            except (TypeError, ValueError):
                pass
        until = time.monotonic() + wait
        if ticker:
            _cooldown[ticker] = until      # other securities stay tradeable
        else:
            _global_cooldown = until
        return None

    if resp.status_code == 401:
        log('401 — API key mismatch with the RIT client. Fix API_KEY.')
    return None


def throttled(ticker):
    now = time.monotonic()
    return now < _global_cooldown or now < _cooldown.get(ticker, 0.0)


# ----------------------------------------------------------------------------
# Market data
# ----------------------------------------------------------------------------
def scan():
    """One call returns every security with quotes, sizes and our position."""
    data = api('GET', '/securities')
    if not data:
        return {}
    return {row['ticker']: row for row in data}


def discover_pairs(secs):
    """
    Build arbitrage pairs generically from the ticker list, so the bot trades
    TAME as well as CRZY without hardcoding either.
    """
    roots = {}
    for ticker in secs:
        if '_' in ticker:
            root, venue = ticker.rsplit('_', 1)
            roots.setdefault(root, {})[venue] = ticker
    pairs = []
    for root, venues in roots.items():
        legs = sorted(venues.values())
        if len(legs) == 2:
            pairs.append(tuple(legs))
    return pairs


def field(sec, *names, default=0):
    """Field names vary a little between RIT builds — try several."""
    for n in names:
        if sec.get(n) is not None:
            return sec[n]
    return default


# ----------------------------------------------------------------------------
# Order handling
# ----------------------------------------------------------------------------
def send(ticker, action, quantity, price=None, decimals=2):
    """Submit one leg. Returns (filled_quantity, order_id_if_resting)."""
    if quantity < 1 or throttled(ticker):
        return 0, None

    params = {'ticker': ticker, 'quantity': int(quantity), 'action': action}
    if price is not None and USE_LIMIT_ORDERS:
        params['type'] = 'LIMIT'
        params['price'] = round(price, decimals)
    else:
        params['type'] = 'MARKET'

    order = api('POST', '/orders', params=params, ticker=ticker)
    if not order:
        return 0, None

    filled = int(order.get('quantity_filled') or 0)
    resting = order.get('order_id') if filled < int(quantity) else None
    return filled, resting


def cancel(order_id):
    if order_id:
        api('DELETE', '/orders/%s' % order_id)


def await_fill_then_cancel(order_id, ticker, quantity_filled):
    """
    Give a resting limit remainder up to CANCEL_GRACE seconds to fill before
    cancelling it. Polls rather than sleeping the full grace period so a fill
    that lands early doesn't cost the rest of the wait.
    """
    if not order_id:
        return quantity_filled

    deadline = time.monotonic() + CANCEL_GRACE
    filled = quantity_filled
    while True:
        order = api('GET', '/orders/%s' % order_id, ticker=ticker)
        if order:
            filled = int(order.get('quantity_filled') or filled)
            if order.get('status') != 'OPEN':
                return filled
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(CANCEL_POLL, remaining))

    cancel(order_id)
    return filled


def flatten(ticker, position, max_size):
    """Market out of a position, chunked to the venue's max trade size."""
    remaining = abs(int(position))
    action = 'SELL' if position > 0 else 'BUY'
    while remaining > 0 and not throttled(ticker):
        chunk = min(remaining, int(max_size) or remaining)
        filled, _ = send(ticker, action, chunk)
        if filled == 0:
            break
        remaining -= filled
    log('FLATTEN %s %s %d' % (action, ticker, abs(int(position)) - remaining))


# ----------------------------------------------------------------------------
# The arbitrage itself
# ----------------------------------------------------------------------------
def evaluate(buy_sec, sell_sec, headroom):
    """
    Net edge per share after both commissions:
        (best_bid on the sell venue) - (best_ask on the buy venue) - fees
    Returns (edge, quantity, buy_price, sell_price) or None if not worth doing.
    """
    ask = field(buy_sec, 'ask')
    bid = field(sell_sec, 'bid')
    if not ask or not bid:
        return None

    fees = field(buy_sec, 'trading_fee', 'commission') + \
           field(sell_sec, 'trading_fee', 'commission')
    edge = bid - ask - fees - EDGE_BUFFER
    if edge <= 0:
        return None

    # Size to whatever is actually resting at the top of each book. Sending
    # 1000 into a 300-share ask walks the book and gives the edge straight back.
    available = min(field(buy_sec, 'ask_size', 'ask_quantity'),
                    field(sell_sec, 'bid_size', 'bid_quantity'))
    cap = min(field(buy_sec, 'max_trade_size', default=MAX_ORDER_SIZE),
              field(sell_sec, 'max_trade_size', default=MAX_ORDER_SIZE),
              MAX_ORDER_SIZE, headroom)
    quantity = int(min(available, cap))

    if quantity < MIN_ORDER_SIZE:
        return None
    return edge, quantity, ask, bid


def execute(pool, buy_sec, sell_sec, quantity, buy_px, sell_px):
    """
    Fire both legs simultaneously. Sequential POSTs let the second leg move
    against you while the first is in flight — that leg risk is where most of
    the money leaks out of the starter algorithm.
    """
    bt, st = buy_sec['ticker'], sell_sec['ticker']
    bd = int(field(buy_sec, 'quoted_decimals', default=2))
    sd = int(field(sell_sec, 'quoted_decimals', default=2))

    fb = pool.submit(send, bt, 'BUY', quantity, buy_px, bd)
    fs = pool.submit(send, st, 'SELL', quantity, sell_px, sd)
    bought, buy_id = fb.result()
    sold, sell_id = fs.result()

    # Any unfilled remainder is now resting in the book. Give it CANCEL_GRACE
    # seconds to fill before killing it — both legs wait concurrently.
    fbw = pool.submit(await_fill_then_cancel, buy_id, bt, bought) if buy_id else None
    fsw = pool.submit(await_fill_then_cancel, sell_id, st, sold) if sell_id else None
    if fbw:
        bought = fbw.result()
    if fsw:
        sold = fsw.result()

    # Partial fills leave us naked. Close the gap now rather than hoping the
    # spread comes back.
    residual = bought - sold
    if residual > 0:
        send(bt, 'SELL', residual)
        log('RESIDUAL sold %d %s' % (residual, bt))
    elif residual < 0:
        send(st, 'BUY', -residual)
        log('RESIDUAL bought %d %s' % (-residual, st))

    matched = min(bought, sold)
    if matched:
        log('ARB %d  BUY %s @%.2f / SELL %s @%.2f' %
            (matched, bt, buy_px, st, sell_px))
    return matched


# ----------------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------------
def main():
    pool = ThreadPoolExecutor(max_workers=2)

    case = api('GET', '/case')
    if not case:
        print('Cannot reach the RIT client. Check that it is running, that the '
              'API is enabled, and that API_KEY matches.')
        return
    tick = case['tick']

    secs = scan()
    if not secs:
        print('No securities returned. Is the case running?')
        return

    pairs = discover_pairs(secs)
    print('Trading pairs:', pairs)
    print('Fees:', {t: field(s, 'trading_fee', 'commission') for t, s in secs.items()})

    gross_limit, gross = 0, 0
    scans = 0
    volume = 0

    while START_TICK < tick < STOP_TICK and not shutdown:
        scans += 1

        if scans % CASE_POLL_EVERY == 0:
            case = api('GET', '/case')
            if case:
                tick = case['tick']
                if case.get('status') == 'STOPPED':
                    break

        if scans % LIMITS_POLL_EVERY == 1:
            limits = api('GET', '/limits')
            if limits:
                row = limits[0] if isinstance(limits, list) else limits
                gross = abs(field(row, 'gross'))
                gross_limit = field(row, 'gross_limit')

        secs = scan()
        if not secs:
            continue

        # Each pair adds 2x quantity to gross exposure, so halve the headroom.
        if gross_limit:
            headroom = max(0, (gross_limit * GROSS_UTILISATION - gross) / 2)
        else:
            headroom = MAX_ORDER_SIZE

        if tick >= FLATTEN_TICK:
            break

        for a, b in pairs:
            sa, sb = secs.get(a), secs.get(b)
            if not sa or not sb:
                continue

            # Check both directions and take whichever pays more.
            best, legs = None, None
            for buy_sec, sell_sec in ((sa, sb), (sb, sa)):
                result = evaluate(buy_sec, sell_sec, headroom)
                if result and (best is None or result[0] > best[0]):
                    best, legs = result, (buy_sec, sell_sec)

            if best:
                edge, quantity, buy_px, sell_px = best
                filled = execute(pool, legs[0], legs[1], quantity, buy_px, sell_px)
                volume += filled
                gross += 2 * filled

    # ---- wind down -----------------------------------------------------
    api('POST', '/commands/cancel', params={'all': 1})
    secs = scan()
    for ticker, sec in secs.items():
        position = field(sec, 'position')
        if position:
            flatten(ticker, position, field(sec, 'max_trade_size',
                                            default=MAX_ORDER_SIZE))

    pool.shutdown(wait=True)

    print('\n'.join(_log[-60:]))
    print('\nScans: %d   Arb shares traded: %d' % (scans, volume))


if __name__ == '__main__':
    _T0 = time.monotonic()
    signal.signal(signal.SIGINT, signal_handler)
    main()
