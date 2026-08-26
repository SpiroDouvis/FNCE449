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
 12. Final-ticks lockout: trading stops a few ticks before the close, then one
     last pair-balance check fires a single corrective order on any pair that
     isn't delta-neutral.

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
FINAL_LOCKOUT_TICKS = 3 # no new arb inside this many ticks of STOP_TICK — final
                        # window is for the balance check/correction only

EDGE_BUFFER = 0.00      # extra $/share required beyond commissions. Raise to
                        # 0.01-0.02 if you're getting picked off on the 2nd leg.
MAX_ORDER_SIZE = 5000   # hard cap per leg regardless of what the book shows
MIN_ORDER_SIZE = 50     # below this the fixed costs aren't worth the API call

USE_LIMIT_ORDERS = True # marketable limits (safe) vs MARKET (always fills)
CANCEL_GRACE = 1.0      # seconds a resting limit remainder gets before we cancel it
CANCEL_POLL = 0.2       # how often we check the order while waiting out the grace period
RESIDUAL_MAX_WAIT = 2.0 # seconds to keep retrying a naked-residual close through cooldowns
FLATTEN_MAX_WAIT = 5.0  # seconds to keep retrying end-of-case flatten through cooldowns
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


def warn(msg):
    """Like log(), but always prints immediately — for failures that matter
    to see live (API errors, unclosed positions), not just in the final dump."""
    entry = '[%7.2fs] %s' % (time.monotonic() - _T0, msg)
    _log.append(entry)
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
        warn('NETWORK FAIL %s %s: %s' % (method, path, exc))
        return None

    if resp.status_code == 200:
        try:
            return _loads(resp.content)
        except ValueError:
            warn('BAD JSON %s %s' % (method, path))
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
        warn('429 %s %s%s — cooling down %.2fs' %
             (method, path, (' [%s]' % ticker) if ticker else '', wait))
        return None

    if resp.status_code == 401:
        warn('401 — API key mismatch with the RIT client. Fix API_KEY.')
        return None

    warn('HTTP %d %s %s' % (resp.status_code, method, path))
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


def send_through_cooldown(ticker, action, quantity, max_wait, chunk_size=None):
    """
    Market order that rides out a 429 cooldown instead of giving up on the
    first throttled attempt — a single-shot send silently leaves a residual
    or an end-of-case position unclosed if the ticker happens to be cooling
    down at that exact instant.
    """
    remaining = int(quantity)
    deadline = time.monotonic() + max_wait
    while remaining > 0 and time.monotonic() < deadline:
        if throttled(ticker):
            time.sleep(0.05)
            continue
        chunk = min(remaining, int(chunk_size) or remaining) if chunk_size else remaining
        filled, _ = send(ticker, action, chunk)
        if filled == 0:
            break
        remaining -= filled
    return int(quantity) - remaining


def flatten(ticker, position, max_size):
    """Market out of a position, chunked to the venue's max trade size."""
    action = 'SELL' if position > 0 else 'BUY'
    closed = send_through_cooldown(ticker, action, abs(int(position)),
                                    FLATTEN_MAX_WAIT, max_size)
    log('FLATTEN %s %s %d' % (action, ticker, closed))
    if closed < abs(int(position)):
        warn('WARNING %s still has %d unflattened after wind-down' %
            (ticker, abs(int(position)) - closed))


def final_balance_check(pairs, secs):
    """
    Last-resort safety net for the closing ticks. The two legs of an arb pair
    settle to the same terminal value, so what matters isn't each leg being
    individually flat (flatten() already tried that) — it's the pair being
    delta-neutral. Any pair left with position_a + position_b != 0 gets a
    single corrective market order on leg a to zero out the imbalance.
    """
    for a, b in pairs:
        pos_a = field(secs.get(a, {}), 'position')
        pos_b = field(secs.get(b, {}), 'position')
        imbalance = int(pos_a + pos_b)
        if imbalance == 0:
            continue
        action = 'SELL' if imbalance > 0 else 'BUY'
        closed = send_through_cooldown(a, action, abs(imbalance), RESIDUAL_MAX_WAIT)
        warn('FINAL CORRECTION %s %s %d  (pair %s/%s was off by %d)' %
             (action, a, closed, a, b, imbalance))
        if closed < abs(imbalance):
            warn('WARNING pair %s/%s still off by %d after final correction' %
                 (a, b, abs(imbalance) - closed))


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
        closed = send_through_cooldown(bt, 'SELL', residual, RESIDUAL_MAX_WAIT)
        log('RESIDUAL sold %d %s' % (closed, bt))
        if closed < residual:
            warn('WARNING %s residual %d unclosed (cooldown)' % (bt, residual - closed))
    elif residual < 0:
        closed = send_through_cooldown(st, 'BUY', -residual, RESIDUAL_MAX_WAIT)
        log('RESIDUAL bought %d %s' % (closed, st))
        if closed < -residual:
            warn('WARNING %s residual %d unclosed (cooldown)' % (st, -residual - closed))

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

    # NLV is the exchange's own authoritative mark, so diffing it against the
    # starting value gives realized P&L without us having to reconstruct fill
    # prices for market-order residuals/flattens.
    trader = api('GET', '/trader')
    start_nlv = field(trader, 'nlv') if trader else None

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

        if start_nlv is not None and scans % CASE_POLL_EVERY == 0:
            trader = api('GET', '/trader')
            if trader:
                log('PNL %.2f' % (field(trader, 'nlv') - start_nlv))

        secs = scan()
        if not secs:
            continue

        # Each pair adds 2x quantity to gross exposure, so halve the headroom.
        if gross_limit:
            headroom = max(0, (gross_limit * GROSS_UTILISATION - gross) / 2)
        else:
            headroom = MAX_ORDER_SIZE

        if tick >= FLATTEN_TICK or tick >= STOP_TICK - FINAL_LOCKOUT_TICKS:
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

    # Belt-and-suspenders: confirm flatten() actually worked rather than
    # trusting it silently, since a cooldown could still eat into its budget.
    secs = scan()
    leftover = {t: field(s, 'position') for t, s in secs.items() if field(s, 'position')}
    if leftover:
        warn('WARNING nonzero positions after wind-down: %s' % leftover)

    # ---- final ticks: no arbitrage runs here — just wait out the close and
    # do one last pair-balance correction as a safety net on top of flatten()
    while not shutdown:
        case = api('GET', '/case')
        if not case or case.get('status') == 'STOPPED' or case['tick'] >= STOP_TICK - 1:
            break
        time.sleep(0.2)

    secs = scan()
    if secs:
        final_balance_check(pairs, secs)

    pnl = None
    if start_nlv is not None:
        trader = api('GET', '/trader')
        if trader:
            pnl = field(trader, 'nlv') - start_nlv

    print('\n'.join(_log[-60:]))
    print('\nScans: %d   Arb shares traded: %d' % (scans, volume))
    if pnl is not None:
        print('Realized P&L: %.2f' % pnl)
    if leftover:
        print('WARNING: nonzero positions remain:', leftover)


if __name__ == '__main__':
    _T0 = time.monotonic()
    signal.signal(signal.SIGINT, signal_handler)
    main()
