"""
RIT ALGO2 — algorithmic market making on a single stock (ALGO).

The case brief's baseline algorithm is:
    1. If you have no orders resting, submit a bid at (LAST - spread) and an
       ask at (LAST + spread).
    2. If only one side is resting, the other side filled — cancel the leftover.
    3. Adjust prices/sizes to keep inventory close to flat.

That baseline works in a range and gets run over in a trend. This version keeps
the same skeleton and adds the pieces that actually decide whether a market
maker makes money:

  1. Fair value from the book midpoint, not LAST. LAST is the price of a trade
     that already happened; the mid is where the market is now.
  2. Dynamic half-spread — a floor, scaled up with the prevailing market spread
     and widened further as inventory grows (risk-adjusted compensation).
  3. Inventory price skew: quotes are shifted against the position, so the side
     that flattens us sits closer to the touch and fills first.
  4. Inventory size skew: the side that would add to the position quotes smaller
     than the side that reduces it.
  5. Passive-only clamping. Our bid never touches the best ask (and vice versa),
     so every fill earns the 0.5c rebate instead of paying the 1c commission.
     Bids floor to the tick, asks ceil — rounding never crosses us.
  6. Requote hysteresis. Cancel/replace only when the target moves more than a
     tolerance, since every replace throws away queue priority.
  7. Hard position guards: past a soft limit we quote one side only; past a
     panic limit we market out back under the soft limit. The 25,000-share cap
     carries a 10c/share fine — it is a constraint, not a target.
  8. Wind-down: stop quoting near the end, work the reducing side at the touch,
     then market-flatten so nothing is marked into the final snapshot.
  9. Nothing raises out of the main loop; 429s set a cooldown rather than
     blind-retrying.

Usage:  python rit_algo2_spiro.py
Stop:   CTRL+C (graceful — cancels resting orders and flattens)
"""

import math
import signal
import threading
import time

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

TICKER = 'ALGO'
TICK_SIZE = 0.01
DECIMALS = 2

START_TICK = 2          # let the opening auction settle before quoting
STOP_TICK = 265         # stop quoting for profit, start reducing inventory.
                        # Early enough that the exit can be worked passively —
                        # crossing to get flat costs 1.5c/share against us.
HARD_FLATTEN_TICK = 293 # market out of whatever is left, fine or no fine

# --- economics (from the case's Security Info, NOT the brief's prose) -------
# The fee is PER_UNIT on EVERY share, active or passive. The rebate is paid
# only on passive fills. So the per-share arithmetic is:
#       passive fill : -FEE + REBATE = +0.005   (we get paid to provide)
#       active  fill : -FEE          = -0.010   (we pay to consume)
# The gap is 0.015/share — one and a half ticks. That number dominates every
# other consideration in this case: a round trip that captures zero spread but
# is passive on both legs still nets +0.01/share, while a passive buy unwound
# by a market sell nets -0.005/share before any adverse price move. Crossing
# the spread to manage a position we have plenty of room to hold is the single
# most expensive mistake available to us.
FEE = 0.010
REBATE = 0.015
PASSIVE_EDGE = REBATE - FEE     # +0.005 per passive share
ACTIVE_COST = FEE               # -0.010 per active share

# --- spread ---------------------------------------------------------------
MIN_HALF_SPREAD = 0.02  # floor, in $/share, either side of fair value. At 0.01
                        # we sat at the top of book and were adversely selected
                        # — bought at 20.0632, sold at 20.0618.
MAX_HALF_SPREAD = 0.10  # cap — past this we're not competitive, just decoration
SPREAD_FRACTION = 0.60  # half-spread also scales with the market's own spread
WIDEN_PER_INVENTORY = 1.0   # half-spread multiplier at full soft-limit inventory

# --- inventory control ----------------------------------------------------
QUOTE_SIZE = 2500       # base size per side. Smaller clips = smaller inventory
                        # jumps per fill = far less position volatility. We make
                        # it back on round-trip count, not on size.
MAX_ORDER_SIZE = 5000   # case rule: 5,000 shares per order
MIN_ORDER_SIZE = 100    # below this the API call isn't worth the latency
BOOK_SIZE_FRACTION = 0.50   # never quote more than this fraction of the size
                        # resting at the opposite touch — sizing past what the
                        # market can absorb is how a clip becomes an position
POSITION_LIMIT = 25000  # case rule; overridden by /limits when available
SOFT_LIMIT_FRAC = 0.50  # beyond this fraction of the limit we quote one side only
PANIC_LIMIT_FRAC = 0.85 # beyond this we market out back to the soft limit. Last
                        # run peaked at 4,750 against a 25,000 limit and still
                        # market-flattened repeatedly — the risk was imaginary
                        # and the fees were real.
SKEW_STRENGTH = 1.5     # price shift, in half-spreads, at full soft-limit
                        # inventory. >1.0 means we'll quote the reducing side
                        # through fair value — deliberately paying to get flat.
SIZE_SKEW_STRENGTH = 1.0    # at 1.0 the adding side hits zero exactly at the
                        # soft limit, rather than still adding a token clip

# --- inventory chasing (PASSIVE ONLY) --------------------------------------
# Urgency escalates where we *quote*, never whether we cross. Every rung on
# this ladder still earns the rebate; none of them pays the 1.5c/share penalty
# of taking liquidity. Market orders are reserved for the panic limit and the
# end-of-case wind-down, and nothing else.
FLAT_BAND = 2500        # |position| below this counts as flat; don't chase.
                        # 2,500 is 10% of the limit — genuinely small.
JOIN_TOUCH_URGENCY = 0.30   # urgency at which the reducing side joins the touch
INSIDE_SPREAD_URGENCY = 0.65    # ...and at which it steps inside the spread
STALE_INVENTORY_SECS = 90.0 # time alone saturates urgency this slowly. At 20s
                        # a single fill triggered a market flatten every time.

# --- order churn ----------------------------------------------------------
# Last run: 1,268 orders submitted, 44 trades — a 3.5% fill rate. Quotes were
# cancelled and replaced on every 1c wiggle in the mid, so they never survived
# in the queue long enough to be filled. Passive fills require patience.
REQUOTE_TOLERANCE = 0.03    # replace a resting quote only if it's this far off
REQUOTE_SIZE_TOLERANCE = 0.60   # ...or if its size is this fraction wrong
MIN_REST_SECS = 3.0     # a quote gets at least this long in the queue before
                        # we're allowed to replace it on price/size drift
LOOP_SLEEP = 0.10       # pacing between scans
CASE_POLL_EVERY = 10    # scans between /v1/case refreshes
LIMITS_POLL_EVERY = 50  # scans between /v1/limits refreshes
FLATTEN_MAX_WAIT = 5.0  # seconds to keep retrying an intra-run flatten through cooldowns
WINDDOWN_MAX_WAIT = 15.0    # seconds to keep retrying the final flatten

VERBOSE = True          # True prints every quote/fill live


# ----------------------------------------------------------------------------
# Plumbing
# ----------------------------------------------------------------------------
shutdown = False
_T0 = time.monotonic()
_local = threading.local()
_log = []
_cooldown = {}          # ticker -> monotonic timestamp until which it's throttled
_global_cooldown = 0.0
_last_heartbeat = 0.0
HEARTBEAT_EVERY = 3.0   # seconds between position prints, regardless of scan rate
_nonflat_since = None   # monotonic time we last left the flat band
_placed_at = {}         # order_id -> monotonic time we submitted it
orders_placed = 0       # how many limit orders we submitted (churn measure)
active_shares = 0       # shares filled via MARKET orders (the expensive kind)


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
    """Like log(), but always prints — for failures worth seeing live."""
    entry = '[%7.2fs] %s' % (time.monotonic() - _T0, msg)
    _log.append(entry)
    print(entry)


def quiet_log(msg):
    """Record without ever printing live — for noise that's expected in normal
    operation (e.g. cancelling an order that already filled) and only useful
    in the post-run dump, regardless of VERBOSE."""
    _log.append('[%7.2fs] %s' % (time.monotonic() - _T0, msg))


def session():
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
    Single entry point for every API call. Returns the decoded body, or None on
    any failure. Never raises — a crash at tick 40 means no quotes, and no
    quotes means no P&L, for the remaining 260 ticks.
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
            _cooldown[ticker] = until
        else:
            _global_cooldown = until
        return None

    if resp.status_code == 401:
        warn('401 — API key mismatch with the RIT client. Fix API_KEY.')
        return None

    if resp.status_code == 404 and method == 'DELETE':
        # Expected and constant: we tried to cancel an order that already
        # filled or was already cancelled by an earlier pass. Not a failure.
        quiet_log('HTTP 404 %s %s (order already gone)' % (method, path))
        return None

    # Routine — a rejected order near a limit, etc. Kept in the buffered log
    # for the post-run dump, just not spammed to console.
    quiet_log('HTTP %d %s %s' % (resp.status_code, method, path))
    return None


def heartbeat(position, tick=None, note=''):
    """Print position on a wall-clock cadence, independent of scan/loop speed —
    this is the number that actually determines whether the P&L figure at the
    end means anything, so it needs to be visible while the run is happening,
    not just discovered after the fact."""
    global _last_heartbeat
    now = time.monotonic()
    if now - _last_heartbeat < HEARTBEAT_EVERY:
        return
    _last_heartbeat = now
    tick_part = ' tick %3d' % tick if tick is not None else ''
    print('[%7.2fs]%s position %+d%s' % (now - _T0, tick_part, position,
                                          ('  ' + note) if note else ''))


def throttled(ticker):
    now = time.monotonic()
    return now < _global_cooldown or now < _cooldown.get(ticker, 0.0)


def field(row, *names, default=0):
    """Field names vary a little between RIT builds — try several."""
    if not row:
        return default
    for n in names:
        if row.get(n) is not None:
            return row[n]
    return default


def floor_tick(price):
    return math.floor(price / TICK_SIZE + 1e-9) * TICK_SIZE


def ceil_tick(price):
    return math.ceil(price / TICK_SIZE - 1e-9) * TICK_SIZE


# ----------------------------------------------------------------------------
# Market data
# ----------------------------------------------------------------------------
def snapshot():
    """One call gives quotes, sizes, last, and our position for the ticker."""
    data = api('GET', '/securities', params={'ticker': TICKER}, ticker=TICKER)
    if not data:
        return None
    row = data[0] if isinstance(data, list) else data
    return row if row and row.get('ticker') == TICKER else None


def open_orders():
    """
    The book is the source of truth for what we have resting — not a local
    cache, which drifts the moment a fill or a rejection happens unseen.
    Returns (best_bid_order, best_ask_order) among our own orders, plus any
    extras we should clean up.
    """
    data = api('GET', '/orders', params={'status': 'OPEN'}, ticker=TICKER)
    if data is None:
        return None, None, []

    bids = [o for o in data if o.get('ticker') == TICKER and o.get('action') == 'BUY']
    asks = [o for o in data if o.get('ticker') == TICKER and o.get('action') == 'SELL']

    # Keep the most aggressive on each side; anything else is a leftover from a
    # replace that raced a fill, and just eats position headroom.
    bids.sort(key=lambda o: field(o, 'price'), reverse=True)
    asks.sort(key=lambda o: field(o, 'price'))
    extras = bids[1:] + asks[1:]
    return (bids[0] if bids else None), (asks[0] if asks else None), extras


def remaining(order):
    return int(field(order, 'quantity')) - int(field(order, 'quantity_filled'))


# ----------------------------------------------------------------------------
# Quote construction — rules 1 and 2 of the brief, made inventory-aware
# ----------------------------------------------------------------------------
def fair_value(sec):
    """
    Midpoint when both sides of the book are populated, else fall back to LAST.
    The brief anchors on LAST; the mid is strictly better information because
    it reflects where the market is willing to trade right now rather than
    where it last traded.
    """
    bid, ask = field(sec, 'bid'), field(sec, 'ask')
    if bid and ask and ask > bid:
        return (bid + ask) / 2.0
    last = field(sec, 'last')
    return last or (bid or ask or 0.0)


def inventory_urgency(position, soft_limit):
    """
    How badly we want to be flat, in [0, 1].

    Two independent drivers, whichever is worse:
      - size : how much of the risk budget the position uses
      - time : how long we've been carrying it at all

    The time term matters because a market maker's edge is the spread, earned
    per round trip. A position held for 60 seconds isn't earning spread — it's
    just a directional bet we never intended to make, and its variance swamps
    the pennies we're collecting. Inside FLAT_BAND we treat ourselves as flat
    and stop the clock, so we don't churn over a 200-share residue.
    """
    global _nonflat_since
    now = time.monotonic()

    if abs(position) <= FLAT_BAND:
        _nonflat_since = None
        return 0.0

    if _nonflat_since is None:
        _nonflat_since = now

    size_urgency = min(1.0, abs(position) / float(soft_limit)) if soft_limit else 0.0
    time_urgency = min(1.0, (now - _nonflat_since) / STALE_INVENTORY_SECS)
    return max(size_urgency, time_urgency)


def compute_quotes(sec, position, soft_limit, hard_limit):
    """
    Returns (bid_price, bid_size, ask_price, ask_size). A size of 0 means
    "don't quote that side".

    Inventory enters three ways:
      - price skew   : shift both quotes against the position, so the flattening
                       side is nearer the touch and fills sooner
      - size skew    : quote less on the side that would grow the position
      - spread widen : demand more edge the more risk we're already carrying
    """
    fair = fair_value(sec)
    if not fair:
        return None

    best_bid = field(sec, 'bid') or fair - TICK_SIZE
    best_ask = field(sec, 'ask') or fair + TICK_SIZE

    # inv in [-1, 1]: how much of our risk budget the position is using
    inv = max(-1.0, min(1.0, position / float(soft_limit))) if soft_limit else 0.0

    market_spread = max(0.0, best_ask - best_bid)
    half = max(MIN_HALF_SPREAD, SPREAD_FRACTION * market_spread)
    half *= (1.0 + WIDEN_PER_INVENTORY * abs(inv))
    half = min(half, MAX_HALF_SPREAD)

    centre = fair - SKEW_STRENGTH * inv * half

    bid_px = floor_tick(centre - half)
    ask_px = ceil_tick(centre + half)

    # --- passive inventory chasing ----------------------------------------
    # Escalate where the *reducing* side is quoted as urgency rises. Both rungs
    # stay strictly passive, so a fill here still earns +0.005/share instead of
    # costing 0.010 — we give up spread, never the rebate.
    urgency = inventory_urgency(position, soft_limit)
    if urgency >= JOIN_TOUCH_URGENCY:
        inside = urgency >= INSIDE_SPREAD_URGENCY
        if position > 0:                    # long: work the ask down
            target = (best_bid + TICK_SIZE) if inside else best_ask
            ask_px = min(ask_px, ceil_tick(target))
        elif position < 0:                  # short: work the bid up
            target = (best_ask - TICK_SIZE) if inside else best_bid
            bid_px = max(bid_px, floor_tick(target))

    # Stay passive. Crossing the touch swaps a +0.005 rebate for a -0.010 fee,
    # a 1.5c/share swing that no amount of spread capture pays back. This clamp
    # runs after the escalation above, so "inside the spread" stays strictly
    # inside — aggressive on queue position, never on fees.
    bid_px = min(bid_px, floor_tick(best_ask - TICK_SIZE))
    ask_px = max(ask_px, ceil_tick(best_bid + TICK_SIZE))
    if ask_px - bid_px < TICK_SIZE:         # never quote through ourselves
        ask_px = bid_px + TICK_SIZE

    bid_sz = QUOTE_SIZE * (1.0 - SIZE_SKEW_STRENGTH * inv)
    ask_sz = QUOTE_SIZE * (1.0 + SIZE_SKEW_STRENGTH * inv)

    # Don't quote more than the market can actually absorb at the touch. A clip
    # larger than the resting size can only fill by the book trading through
    # us, which is precisely when we least want the fill.
    bid_book = field(sec, 'ask_size', 'ask_quantity')
    ask_book = field(sec, 'bid_size', 'bid_quantity')
    if bid_book:
        bid_sz = min(bid_sz, max(MIN_ORDER_SIZE, BOOK_SIZE_FRACTION * bid_book))
    if ask_book:
        ask_sz = min(ask_sz, max(MIN_ORDER_SIZE, BOOK_SIZE_FRACTION * ask_book))

    # Never quote more than the room left before the position limit — a fill we
    # can't legally hold costs 10c/share.
    bid_sz = min(bid_sz, hard_limit - position, MAX_ORDER_SIZE)
    ask_sz = min(ask_sz, hard_limit + position, MAX_ORDER_SIZE)

    # When we're chasing flat, let the reducing side carry the whole position
    # rather than dribbling it out one base clip at a time.
    if urgency >= JOIN_TOUCH_URGENCY:
        if position > 0:
            ask_sz = max(ask_sz, min(abs(position), MAX_ORDER_SIZE))
        elif position < 0:
            bid_sz = max(bid_sz, min(abs(position), MAX_ORDER_SIZE))

    # Past the soft limit, one-sided quoting: only the side that flattens us.
    if position >= soft_limit:
        bid_sz = 0
    elif position <= -soft_limit:
        ask_sz = 0

    bid_sz = int(bid_sz) if bid_sz >= MIN_ORDER_SIZE else 0
    ask_sz = int(ask_sz) if ask_sz >= MIN_ORDER_SIZE else 0
    return round(bid_px, DECIMALS), bid_sz, round(ask_px, DECIMALS), ask_sz


def needs_replace(order, target_px, target_sz):
    """
    Hysteresis. Replacing a resting order sends it to the back of the queue at
    its new price, so we only pay that cost when the quote is meaningfully
    stale — either mispriced or badly sized.

    The MIN_REST_SECS floor is the important part: a passive fill only happens
    if the order is still sitting there when the market comes to it. Replacing
    on every 1c wiggle produced 1,268 orders and 44 trades last run, and zero
    passive sells. An order that hasn't had its time in the queue is left alone
    even if the target has drifted — unless the drift is so large that leaving
    it would be dangerous (handled by the wide-miss check below).
    """
    if order is None:
        return target_sz > 0
    if target_sz <= 0:
        return True

    drift = abs(field(order, 'price') - target_px)

    # A quote that has drifted more than a full half-spread is not just stale,
    # it's exposed — replace it regardless of how long it has rested.
    if drift >= max(REQUOTE_TOLERANCE * 2, MAX_HALF_SPREAD):
        return True

    resting_for = time.monotonic() - _placed_at.get(order.get('order_id'), 0.0)
    if resting_for < MIN_REST_SECS:
        return False

    if drift >= REQUOTE_TOLERANCE:
        return True
    rest = remaining(order)
    return abs(rest - target_sz) > REQUOTE_SIZE_TOLERANCE * target_sz


# ----------------------------------------------------------------------------
# Order handling
# ----------------------------------------------------------------------------
def place(action, quantity, price):
    global orders_placed
    if quantity < MIN_ORDER_SIZE or throttled(TICKER):
        return None
    orders_placed += 1
    order = api('POST', '/orders', params={
        'ticker': TICKER, 'type': 'LIMIT', 'action': action,
        'quantity': int(quantity), 'price': round(price, DECIMALS),
    }, ticker=TICKER)
    if order:
        if order.get('order_id') is not None:
            _placed_at[order['order_id']] = time.monotonic()
        log('QUOTE %s %d @ %.2f' % (action, quantity, price))
    return order


def cancel(order):
    if order:
        _placed_at.pop(order.get('order_id'), None)
        api('DELETE', '/orders/%s' % order['order_id'], ticker=TICKER)


def cancel_all():
    api('POST', '/commands/cancel', params={'ticker': TICKER}, ticker=TICKER)


def flatten_to(target=0, deadline_s=10.0, respect_shutdown=True):
    """
    Reduce the position toward `target`, judging progress by what /securities
    actually reports rather than by what an order response claims. A rejected
    or unacknowledged market order is retried until the deadline instead of
    silently ending the flatten. Returns True if we reached the target.

    `respect_shutdown=False` for the final wind-down: CTRL+C is supposed to get
    us flat, so the shutdown flag must not abort the very flatten it requested.
    A second CTRL+C still kills the process outright.

    Every share this function trades costs 0.010 instead of earning 0.005, so
    it is a last resort — the panic limit and the final wind-down, nothing else.
    """
    global active_shares
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        if respect_shutdown and shutdown:
            return False
        sec = snapshot()
        if not sec:
            time.sleep(0.05)
            continue
        position = field(sec, 'position')
        heartbeat(position, note='flattening to %+d' % target)
        excess = position - target
        if abs(excess) < 1:
            return True
        if throttled(TICKER):
            time.sleep(0.05)
            continue
        action = 'SELL' if excess > 0 else 'BUY'
        chunk = min(abs(int(excess)), MAX_ORDER_SIZE)
        resp = api('POST', '/orders', params={
            'ticker': TICKER, 'type': 'MARKET',
            'action': action, 'quantity': chunk,
        }, ticker=TICKER)
        if resp is None:
            warn('FLATTEN rejected: %s %d at position %d' % (action, chunk, position))
        else:
            active_shares += int(field(resp, 'quantity_filled', default=chunk))
            log('MARKET %s %d  (position was %d)' % (action, chunk, position))
        time.sleep(0.05)
    return False


# ----------------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------------
def main():
    case = api('GET', '/case')
    if not case:
        print('Cannot reach the RIT client. Check that it is running, that the '
              'API is enabled, and that API_KEY matches.')
        return
    tick = case['tick']

    sec = snapshot()
    if not sec:
        print('Security %s not found. Is the ALGO2 case running?' % TICKER)
        return

    trader = api('GET', '/trader')
    start_nlv = field(trader, 'nlv') if trader else None

    hard_limit = POSITION_LIMIT
    limits = api('GET', '/limits')
    if limits:
        row = limits[0] if isinstance(limits, list) else limits
        hard_limit = field(row, 'net_limit', 'gross_limit', default=POSITION_LIMIT)
    soft_limit = max(MIN_ORDER_SIZE, hard_limit * SOFT_LIMIT_FRAC)
    panic_limit = hard_limit * PANIC_LIMIT_FRAC
    print('%s  position limit %d  soft %d  panic %d'
          % (TICKER, hard_limit, soft_limit, panic_limit))

    scans = 0
    last_position = field(sec, 'position')
    bought = sold = 0

    while START_TICK < tick < HARD_FLATTEN_TICK and not shutdown:
        scans += 1

        if scans % CASE_POLL_EVERY == 0:
            case = api('GET', '/case')
            if case:
                tick = case['tick']
                if case.get('status') == 'STOPPED':
                    break

        if scans % LIMITS_POLL_EVERY == 0:
            limits = api('GET', '/limits')
            if limits:
                row = limits[0] if isinstance(limits, list) else limits
                hard_limit = field(row, 'net_limit', 'gross_limit', default=hard_limit)
                soft_limit = max(MIN_ORDER_SIZE, hard_limit * SOFT_LIMIT_FRAC)
                panic_limit = hard_limit * PANIC_LIMIT_FRAC

        sec = snapshot()
        if not sec:
            time.sleep(LOOP_SLEEP)
            continue
        position = field(sec, 'position')
        urgency = inventory_urgency(position, soft_limit)
        heartbeat(position, tick=tick, note='urgency %.2f' % urgency)

        # Track fills for the end-of-run report.
        delta = position - last_position
        if delta > 0:
            bought += delta
        elif delta < 0:
            sold += -delta
        if delta:
            log('FILL %+d  position %d' % (delta, position))
        last_position = position

        # --- risk guard: too big to keep quoting through ---------------------
        if abs(position) > panic_limit:
            warn('PANIC position %d > %d — reducing at market' % (position, panic_limit))
            cancel_all()
            flatten_to(soft_limit if position > 0 else -soft_limit,
                       deadline_s=FLATTEN_MAX_WAIT)
            time.sleep(LOOP_SLEEP)
            continue

        # NOTE: there is deliberately no time-based market flatten here. The
        # previous version market-sold whenever urgency saturated, which made
        # every sell an active fill: 29,852 active sells, 0 passive sells,
        # -$298 of commission where +$448 of rebate was available. Below the
        # panic limit we have room to be patient, so we stay passive and let
        # the quote escalation in compute_quotes do the work.

        bid_order, ask_order, extras = open_orders()
        for extra in extras:                # duplicate quotes from a raced replace
            cancel(extra)

        # --- wind-down: stop making a market, start getting flat -------------
        if tick >= STOP_TICK:
            # Cancel only the side that would *grow* the position. The previous
            # version called cancel_all() every pass, which killed the very
            # flattening order it had placed 0.1s earlier — so it churned for
            # the whole wind-down and never filled passively even once.
            reducing = ask_order if position > 0 else bid_order
            adding = bid_order if position > 0 else ask_order
            if adding:
                cancel(adding)

            if position:
                # Work the touch passively — at +0.005 vs -0.010 a share, a
                # passive exit is worth 1.5c/share more than crossing, and we
                # still have ticks left for it to fill.
                side = 'SELL' if position > 0 else 'BUY'
                px = field(sec, 'bid') if position > 0 else field(sec, 'ask')
                size = min(abs(position), MAX_ORDER_SIZE)
                if px and needs_replace(reducing, px, size):
                    cancel(reducing)
                    place(side, size, px)
            elif reducing:
                cancel(reducing)
            time.sleep(LOOP_SLEEP)
            continue

        quotes = compute_quotes(sec, position, soft_limit, hard_limit)
        if not quotes:
            time.sleep(LOOP_SLEEP)
            continue
        bid_px, bid_sz, ask_px, ask_sz = quotes

        # The brief says to cancel the survivor when only one side is resting.
        # We deliberately don't do that blindly. When our bid fills we are long,
        # and the surviving *ask* is precisely the order that will get us flat
        # again — cancelling it throws away the queue priority of the one fill
        # we most want. That is a large part of why last run had zero passive
        # sells. The survivor is only replaced if it's genuinely stale, which
        # needs_replace() already judges on price drift and resting time; the
        # missing side is refilled below regardless.
        replace_bid = needs_replace(bid_order, bid_px, bid_sz)
        replace_ask = needs_replace(ask_order, ask_px, ask_sz)

        if replace_bid:
            cancel(bid_order)
            place('BUY', bid_sz, bid_px)
        if replace_ask:
            cancel(ask_order)
            place('SELL', ask_sz, ask_px)

        time.sleep(LOOP_SLEEP)

    # ---- wind down ---------------------------------------------------------
    cancel_all()
    flat = flatten_to(0, deadline_s=WINDDOWN_MAX_WAIT, respect_shutdown=False)

    sec = snapshot()
    leftover = field(sec, 'position') if sec else 0

    pnl = None
    if start_nlv is not None:
        trader = api('GET', '/trader')
        if trader:
            pnl = field(trader, 'nlv') - start_nlv

    print('\n'.join(_log[-60:]))
    print('\nScans: %d   Bought: %d   Sold: %d' % (scans, bought, sold))
    # The ratio that decides this case. Every active share costs 0.010; every
    # passive share earns 0.005. Last run this was 0 passive sells out of
    # 29,852 — a $448 rebate left on the table plus $298 of commission paid.
    print('Orders submitted: %d   Active (market) shares: %d'
          % (orders_placed, active_shares))
    if bought + sold:
        print('Passive share of volume: %.1f%%'
              % (100.0 * (1.0 - active_shares / float(bought + sold))))
    print('Final position: %+d' % leftover)
    if pnl is not None:
        if leftover:
            print('P&L (INCLUDES an unrealized mark on the %+d leftover — NOT purely '
                  'realized): %.2f' % (leftover, pnl))
        else:
            print('Realized P&L: %.2f' % pnl)
    if leftover or not flat:
        print('*** WARNING: wind-down did not reach flat (position %+d). The P&L '
              'above is not trustworthy as a realized number — the case likely '
              'stopped accepting orders before the flatten finished. Consider '
              'raising STOP_TICK\'s margin or shortening WINDDOWN_MAX_WAIT '
              'expectations to fit inside the remaining ticks. ***' % leftover)


if __name__ == '__main__':
    _T0 = time.monotonic()
    signal.signal(signal.SIGINT, signal_handler)
    main()
