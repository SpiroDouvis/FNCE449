"""
RIT ALGO2 — minimal market maker.

The case brief's algorithm, and nothing else:
    1. Always have a bid and an ask resting in the market.
    2. If only one side is resting, the other filled — cancel the leftover and
       requote both.
    3. Skew the quotes against the position to keep inventory near flat.

Fixed 2,000-share clips on both sides. Two API reads per loop, one arithmetic
pass, no threading, no order bookkeeping. Everything else was cut.

Usage:  python rit_algo2_simple.py
Stop:   CTRL+C (cancels resting orders and flattens)
"""

import signal
import time

import requests

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
API_KEY = {'X-API-Key': 'FLI7E73K'}
BASE = 'http://localhost:9999/v1'

TICKER = 'ALGO'
SIZE = 2000             # fixed clip per side, regardless of the market
HALF_SPREAD = 0.02      # quote this far either side of the midpoint
SKEW_AT_LIMIT = 0.03    # price shift at a full 25,000 position. Deliberately
                        # small: at a typical 5,000 position it moves the quote
                        # less than a cent.
POSITION_LIMIT = 25000  # case rule — stop adding to a side that would breach it

START_TICK = 2          # let the open settle
STOP_TICK = 280         # stop quoting, flatten out
LOOP_SLEEP = 0.05       # 20 passes/sec; the case allows 5 orders/sec
CASE_POLL_EVERY = 10    # loops between tick refreshes
HEARTBEAT = 3.0         # seconds between position prints

shutdown = False
_t0 = time.monotonic()
_last_beat = 0.0

session = requests.Session()
session.headers.update(API_KEY)


def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True


# ----------------------------------------------------------------------------
# API — thin wrappers that swallow failures so the loop never dies
# ----------------------------------------------------------------------------
def get(path, params=None):
    try:
        r = session.get(BASE + path, params=params, timeout=1.0)
        return r.json() if r.ok else None
    except (requests.RequestException, ValueError):
        return None


def post(path, params):
    try:
        r = session.post(BASE + path, params=params, timeout=1.0)
        return r.json() if r.ok else None
    except (requests.RequestException, ValueError):
        return None


def delete(path):
    try:
        session.delete(BASE + path, timeout=1.0)
    except requests.RequestException:
        pass


def limit(action, quantity, price):
    post('/orders', {'ticker': TICKER, 'type': 'LIMIT', 'action': action,
                     'quantity': int(quantity), 'price': round(price, 2)})


def market(action, quantity):
    return post('/orders', {'ticker': TICKER, 'type': 'MARKET',
                            'action': action, 'quantity': int(quantity)})


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    global _last_beat

    case = get('/case')
    if not case:
        print('No connection to the RIT client — check it is running, the API '
              'is enabled, and API_KEY matches.')
        return
    tick = case['tick']
    loops = 0

    while START_TICK < tick < STOP_TICK and not shutdown:
        loops += 1

        if loops % CASE_POLL_EVERY == 0:
            case = get('/case')
            if case:
                tick = case['tick']
                if case.get('status') == 'STOPPED':
                    break

        secs = get('/securities', {'ticker': TICKER})
        if not secs:
            time.sleep(LOOP_SLEEP)
            continue
        sec = secs[0]
        bid, ask = sec.get('bid'), sec.get('ask')
        position = sec.get('position') or 0

        now = time.monotonic()
        if now - _last_beat >= HEARTBEAT:
            _last_beat = now
            print('[%6.1fs] tick %3d  position %+d' % (now - _t0, tick, position))

        if not bid or not ask:
            time.sleep(LOOP_SLEEP)
            continue

        # --- the brief's rule 1: what is resting right now? ------------------
        orders = get('/orders', {'status': 'OPEN'})
        if orders is None:
            time.sleep(LOOP_SLEEP)
            continue
        have_bid = any(o['action'] == 'BUY' for o in orders)
        have_ask = any(o['action'] == 'SELL' for o in orders)

        if have_bid and have_ask:       # a full pair is working — leave it be
            time.sleep(LOOP_SLEEP)
            continue

        if have_bid or have_ask:        # unpaired leftover — reset the pair
            for o in orders:
                delete('/orders/%s' % o['order_id'])

        # --- quote ------------------------------------------------------------
        mid = (bid + ask) / 2.0
        skew = SKEW_AT_LIMIT * (position / float(POSITION_LIMIT))
        my_bid = mid - HALF_SPREAD - skew
        my_ask = mid + HALF_SPREAD - skew

        # Stay passive — a limit that crosses pays the commission instead of
        # earning the rebate.
        my_bid = min(my_bid, ask - 0.01)
        my_ask = max(my_ask, bid + 0.01)

        if position + SIZE <= POSITION_LIMIT:
            limit('BUY', SIZE, my_bid)
        if position - SIZE >= -POSITION_LIMIT:
            limit('SELL', SIZE, my_ask)

        time.sleep(LOOP_SLEEP)

    # --- flatten -------------------------------------------------------------
    post('/commands/cancel', {'ticker': TICKER})
    for _ in range(20):
        secs = get('/securities', {'ticker': TICKER})
        position = secs[0].get('position') or 0 if secs else 0
        if not position:
            break
        market('SELL' if position > 0 else 'BUY', min(abs(position), 5000))
        time.sleep(0.1)

    secs = get('/securities', {'ticker': TICKER})
    final = secs[0].get('position') or 0 if secs else 0
    trader = get('/trader')
    print('\nLoops: %d   Final position: %+d' % (loops, final))
    if trader:
        print('NLV: %.2f' % trader.get('nlv', 0))


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main()
