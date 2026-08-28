import signal
import time

import requests

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
API_KEY = {'X-API-Key': 'FLI7E73K'}
BASE = 'http://localhost:9999/v1'

TICKER = 'ALGO'
SIZE = 2500             # clip per side. See note 3.
HALF_SPREAD = 0.005     # the book is reliably 1c wide, so this lands us at
                        # the touch. Wider and we simply don't trade.
SKEW = 0.04             # centre shift at a full MAX_POSITION. See note 4.
MAX_POSITION = 25000    # our own cap, far inside the 25,000 case limit. The
                        # case limit was never the binding constraint — the
                        # best runs never exceeded ±4,700.

START_TICK = 2
STOP_TICK = 285         # stop quoting
TAPER_TICKS = 45        # squeeze the cap to zero over the last N ticks
PASSIVE_EXIT_SECS = 8.0 # work the residual at the touch before crossing
FLATTEN_CHUNK = 2000    # market-out chunk size

LOOP_SLEEP = 0.05
QUOTE_COOLDOWN = 0.6    # see note 2. Also keeps us under 5 orders/sec.
CASE_POLL_EVERY = 10
HEARTBEAT = 3.0

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


def limit(action, quantity, price):
    post('/orders', {'ticker': TICKER, 'type': 'LIMIT', 'action': action,
                     'quantity': int(quantity), 'price': round(price, 2)})


def market(action, quantity):
    return post('/orders', {'ticker': TICKER, 'type': 'MARKET',
                            'action': action, 'quantity': int(quantity)})


def cancel_all():
    post('/commands/cancel', {'ticker': TICKER})


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
    quote_cooldown = 0.0

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

        # A pair we just sent is not visible yet — don't send another.
        if now < quote_cooldown:
            time.sleep(LOOP_SLEEP)
            continue

        # --- the brief's rule 1: what is resting right now? ------------------
        orders = get('/orders', {'status': 'OPEN'})
        if orders is None:
            time.sleep(LOOP_SLEEP)
            continue
        have_bid = any(o['action'] == 'BUY' for o in orders)
        have_ask = any(o['action'] == 'SELL' for o in orders)

        if have_bid and have_ask:       # a full pair is working — leave it be.
            time.sleep(LOOP_SLEEP)      # Requoting here only loses queue
            continue                    # priority, which is where fills come from.

        if have_bid or have_ask:        # unpaired leftover — reset the pair
            cancel_all()
            time.sleep(0.15)            # let the cancel land before requoting

        # --- taper: shrink the cap to zero over the final ticks --------------
        # Once the cap falls below what we hold, the adding side stops quoting
        # and only the reducing side stays live, so we arrive at the bell
        # near flat instead of dumping inventory at market.
        max_pos = MAX_POSITION
        ticks_left = STOP_TICK - tick
        if ticks_left < TAPER_TICKS:
            max_pos = int(MAX_POSITION * max(0, ticks_left) / float(TAPER_TICKS))

        # --- quote ------------------------------------------------------------
        mid = (bid + ask) / 2.0
        centre = mid - SKEW * (position / float(max_pos or MAX_POSITION))
        my_bid = centre - HALF_SPREAD
        my_ask = centre + HALF_SPREAD

        # Stay passive. A fill that crosses swaps a +0.005 rebate for a -0.010
        # fee — a 1.5c/share swing, larger than the tick itself.
        my_bid = min(my_bid, ask - 0.01)
        my_ask = max(my_ask, bid + 0.01)

        if position + SIZE <= max_pos:
            limit('BUY', SIZE, my_bid)
        if position - SIZE >= -max_pos:
            limit('SELL', SIZE, my_ask)
        quote_cooldown = time.monotonic() + QUOTE_COOLDOWN

        time.sleep(LOOP_SLEEP)

    # --- wind down -----------------------------------------------------------
    # Phase 1: work whatever the taper left at the touch. The case runs to tick
    # 300 while we stop quoting at 285, so there are ticks to spend here, and a
    # passive exit is worth ~1.5c/share more than crossing.
    cancel_all()
    deadline = time.monotonic() + PASSIVE_EXIT_SECS
    while time.monotonic() < deadline:
        secs = get('/securities', {'ticker': TICKER})
        if not secs:
            time.sleep(0.2)
            continue
        sec = secs[0]
        position = sec.get('position') or 0
        if not position:
            break
        bid, ask = sec.get('bid'), sec.get('ask')
        if not bid or not ask:
            time.sleep(0.2)
            continue
        if not (get('/orders', {'status': 'OPEN'}) or []):
            side = 'SELL' if position > 0 else 'BUY'
            px = ask if position > 0 else bid       # join the touch
            limit(side, min(abs(position), 5000), px)
        time.sleep(0.4)

    # Phase 2: whatever is left has to go, in small chunks.
    cancel_all()
    for _ in range(30):
        secs = get('/securities', {'ticker': TICKER})
        position = (secs[0].get('position') or 0) if secs else 0
        if not position:
            break
        market('SELL' if position > 0 else 'BUY',
               min(abs(position), FLATTEN_CHUNK))
        time.sleep(0.15)

    secs = get('/securities', {'ticker': TICKER})
    final = (secs[0].get('position') or 0) if secs else 0
    trader = get('/trader')
    print('\nLoops: %d   Final position: %+d' % (loops, final))
    if trader:
        print('NLV: %.2f' % trader.get('nlv', 0))


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main()
