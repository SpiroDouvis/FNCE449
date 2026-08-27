"""
RIT ALGO2 — cockpit. Fast dumb execution, slow smart supervision.

This is not an algorithm in the sense of the other two files. It makes no
strategic decisions of its own. It does three things:

  1. Executes quotes at machine speed using whatever parameters it is told.
  2. Publishes a full picture of the market and our book to state.json.
  3. Re-reads control.json every loop, so a supervisor (Claude, or you) can
     change strategy mid-case without restarting anything.

The division of labour is the point. Order placement needs to happen in
milliseconds and requires no judgement. Deciding whether the market is
trending, whether inventory is building for a reason, and whether to be
quoting at all requires judgement and can happen every 10-30 seconds. This
file does the first job and exposes the controls for the second.

Files (written next to this script):
    state.json    - written ~2x/sec. Read this to see what is happening.
    control.json  - read every loop. Write this to change behaviour.
    cockpit.log   - append-only record of fills and control changes.

Usage:  python rit_algo2_cockpit.py
Stop:   CTRL+C (cancels resting orders and flattens)
"""

import json
import os
import signal
import time
from collections import deque

import requests

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
API_KEY = {'X-API-Key': 'FLI7E73K'}
BASE = 'http://localhost:9999/v1'
TICKER = 'ALGO'

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(HERE, 'state.json')
CONTROL_FILE = os.path.join(HERE, 'control.json')
LOG_FILE = os.path.join(HERE, 'cockpit.log')

START_TICK = 2
STOP_TICK = 285         # hard stop; supervisor can flatten earlier via control
TAPER_TICKS = 45        # squeeze the position cap to zero over the last N ticks
PASSIVE_EXIT_SECS = 8.0 # work the residual at the touch before crossing
FLATTEN_CHUNK = 2000    # market-out chunk. Smaller chunks walk the book less
                        # than one 5,000 print, which is where the 6c/share of
                        # slippage came from last run.
LOOP_SLEEP = 0.05
CASE_POLL_EVERY = 10
STATE_EVERY = 0.5       # seconds between state.json writes
MID_HISTORY = 40        # ~40 samples of mid price for trend reading

# Defaults, used until control.json says otherwise.
DEFAULT_CONTROL = {
    'seq': 0,           # bump this when you change anything; state echoes it
                        # back so you can confirm the change was picked up
    'mode': 'quote',    # quote | flatten | pause | manual
    'half_spread': 0.02,    # quote this far either side of the centre
    'size': 4000,       # clip size per side
    'skew': 0.03,       # price shift at a full position limit (inventory term)
    'bias': 0.00,       # discretionary shift of BOTH quotes. Positive = lean
                        # bullish (bid and ask both higher). This is the
                        # supervisor's directional view; the algo has none.
    'max_position': 25000,
    'orders': [],       # manual mode only: [{"side","qty","px"|null}]
    'note': '',         # free text, echoed to the log for the record
}

shutdown = False
_t0 = time.monotonic()

session = requests.Session()
session.headers.update(API_KEY)


def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True


# ----------------------------------------------------------------------------
# API
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
    return post('/orders', {'ticker': TICKER, 'type': 'LIMIT', 'action': action,
                            'quantity': int(quantity), 'price': round(price, 2)})


def market(action, quantity):
    return post('/orders', {'ticker': TICKER, 'type': 'MARKET',
                            'action': action, 'quantity': int(quantity)})


# ----------------------------------------------------------------------------
# Control / state plumbing
# ----------------------------------------------------------------------------
def log(msg):
    line = '[%6.1fs] %s' % (time.monotonic() - _t0, msg)
    print(line)
    try:
        with open(LOG_FILE, 'a') as fh:
            fh.write(line + '\n')
    except OSError:
        pass


def read_control(current):
    """
    Re-read control.json every loop. A local file read is microseconds, so
    this costs nothing next to the API calls, and it means the supervisor can
    change strategy at any instant without a restart or a race.

    A malformed or half-written file just leaves the previous control in
    place — never crash the executor over a bad edit.
    """
    try:
        with open(CONTROL_FILE) as fh:
            loaded = json.load(fh)
    except (OSError, ValueError):
        return current
    merged = dict(DEFAULT_CONTROL)
    merged.update({k: v for k, v in loaded.items() if k in DEFAULT_CONTROL})
    if merged['seq'] != current.get('seq'):
        log('CONTROL seq=%s mode=%s half=%.3f size=%d skew=%.3f bias=%+.3f  %s'
            % (merged['seq'], merged['mode'], merged['half_spread'],
               merged['size'], merged['skew'], merged['bias'], merged['note']))
    return merged


def write_state(payload):
    """Atomic write — the supervisor must never read a half-written file."""
    tmp = STATE_FILE + '.tmp'
    try:
        with open(tmp, 'w') as fh:
            json.dump(payload, fh, indent=1)
        os.replace(tmp, STATE_FILE)
    except OSError:
        pass


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    case = get('/case')
    if not case:
        print('No connection to the RIT client.')
        return
    tick = case['tick']

    control = dict(DEFAULT_CONTROL)
    if not os.path.exists(CONTROL_FILE):
        write_state({})
        with open(CONTROL_FILE, 'w') as fh:
            json.dump(DEFAULT_CONTROL, fh, indent=1)
        log('wrote default %s' % CONTROL_FILE)

    mids = deque(maxlen=MID_HISTORY)
    loops = 0
    last_state = 0.0
    last_position = None
    buy_filled = sell_filled = 0
    done_manual = -1        # seq whose manual orders we've already sent
    quote_cooldown = 0.0    # suppress requoting while a placement is in flight

    while START_TICK < tick < STOP_TICK and not shutdown:
        loops += 1
        control = read_control(control)

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

        if last_position is not None:
            d = position - last_position
            if d > 0:
                buy_filled += d
            elif d < 0:
                sell_filled += -d
            if d:
                log('FILL %+d -> position %+d' % (d, position))
        last_position = position

        orders = get('/orders', {'status': 'OPEN'}) or []
        my_bids = [o for o in orders if o['action'] == 'BUY']
        my_asks = [o for o in orders if o['action'] == 'SELL']

        if bid and ask:
            mids.append(round((bid + ask) / 2.0, 4))

        # ---- publish state ------------------------------------------------
        now = time.monotonic()
        if now - last_state >= STATE_EVERY:
            last_state = now
            trader = get('/trader') or {}
            write_state({
                'elapsed': round(now - _t0, 1),
                'tick': tick,
                'control_seq': control['seq'],
                'mode': control['mode'],
                'bid': bid, 'ask': ask,
                'spread': round(ask - bid, 4) if (bid and ask) else None,
                'last': sec.get('last'),
                'bid_size': sec.get('bid_size'), 'ask_size': sec.get('ask_size'),
                'position': position,
                'nlv': trader.get('nlv'),
                'my_bids': [{'px': o.get('price'), 'qty': o.get('quantity'),
                             'filled': o.get('quantity_filled')} for o in my_bids],
                'my_asks': [{'px': o.get('price'), 'qty': o.get('quantity'),
                             'filled': o.get('quantity_filled')} for o in my_asks],
                'buy_filled': buy_filled, 'sell_filled': sell_filled,
                'mid_history': list(mids),
            })

        mode = control['mode']

        # ---- pause: no quotes, but keep publishing state -------------------
        if mode == 'pause':
            if orders:
                post('/commands/cancel', {'ticker': TICKER})
            time.sleep(LOOP_SLEEP)
            continue

        # ---- flatten: get to zero and stay there ---------------------------
        if mode == 'flatten':
            if orders:
                post('/commands/cancel', {'ticker': TICKER})
            if position:
                market('SELL' if position > 0 else 'BUY',
                       min(abs(position), 5000))
            time.sleep(LOOP_SLEEP)
            continue

        # ---- manual: send exactly what the supervisor listed, once ---------
        if mode == 'manual':
            if control['seq'] != done_manual:
                done_manual = control['seq']
                for o in control['orders']:
                    side = o.get('side', 'BUY').upper()
                    qty = int(o.get('qty', 0))
                    px = o.get('px')
                    if qty <= 0:
                        continue
                    if px is None:
                        market(side, qty)
                        log('MANUAL MARKET %s %d' % (side, qty))
                    else:
                        limit(side, qty, float(px))
                        log('MANUAL LIMIT %s %d @ %.2f' % (side, qty, float(px)))
            time.sleep(LOOP_SLEEP)
            continue

        # ---- quote ---------------------------------------------------------
        if not bid or not ask:
            time.sleep(LOOP_SLEEP)
            continue

        # A just-placed order is not visible to GET /orders for a few hundred
        # ms. Without this cooldown the loop sees an empty book at 20Hz and
        # fires another pair every 50ms — three or four pairs stack up before
        # the first one appears, multiplying our real exposure.
        if time.monotonic() < quote_cooldown:
            time.sleep(LOOP_SLEEP)
            continue

        if my_bids and my_asks:         # full pair working — leave it alone
            time.sleep(LOOP_SLEEP)
            continue
        if my_bids or my_asks:          # unpaired leftover — reset
            post('/commands/cancel', {'ticker': TICKER})
            time.sleep(0.15)            # let the cancel land before requoting

        size = int(control['size'])
        max_pos = int(control['max_position'])

        # ---- taper -----------------------------------------------------------
        # Squeeze the position cap linearly to zero over the last TAPER_TICKS.
        # Once the cap drops below what we're holding, the adding side stops
        # quoting and only the reducing side stays live, so inventory bleeds
        # off through PASSIVE fills that still earn the rebate.
        #
        # Without this we carried +8,000 into the bell and market-dumped it:
        # active sells printed 20.0316 against a passive 20.0404, and the P&L
        # chart gave back ~486 in the final ticks. Exiting early and passively
        # is worth roughly 2.5c/share versus exiting late and actively.
        ticks_left = STOP_TICK - tick
        if ticks_left < TAPER_TICKS:
            max_pos = int(max_pos * max(0.0, ticks_left) / float(TAPER_TICKS))

        centre = (bid + ask) / 2.0 + control['bias']
        centre -= control['skew'] * (position / float(max_pos or 25000))

        my_bid = min(centre - control['half_spread'], ask - 0.01)
        my_ask = max(centre + control['half_spread'], bid + 0.01)

        if position + size <= max_pos:
            limit('BUY', size, my_bid)
        if position - size >= -max_pos:
            limit('SELL', size, my_ask)
        quote_cooldown = time.monotonic() + 0.6

        time.sleep(LOOP_SLEEP)

    # ---- wind down ---------------------------------------------------------
    # Phase 1: work whatever the taper left at the touch. A passive exit earns
    # +0.005/share; a market exit pays -0.010 and walks the book on top. With
    # the case running to tick 300 and the loop stopping at 285, there are
    # ticks to spare — spend them rather than crossing immediately.
    post('/commands/cancel', {'ticker': TICKER})
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
        orders = get('/orders', {'status': 'OPEN'}) or []
        if not orders:                  # only requote once the last one is gone
            side = 'SELL' if position > 0 else 'BUY'
            px = ask if position > 0 else bid   # join the touch, stay passive
            limit(side, min(abs(position), 5000), px)
        time.sleep(0.4)

    # Phase 2: whatever is still left has to go, in small chunks.
    post('/commands/cancel', {'ticker': TICKER})
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
    trader = get('/trader') or {}
    log('DONE loops=%d final_position=%+d nlv=%.2f'
        % (loops, final, trader.get('nlv', 0)))


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main()
