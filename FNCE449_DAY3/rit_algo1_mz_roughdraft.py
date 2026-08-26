# This is a python example algorithm using REST API for the RIT ALGO1 Case

import signal
import requests
from time import sleep

# this class definition allows us to print error messages and stop the program when needed
class ApiException(Exception):
    pass

# this signal handler allows for a graceful shutdown when CTRL+C is pressed
def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True

API_KEY = {'X-API-Key': 'FLI7E73K'}
shutdown = False

# trading controls
MIN_EDGE = 0.05
AGGRESSIVE_EDGE = 0.20
ORDER_SIZE_FRACTION = 0.25
MIN_ORDER_SIZE = 1
MAX_ORDER_SIZE = 1000

# this helper method returns the current 'tick' of the running case
def get_tick(session):
    resp = session.get('http://localhost:9999/v1/case')
    if resp.ok:
        case = resp.json()
        return case['tick']
    raise ApiException('The API key provided in this Python code must match that in the RIT client.')

# this helper method returns the bid and ask for a given security
def ticker_bid_ask(session, ticker):
    payload = {'ticker': ticker}
    resp = session.get('http://localhost:9999/v1/securities/book', params=payload)
    if resp.ok:
        book = resp.json()
        return book['bids'][0]['price'], book['asks'][0]['price']
    raise ApiException('The API key provided in this Python code must match that in the RIT client.')


def ticker_top_of_book(session, ticker):
    payload = {'ticker': ticker}
    resp = session.get('http://localhost:9999/v1/securities/book', params=payload)
    if resp.ok:
        book = resp.json()
        best_bid = book.get('bids', [{}])[0]
        best_ask = book.get('asks', [{}])[0]

        bid_quantity = int(best_bid.get('quantity', best_bid.get('size', best_bid.get('volume', 0))))
        ask_quantity = int(best_ask.get('quantity', best_ask.get('size', best_ask.get('volume', 0))))

        return {
            'bid_price': best_bid.get('price', 0),
            'bid_quantity': bid_quantity,
            'ask_price': best_ask.get('price', 0),
            'ask_quantity': ask_quantity,
        }

    raise ApiException('The API key provided in this Python code must match that in the RIT client.')


def submit_limit_order(session, ticker, action, quantity, price):
    params = {
        'ticker': ticker,
        'type': 'LIMIT',
        'quantity': quantity,
        'action': action,
        'price': price,
    }
    resp = session.post('http://localhost:9999/v1/orders', params=params)
    if resp.ok:
        return resp.json()
    raise ApiException('Order submission failed for {} {}'.format(action, ticker))


def execute_spread_trade(session, buy_ticker, sell_ticker, buy_book, sell_book):
    edge = sell_book['bid_price'] - buy_book['ask_price']
    if edge < MIN_EDGE:
        return False

    available_quantity = min(buy_book['ask_quantity'], sell_book['bid_quantity'])
    if available_quantity <= 0:
        return False

    quantity = max(MIN_ORDER_SIZE, min(MAX_ORDER_SIZE, int(available_quantity * ORDER_SIZE_FRACTION)))
    if quantity <= 0:
        return False

    if edge >= AGGRESSIVE_EDGE:
        buy_price = buy_book['ask_price']
        sell_price = sell_book['bid_price']
    else:
        buy_price = buy_book['bid_price']
        sell_price = sell_book['ask_price']

    submit_limit_order(session, buy_ticker, 'BUY', quantity, buy_price)
    submit_limit_order(session, sell_ticker, 'SELL', quantity, sell_price)
    return True

def main():
    with requests.Session() as s:
        s.headers.update(API_KEY)
        tick = get_tick(s)
        while tick > 5 and tick < 295 and not shutdown:
            crzy_m_book = ticker_top_of_book(s, 'CRZY_M')
            crzy_a_book = ticker_top_of_book(s, 'CRZY_A')

            if crzy_m_book['bid_price'] - crzy_a_book['ask_price'] >= MIN_EDGE:
                execute_spread_trade(s, 'CRZY_A', 'CRZY_M', crzy_a_book, crzy_m_book)
                sleep(1)

            if crzy_a_book['bid_price'] - crzy_m_book['ask_price'] >= MIN_EDGE:
                execute_spread_trade(s, 'CRZY_M', 'CRZY_A', crzy_m_book, crzy_a_book)
                sleep(1)
                
            # IMPORTANT to update the tick at the end of the loop to check that the algorithm should still run or not
            tick = get_tick(s)

if __name__ == '__main__':
    # register the custom signal handler for graceful shutdowns
    signal.signal(signal.SIGINT, signal_handler)
    main()