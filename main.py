import threading
import multiprocessing
import os
import time
import traceback
from collections import deque

import ccxt
import redis as redispy

bf = ccxt.bitflyer()
bf.timeout = 30000
bf.apiKey = os.environ['BITFLYER_API_KEY']
bf.secret = os.environ['BITFLYER_API_SECRET']
bf.verbose = False

redis = redispy.Redis(
    connection_pool=redispy.ConnectionPool(host='localhost', port=6379, db=0))


def set_interval(interval, expr: callable, args=(), kwargs=None):
    """
    Process だと API の nonce のエラー出ることがあるので Thread
    :param int interval:
    :param callable expr:
    :param tuple args:
    :param dict kwargs:
    :rtype: threading.Thread
    """
    kwargs = kwargs or {}

    def fn():
        while True:
            try:
                expr(*args, **kwargs)
            except:
                print(traceback.format_exc())
            time.sleep(interval)

    thread = threading.Thread(target=fn, args=(), daemon=True)
    thread.start()
    return thread


class PositionCounter:
    def __init__(self, bf, redis):
        """
        :param ccxt.bitflyer bf:
        :param redispy.Redis redis:
        """
        self._bf = bf
        self._redis = redis
        self._position = 0.0

        self._uniq_ids = deque(maxlen=300)
        self._excs_queue = multiprocessing.Queue()

    @property
    def position(self):
        return round(self._position, 8)

    def enqueue_exc(self, exc):
        self._excs_queue.put(exc)

    def update(self):
        new_execs = []
        while not self._excs_queue.empty():
            exc = self._excs_queue.get()
            if exc['id'] in self._uniq_ids:
                continue
            self._uniq_ids.append(exc['id'])
            new_execs.append(exc)

        for exc in new_execs:
            delta = exc['amount'] if exc['side'].lower() == 'buy' else -exc['amount']
            self._position += delta
        self._redis.set('position', round(self._position, 8))

    def refresh(self):
        self._position = self.fetch()
        for exc in bf.fetch_my_trades('FX_BTC_JPY', limit=100):
            self._uniq_ids.append(exc['id'])

    def fetch(self):
        positions = self._bf.private_get_getpositions({'product_code': 'FX_BTC_JPY'})
        buys = sum([p['size'] for p in positions if p['side'] == 'BUY'])
        sells = sum([p['size'] for p in positions if p['side'] == 'SELL'])
        return buys - sells


def load_executions():
    executions = bf.fetch_my_trades('FX_BTC_JPY', limit=100)
    for exc in executions:
        counter.enqueue_exc(exc)


counter = PositionCounter(bf=bf, redis=redis)
counter.refresh()

set_interval(10, lambda: load_executions())
set_interval(1, lambda: counter.update())
set_interval(10, lambda: print(f'bF: {counter.fetch()} BTC'))

while True:
    time.sleep(1)
    redis_pos = round(float(redis.get('position')), 8)
    print(f'Redis: {redis_pos} BTC')
    print(f'Counter: {counter.position} BTC')
