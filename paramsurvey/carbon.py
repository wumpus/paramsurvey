import sys
import multiprocessing
import functools
import socket
import pickle
import struct


pool = None


def init():
    global pool

    if not pool:
        ctx = multiprocessing.get_context('spawn')
        pool = ctx.Pool(processes=4)


class TimeBin:
    '''
    Given a series of values over time intervals, rebin them into N-second bins
    Useful for reporting 1-second and 30-second values to Carbon when your process
    is busy and sometimes produces values late
    '''

    def __init__(self, interval=1.0):
        self.interval = interval
        self.t0 = 0.
        self.fraction = 0.
        self.value = 0
        self.tuples = []

    def point(self, t, value):
        t0 = int(t / self.interval) * self.interval
        fraction = t - t0

        if t < self.t0 + self.fraction:
            raise ValueError('time t is in the past')

        if self.t0 == 0:
            # initial bin
            self.t0 = t0
            self.fraction = 0.

        if t0 == self.t0:
            # we are in the same bin as before
            # add in an appropriate amount of value
            delta = (fraction - self.fraction)/self.interval
            self.value += value * delta
            self.fraction = fraction
        elif t0 > self.t0:
            # we are in a future bin.
            # finish off the previous bin, push it as a tuple
            delta = (self.interval - self.fraction)/self.interval
            self.value += value * delta
            self.tuples.append((self.t0, self.value))
            # make 0+ intermediate bins
            while self.t0 + self.interval + 0.0001 < t0:
                self.t0 += self.interval
                self.tuples.append((self.t0, value))
            # make a final bin
            self.t0 = t0
            self.fraction = fraction
            delta = fraction / self.interval
            self.value = value * delta

    def gettuples(self, path=None):
        tuples = self.tuples
        self.tuples = []

        if path:
            return [(path, t) for t in tuples]
        else:
            return tuples


def carbon_push_pickle(tuples, carbon_server=None, carbon_port=None):
    if not pool:
        raise RuntimeError('carbon_push called without carbon.init')

    callback_partial = functools.partial(callback, tuples, carbon_server, carbon_port)
    error_callback_partial = functools.partial(callback, tuples, carbon_server, carbon_port)

    pool.apply_async(carbon_push_pickle_remote, (tuples, carbon_server, carbon_port),
                     {}, callback_partial, error_callback_partial)


def callback(tuples, server, port, result):
    pass


def error_callback(tuples, server, port, exc):
    print('sad carbon push callback, server {} port {} exception {}'.format(server, port, repr(exc)), file=sys.stderr)


def carbon_push_pickle_remote(tuples, server, port):
    payload = pickle.dumps(tuples, protocol=2)
    header = struct.pack("!L", len(payload))
    message = header + payload

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.connect((server, port))
        msglen = len(message)
        totalsent = 0
        while totalsent < msglen:
            sent = s.send(message[totalsent:])
            if sent == 0:
                raise RuntimeError('carbon_push_pickle socket connection broken')
            totalsent += sent
        s.close()
    except Exception as e:  # (OSError, RuntimeError) -- if not caught, will be silent, so catch all
        print('exception in carbon_push_pickle_remote, server {} port {} exception {}'.format(server, port, repr(e)), file=sys.stderr)
