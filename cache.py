import argparse
import json
import tempfile
import time
import random

from abc import ABCMeta, abstractmethod
from collections import Counter
from operator import itemgetter


class Memory(object):
    def __init__(self):
        self.base = 0

    def allocate(self, size_t, n):
        self.base += size_t * n
        return self.base - size_t * n


class Matrix(object):
    def __init__(self, size_t, n, memory):
        self.base = memory.allocate(size_t, n * n)
        self.size_t = size_t

    def get_addr(self, position):
        return self.base + self.size_t * position


class Cache(object):
    __metaclass__ = ABCMeta

    def __init__(self, line_size, size, *args, **kwargs):
        self.line_size = line_size
        self.size = size
        self.blocks = size / line_size
        self.hits = 0
        self.requests = 0
        # combined stats about hits
        with tempfile.NamedTemporaryFile() as tmpf:
            self.in_cache_file = tmpf.name
            self.in_cache_len = 0
            self.in_cache_history = []

        with tempfile.NamedTemporaryFile() as tmpf:
            self.not_in_cache_file = tmpf.name
            self.not_in_cache_len = 0
            self.not_in_cache_history = []
        # history of lines
        self.history = dict()

        self._post_init(*args, **kwargs)

    @abstractmethod
    def _post_init(self, *args, **kwargs):
        pass

    @abstractmethod
    def _in_cache(self, line_number):
        # checks availability of addr in cache
        # if unloads cache_line writes id to history
        # if adds line do not touch history
        # returns verdict:Boolean

        pass

    def get(self, addr):
        line_number = addr / self.line_size
        verdict = self._in_cache(line_number)
        self.requests += 1

        if verdict:
            self.hits += 1
            in_cache_for = self.requests - self.history[line_number]
            self.add_miss_stat(True, in_cache_for)

        else:
            if line_number not in self.history:
                not_in_cache_for = -1
            else:
                not_in_cache_for = self.requests - self.history[line_number]
            self.history[line_number] = self.requests
            self.add_miss_stat(False, not_in_cache_for)

        return verdict

    def flush(self, in_cache):
        f_name = (self.in_cache_file if in_cache else self.not_in_cache_file)
        with open(f_name, "a") as f:
            for val in (self.in_cache_history if in_cache else self.not_in_cache_history):
                f.write("{}\n".format(val))
            if in_cache:
                self.in_cache_history = []
                self.in_cache_len = 0
            else:
                self.not_in_cache_history = []
                self.not_in_cache_len = 0

    def add_miss_stat(self, in_cache, val):
        if in_cache:
            self.in_cache_len += 1
        else:
            self.not_in_cache_len += 1
        (self.in_cache_history if in_cache else self.not_in_cache_history).append(val)
        if (self.in_cache_len if in_cache else self.not_in_cache_len) > 10000:
            self.flush(in_cache)

    def get_hit_stat(self):
        misses = self.requests - self.hits
        return {
            'requests': self.requests,
            'hits': self.hits,
            'misses': misses,
            'miss_chance': misses * 1.0 / self.requests,
        }

    def get_extended_stat(self):
        in_cache = Counter()
        with open(self.in_cache_file, 'rb') as f:
            in_cache.update((int(l.strip('\n')) for l in f))

        not_in_cache = Counter()
        with open(self.not_in_cache_file, 'rb') as f:
            not_in_cache.update((int(l.strip('\n')) for l in f))

        return {
            'in_cache': sorted(in_cache.items()),
            'not_in_cache': sorted(not_in_cache.items()),
        }


class DirectCache(Cache):
    def _post_init(self, *args, **kwargs):
        self.cache = [-1 for _ in xrange(self.blocks)]
        self.history[-1] = 0

    def _in_cache(self, line_number):
        slot_number = line_number % self.blocks
        v = self.cache[slot_number]
        if v == line_number:
            return True
        else:
            self.history[v] = self.requests
            self.cache[slot_number] = line_number
            return False


def random_displacement(cache):
    return random.choice(cache.keys())


def oldest_displacement(cache):
    return sorted(cache.iteritems(), key=itemgetter(1))[0][0]


class FullyAssociativeCache(Cache):
    def _post_init(self, displacement_strategy, *args, **kwargs):
        self.displacement_strategy = displacement_strategy
        self.cache = dict()
        self.used_size = 0
        # hack for set associative
        self.last_replaced = -1

    def _in_cache(self, line_number):
        if line_number in self.cache:
            return True
        else:
            if self.used_size < self.blocks:
                self.used_size += 1
                self.last_replaced = -1
            else:
                to_be_replaced = self.displacement_strategy(self.cache)
                self.history[to_be_replaced] = self.requests + 1
                self.last_replaced = to_be_replaced
                del self.cache[to_be_replaced]
            self.cache.update({line_number: self.requests + 1})
            return False


class SetAssociativeCache(Cache):
    def _post_init(self, displacement_strategy, sets_amount, *args, **kwargs):
        self.sets_block_size = self.size / sets_amount
        self.cache = [
            FullyAssociativeCache(self.line_size, self.sets_block_size, displacement_strategy=displacement_strategy) for
            _ in xrange(sets_amount)]
        self.sets_amount = sets_amount

    def _in_cache(self, line_number):
        set_number = (line_number / self.sets_block_size) % self.sets_amount
        res = self.cache[set_number].get(line_number * self.line_size)

        # kill me pls
        lr = self.cache[set_number].last_replaced
        if lr != -1:
            self.history[lr] = self.requests + 1
        return res


def algo(cache, n, a, b, c):
    # faster
    cag = cache.get
    ag = a.get_addr
    bg = b.get_addr
    cg = c.get_addr

    for i in xrange(n):
        for j in xrange(n):
            cag(cg(i * n + j))
            for k in xrange(n):
                cag(ag(i * n + k))
                cag(bg(i * n + k))
                cag(cg(i * n + j))


def try_algo(n, cache, name, element_size=4):
    mem = Memory()
    a = Matrix(element_size, n, mem)
    b = Matrix(element_size, n, mem)
    c = Matrix(element_size, n, mem)
    t = time.time()
    algo(cache, n, a, b, c)
    # drop all to files
    cache.flush(True)
    cache.flush(False)

    spent = time.time() - t
    print "testing time", spent
    with open('{}_{}_results.dump'.format(name, n), 'wb') as f:
        f.write(str(spent) + "\n")
        f.write(json.dumps(cache.get_hit_stat()) + '\n')
        f.flush()

        f.write(json.dumps(cache.get_extended_stat()) + '\n')
    print cache.get_hit_stat()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', default=0, help='mode to use. see sources')
    parser.add_argument('-s', '--size', default=1024, help='matrix size')
    parser.add_argument('-a', '--sets', default=3)
    return parser.parse_args()


def main():
    options = parse_args()
    choice = [
        ('direct', DirectCache(64, 3 * 1024)),
        ('associative_oldest', FullyAssociativeCache(64, 3 * 1024, displacement_strategy=oldest_displacement)),
        ('associative_random', FullyAssociativeCache(64, 3 * 1024, displacement_strategy=random_displacement)),
        ('set_{}_oldest'.format(options.sets), SetAssociativeCache(64, 3 * 1024,
                                                                   displacement_strategy=oldest_displacement,
                                                                   sets_amount=int(options.sets))),
        ('set_{}_random'.format(options.sets), SetAssociativeCache(64, 3 * 1024,
                                                                   displacement_strategy=random_displacement,
                                                                   sets_amount=int(options.sets))),
    ][int(options.mode)]
    name, cache = choice
    try_algo(int(options.size), cache, name)


if __name__ == "__main__":
    main()

