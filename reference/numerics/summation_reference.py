#!/usr/bin/env python3
"""Independent reference for pprof.spark.numerics.Summation (ADR-0003).

Re-implements the pairwise and Neumaier algorithms in Python and prints the golden values that
numerics/src/test/scala/pprof/spark/numerics/SummationSuite.scala pins. Python floats are IEEE
754 doubles with the same operation order, so matching bits show that the Scala code implements
the documented algorithms. The data set is built from integer hashes, so it is exactly the same
in both languages. Standard library only.
"""

import math
import struct

SIZE = 10_000
LEAF = 32


def data():
    values = []
    for i in range(SIZE):
        h = (i * 2654435761) & 0xFFFFFFFF
        e = (i * 37) % 81 - 40
        values.append(math.ldexp(h / 4294967296.0 - 0.5, e))
    return values


def pairwise(xs, lo, hi, leaf=LEAF):
    n = hi - lo
    if n == 0:
        return 0.0
    if n <= leaf:
        acc = xs[lo]
        for i in range(lo + 1, hi):
            acc += xs[i]
        return acc
    mid = lo + n // 2
    return pairwise(xs, lo, mid, leaf) + pairwise(xs, mid, hi, leaf)


def neumaier(xs):
    total, compensation = 0.0, 0.0
    for x in xs:
        t = total + x
        if math.isfinite(t):
            if abs(total) >= abs(x):
                compensation += (total - t) + x
            else:
                compensation += (x - t) + total
        total = t
    return total if not math.isfinite(total) else total + compensation


def naive(xs):
    acc = 0.0
    for x in xs:
        acc += x
    return acc


def bits(x):
    return "0x%016xL" % struct.unpack("<Q", struct.pack("<d", x))[0]


if __name__ == "__main__":
    xs = data()
    exact = math.fsum(xs)
    abs_sum = math.fsum(abs(x) for x in xs)
    print("pairwise      ", bits(pairwise(xs, 0, SIZE)), pairwise(xs, 0, SIZE))
    print("neumaier      ", bits(neumaier(xs)), neumaier(xs))
    print("exact (fsum)  ", bits(exact), exact)
    print("naive         ", bits(naive(xs)), naive(xs))
    print("pairwise leaf16", bits(pairwise(xs, 0, SIZE, 16)))
    print("sum |x| (fsum)", bits(abs_sum), abs_sum)
    print("first values  ", [bits(x) for x in xs[:3]])
