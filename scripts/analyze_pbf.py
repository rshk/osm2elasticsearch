"""
Let's try reading those PBF files..
"""

from __future__ import print_function

import sys
from functools import partial
from imposm.parser import OSMParser

counter = {
    'nodes': 0,
    'ways': 0,
    'relations': 0,
}


def inc_counter(name, *a):
    counter[name] += 1

p = OSMParser(concurrency=4,
              ways_callback=partial(inc_counter, 'ways'),
              nodes_callback=partial(inc_counter, 'nodes'),
              relations_callback=partial(inc_counter, 'relations'))

p.parse(sys.argv[1])

print("Nodes:", counter['nodes'])
print("Ways:", counter['ways'])
print("Relations:", counter['relations'])
