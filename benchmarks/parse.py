#!/usr/bin/env python3

# You need pandas installed to run this
# It should be run in the benchmarks directory

from glob import glob
import json
import re
import pandas as pd
pd.options.display.float_format = '{:,.2f}'.format

dfs = []
for file in glob('benchmark-*.out'):

    key = re.search(r'benchmark-([^\.]+).out', file).group(1)
    with open(file) as f:
        times = [float(l.split()[-1]) for l in f if 'Elapsed:' in l]

    df = pd.DataFrame(times).describe()
    df.loc['#u'] = int(key)
    dfs.append(df)

stats = pd.concat(dfs, axis=1).transpose().set_index('#u').sort_index()
stats.index = stats.index.astype(int)
print(stats)