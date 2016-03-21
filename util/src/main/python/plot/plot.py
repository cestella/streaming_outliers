import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as md
import sys
import datetime as dt

with open(sys.argv[1]) as f:
    ts_content = [ t.split(',') for t in f.readlines()]
with open(sys.argv[2]) as f:
    outlier_content = [ t.split(',') for t in f.readlines()]

outliers_t = np.array(md.date2num([ dt.datetime.fromtimestamp(long(t[0])/1000.0) for t in outlier_content ]))
outliers_y = np.array([ float(t[1]) for t in outlier_content ])

normal_t = np.array(md.date2num([ dt.datetime.fromtimestamp(long(t[0])/1000.0) for t in ts_content ]))
normal_y = np.array([ float(t[1]) for t in ts_content ])
plt.subplots_adjust(bottom=0.2)
plt.xticks( rotation=25 )
ax=plt.gca()
xfmt = md.DateFormatter('%Y-%m-%d %H:%M:%S')
ax.xaxis.set_major_formatter(xfmt)
plt.plot(np.array(outliers_t), np.array(outliers_y), 'ro', np.array(normal_t), np.array(normal_y), 'b.')
plt.show()
