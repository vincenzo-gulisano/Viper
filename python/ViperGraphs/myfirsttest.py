__author__ = 'Vincenzo Gulisano'

import statistics as stat

import numpy as np
import scipy.interpolate as ntr
import matplotlib.pyplot as plt

import myfirsttest


def interpolate_and_sum(data_ts, data_v):
    """interpolate each array based on timestamps and sum"""
    num_of_entries = len(data_ts)
    # find highest start timestamp
    start_ts = data_ts[0][0]
    for i in range(1, num_of_entries):
        if data_ts[i][0] > start_ts:
            start_ts = data_ts[i][0]
    # find lowest end timestamp
    end_ts = data_ts[0][-1]
    for i in range(1, num_of_entries):
        if data_ts[i][-1] < end_ts:
            end_ts = data_ts[i][-1]
    # Create timestamps
    ts = list(range(start_ts, end_ts))
    # now interpolate and sum
    first_pos = np.where(data_ts[0] == start_ts)
    last_pos = np.where(data_ts[0] == end_ts)
    x = data_ts[0][first_pos[0]:last_pos[0]];
    y = data_v[0][first_pos[0]:last_pos[0]];
    v = ntr.interp1d(x, y)(ts)
    for i in range(1, num_of_entries):
        first_pos = np.where(data_ts[i] == start_ts)
        last_pos = np.where(data_ts[i] == end_ts)

        x = data_ts[i][first_pos[0]:last_pos[0]];
        y = data_v[i][first_pos[0]:last_pos[0]];

        v += ntr.interp1d(x, y)(ts)
    # Return values
    return ts, v


def compute_op_rate(base_folder, op_name, instances):
    spout_data_ts = []
    spout_data_v = []
    for i in range(0, instances):
        time, v = np.genfromtxt(base_folder + 'p' + str(r) + '_' + op_name + '.' + str(i) + '.rate.csv', dtype=int,
                                delimiter=',').transpose()
        time /= 1000
        spout_data_ts.append(time)
        spout_data_v.append(v)
    return myfirsttest.interpolate_and_sum(spout_data_ts, spout_data_v)


base_folder = '/Users/vinmas/repositories/viper_experiments/scalability_stateless_50/'
mul_v_means = []
for r in range(2, 190, 10):
    (spout_ts, spout_v) = myfirsttest.compute_op_rate(base_folder, 'spout', 2)
    (mul_merger_ts, mul_merger_v) = myfirsttest.compute_op_rate(base_folder, 'mul_merger', r)
    (mul_ts, mul_v) = myfirsttest.compute_op_rate(base_folder, 'mul', r)
    mul_v_means.append(stat.mean(mul_v[30:90]))
    print(
        'P' + str(r) + ' spout: ' + str(stat.mean(spout_v[30:90])) + ' mul_merger: ' + str(
            stat.mean(mul_merger_v[30:90])) + ' mul: ' + str(stat.mean(mul_v[30:90])))

threads = [x * 2 for x in range(2, 190, 10)]
plt.plot(threads, mul_v_means)

plt.xlabel('Number of threads')
plt.ylabel('Throughput (t/s)')
plt.title('Scalability')
plt.grid(True)
plt.show()
