import numpy as np
import scipy.interpolate as ntr


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
    x = data_ts[0][first_pos[0]:(last_pos[0] + 1)]
    y = data_v[0][first_pos[0]:(last_pos[0] + 1)]
    v = ntr.interp1d(x, y)(ts)
    for i in range(1, num_of_entries):
        first_pos = np.where(data_ts[i] == start_ts)
        last_pos = np.where(data_ts[i] == end_ts)

        x = data_ts[i][first_pos[0]:(last_pos[0] + 1)]
        y = data_v[i][first_pos[0]:(last_pos[0] + 1)]

        v += ntr.interp1d(x, y)(ts)
    # Return values
    return ts, v


def interpolate_and_avg(data_ts, data_v):
    """interpolate each array based on timestamps and average"""
    num_of_entries = len(data_ts)
    (ts, v) = interpolate_and_sum(data_ts, data_v)
    v /= num_of_entries
    # Return values
    return ts, v
