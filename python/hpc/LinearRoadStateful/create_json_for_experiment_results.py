__author__ = 'vinmas'
import os.path
import numpy as np
from interpolation import interpolate_and_sum
from interpolation import interpolate_and_avg
import json
import sys

def read_parallel_op_data_time_value(files, interpolation_type):
    data_ts = []
    data_v = []
    for file in files:
        t, v = np.genfromtxt(file, dtype=int, delimiter=',').transpose()
        t /= 1000
        data_ts.append(t)
        data_v.append(v)

    time = []
    value = []
    if interpolation_type == 0:
        (time, value) = interpolate_and_sum(data_ts, data_v)
    elif interpolation_type == 1:
        (time, value) = interpolate_and_avg(data_ts, data_v)

    return time, value


def read_parallel_op_statistics(prefix, op_name, instances):
    data = dict()

    # RATE
    try:
        rate_files = []
        for i in range(0, instances):
            file_name = op_name + '.' + str(i) + '.rate'
            file_path = prefix + file_name
            rate_files.append(file_path + '.csv')
        (time, value) = read_parallel_op_data_time_value(rate_files, 0)
        data[op_name + "_rate_ts"] = time
        data[op_name + "_rate_value"] = value.tolist()
    except:
        print('Could not compute rate stats for operator ' + op_name)
        print('Error:' + sys.exc_info()[0])

    # INVOCATIONS
    try:
        invocations_files = []
        for i in range(0, instances):
            file_name = op_name + '.' + str(i) + '.invocations'
            file_path = prefix + file_name
            invocations_files.append(file_path + '.csv')
        (time, value) = read_parallel_op_data_time_value(invocations_files, 0)
        data[op_name + "_invocations_ts"] = time
        data[op_name + "_invocations_value"] = value.tolist()
    except:
        print('Could not compute invocations stats for operator ' + op_name)
        print('Error:' + sys.exc_info()[0])

    # COST
    try:
        cost_files = []
        for i in range(0, instances):
            file_name = op_name + '.' + str(i) + '.cost'
            file_path = prefix + file_name
            cost_files.append(file_path + '.csv')
        (time, value) = read_parallel_op_data_time_value(cost_files, 1)
        data[op_name + "_cost_ts"] = time
        data[op_name + "_cost_value"] = value.tolist()
    except:
        print('Could not compute cost stats for operator ' + op_name)
        print('Error:' + sys.exc_info()[0])

    # LATENCY
    try:
        latency_files = []
        if os.path.exists(prefix + op_name + '.0.latency.csv'):
            for i in range(0, instances):
                file_name = op_name + '.' + str(i) + '.latency'
                file_path = prefix + file_name
                latency_files.append(file_path + '.csv')
            (time, value) = read_parallel_op_data_time_value(latency_files, 1)
            data[op_name + "_latency_ts"] = time
            data[op_name + "_latency_value"] = value.tolist()
    except:
        print('Could not compute latency stats for operator ' + op_name)
        print('Error:' + sys.exc_info()[0])

    return data


def read_topology_parallel_op_data(prefix, op_names, instances):
    global_data = dict()
    for i in range(0, len(op_names)):
        this_data = read_parallel_op_statistics(prefix, op_names[i], instances[i])
        global_data.update(this_data)
    return global_data


def read_topology_parallel_op_data_and_store_json(prefix, op_names, instances, output_json):
    global_data = read_topology_parallel_op_data(prefix, op_names, instances)
    json.dump(global_data, open(output_json, 'w'))
    return
