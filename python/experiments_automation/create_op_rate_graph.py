import datetime as dt
import os.path

import numpy as np

import interpolation

def create_parallel_op_graph_time_value(files, interpolation_type):
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
        (time, value) = interpolation.interpolate_and_sum(data_ts, data_v)
    elif interpolation_type == 1:
        (time, value) = interpolation.interpolate_and_avg(data_ts, data_v)

    return time, value


def manage_parallel_op_graphs(dir, op_name, instances, warm_up_end=None,
                              measurement_end=None):
    data = {}
    # RATE

    rate_files = [];
    for i in range(0, instances):
        file_name = op_name + '.' + str(i) + '.rate'
        file_path = dir + file_name
        rate_files.append(file_path + '.csv')
    (time, value) = create_parallel_op_graph_time_value(rate_files, dir + op_name + '.rate.pdf', op_name + '.rate',
                                                        'Rate (t/s)', 0, warm_up_end, measurement_end)
    data[op_name + "_rate"] = (time, value)

    # INVOCATIONS
    invocations_files = [];
    for i in range(0, instances):
        file_name = op_name + '.' + str(i) + '.invocations'
        file_path = dir + file_name
        invocations_files.append(file_path + '.csv')
    (time, value) = create_parallel_op_graph_time_value(invocations_files, dir + op_name + '.invocations.pdf',
                                                        op_name + '.invocations',
                                                        'Invocations', 0, warm_up_end, measurement_end)
    data[op_name + "_invocations"] = (time, value)

    # COST
    cost_files = [];
    for i in range(0, instances):
        file_name = op_name + '.' + str(i) + '.cost'
        file_path = dir + file_name
        cost_files.append(file_path + '.csv')
    (time, value) = create_parallel_op_graph_time_value(cost_files, dir + op_name + '.cost.pdf', op_name + '.cost',
                                                        'Cost (ms)', 1, warm_up_end, measurement_end)
    data[op_name + "_cost"] = (time, value)

    # LATENCY
    latency_files = [];
    if os.path.exists(dir + op_name + '.0.latency.csv'):
        for i in range(0, instances):
            file_name = op_name + '.' + str(i) + '.latency'
            file_path = dir + file_name
            latency_files.append(file_path + '.csv')
        (time, value) = create_parallel_op_graph_time_value(latency_files, dir + op_name + '.latency.pdf',
                                                            op_name + '.latency',
                                                            'Latency (ms)', 1, warm_up_end, measurement_end)
        data[op_name + "_latency"] = (time, value)

    return data


def manage_topology_parallel_op_graphs(dir, op_names, instances, warm_up_end=None,
                                       measurement_end=None):
    global_data = {}
    for i in range(0, len(op_names)):
        this_data = manage_parallel_op_graphs(dir, op_names[i], instances[i], warm_up_end,
                                              measurement_end)
        global_data.update(this_data)
    return global_data
