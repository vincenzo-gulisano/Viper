import datetime as dt
import os.path
import numpy as np
import interpolation
import matplotlib

matplotlib.use('Agg')
from matplotlib import rcParams
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt


def create_graph_time_value(x, y, title, x_label, y_label, outFile):
    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(outFile)

    f = plt.figure()
    ax = plt.gca()

    plt.plot(x, y)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True)
    plt.close()

    pp.savefig(f)
    pp.close()

    return


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
        (time, value) = interpolation.interpolate_and_sum(data_ts, data_v)
    elif interpolation_type == 1:
        (time, value) = interpolation.interpolate_and_avg(data_ts, data_v)

    return time, value


def read_parallel_op_statistics(dir, op_name, instances, warm_up_end=None,
                                measurement_end=None):
    data = {}
    # RATE

    rate_files = [];
    for i in range(0, instances):
        file_name = op_name + '.' + str(i) + '.rate'
        file_path = dir + file_name
        rate_files.append(file_path + '.csv')
    (time, value) = read_parallel_op_data_time_value(rate_files, 0)
    data[op_name + "_rate"] = (time, value)
    create_graph_time_value(time, value, 'Rate', 'Time (secs)', 'Rate (t/s)', dir + op_name + '_rate.pdf')

    # INVOCATIONS
    invocations_files = [];
    for i in range(0, instances):
        file_name = op_name + '.' + str(i) + '.invocations'
        file_path = dir + file_name
        invocations_files.append(file_path + '.csv')
    (time, value) = read_parallel_op_data_time_value(invocations_files, 0)
    data[op_name + "_invocations"] = (time, value)
    create_graph_time_value(time, value, 'Invocations', 'Time (secs)', 'Invocations', dir + op_name + '_invocations.pdf')

    # COST
    cost_files = [];
    for i in range(0, instances):
        file_name = op_name + '.' + str(i) + '.cost'
        file_path = dir + file_name
        cost_files.append(file_path + '.csv')
    (time, value) = read_parallel_op_data_time_value(cost_files, 1)
    data[op_name + "_cost"] = (time, value)
    create_graph_time_value(time, value, 'Cost', 'Time (secs)', 'Cost', dir + op_name + '_cost.pdf')

    # LATENCY
    latency_files = [];
    if os.path.exists(dir + op_name + '.0.latency.csv'):
        for i in range(0, instances):
            file_name = op_name + '.' + str(i) + '.latency'
            file_path = dir + file_name
            latency_files.append(file_path + '.csv')
        (time, value) = read_parallel_op_data_time_value(latency_files, 1)
        data[op_name + "_latency"] = (time, value)
    create_graph_time_value(time, value, 'Latency', 'Time (secs)', 'Latency', dir + op_name + '_latency.pdf')

    return data


def read_topology_parallel_op_data(dir, op_names, instances, warm_up_end=None,
                                   measurement_end=None):
    global_data = {}
    for i in range(0, len(op_names)):
        this_data = read_parallel_op_statistics(dir, op_names[i], instances[i], warm_up_end,
                                                measurement_end)
        global_data.update(this_data)
    return global_data
