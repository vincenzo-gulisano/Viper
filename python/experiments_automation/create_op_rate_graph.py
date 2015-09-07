import datetime as dt
import os.path

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.dates as md
from matplotlib import rcParams

import interpolation


def create_op_graph_time_value(file, outFile, title, ylabel):
    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(outFile)

    time, v = np.genfromtxt(file, dtype=int, delimiter=',').transpose()
    time /= 1000
    dates = [dt.datetime.fromtimestamp(ts) for ts in time]
    datenums = md.date2num(dates)
    f = plt.figure()

    plt.xticks(rotation=90)
    ax = plt.gca()
    xfmt = md.DateFormatter('%H:%M:%S')
    ax.xaxis.set_major_formatter(xfmt)

    plt.plot(datenums, v)

    plt.xlabel('Time (s)')
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)
    plt.close()

    pp.savefig(f)
    pp.close()

    return


def create_parallel_op_graph_time_value(files, outFile, title, ylabel, interpolation_type, warm_up_end=None,
                                        measurement_end=None):
    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(outFile)

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

    dates = [dt.datetime.fromtimestamp(ts) for ts in time]
    datenums = md.date2num(dates)
    f = plt.figure()

    plt.xticks(rotation=90)
    ax = plt.gca()
    xfmt = md.DateFormatter('%H:%M:%S')
    ax.xaxis.set_major_formatter(xfmt)

    plt.plot(datenums, value)
    if warm_up_end:
        plt.plot([datenums[warm_up_end], datenums[warm_up_end]], ax.get_ylim(), 'r')
    if measurement_end:
        plt.plot([datenums[measurement_end], datenums[measurement_end]], ax.get_ylim(), 'r')

    plt.xlabel('Time (s)')
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)

    pp.savefig(f)
    pp.close()
    plt.close()

    return time, value


def manage_parallel_op_graphs(dir, op_name, instances, generate_individual_graphs, warm_up_end=None,
                              measurement_end=None):
    data = {}
    # RATE

    rate_files = [];
    for i in range(0, instances):
        file_name = op_name + '.' + str(i) + '.rate'
        file_path = dir + file_name
        rate_files.append(file_path + '.csv')
        if generate_individual_graphs:
            create_op_graph_time_value(file_path + '.csv',
                                       file_path + '.pdf',
                                       file_name, 'Rate (t/s)')
    (time, value) = create_parallel_op_graph_time_value(rate_files, dir + op_name + '.rate.pdf', op_name + '.rate',
                                                        'Rate (t/s)', 0, warm_up_end, measurement_end)
    data[op_name + "_rate"] = (time, value)

    # INVOCATIONS
    invocations_files = [];
    for i in range(0, instances):
        file_name = op_name + '.' + str(i) + '.invocations'
        file_path = dir + file_name
        invocations_files.append(file_path + '.csv')
        if generate_individual_graphs:
            create_op_graph_time_value(file_path + '.csv',
                                       file_path + '.pdf',
                                       file_name, 'Invocations')
    (time, value) = create_parallel_op_graph_time_value(invocations_files, dir + op_name + '.invocations.pdf', op_name + '.invocations',
                                                        'Invocations', 0, warm_up_end, measurement_end)
    data[op_name + "_invocations"] = (time, value)

    # COST
    cost_files = [];
    for i in range(0, instances):
        file_name = op_name + '.' + str(i) + '.cost'
        file_path = dir + file_name
        cost_files.append(file_path + '.csv')
        if generate_individual_graphs:
            create_op_graph_time_value(file_path + '.csv',
                                       file_path + '.pdf',
                                       file_name, 'Cost (ms)')
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
            if generate_individual_graphs:
                create_op_graph_time_value(file_path + '.csv',
                                           file_path + '.pdf',
                                           file_name, 'Latency (ms)')
        (time, value) = create_parallel_op_graph_time_value(latency_files, dir + op_name + '.latency.pdf',
                                                            op_name + '.latency',
                                                            'Latency (ms)', 1, warm_up_end, measurement_end)
        data[op_name + "_latency"] = (time, value)

    return data


def manage_topology_parallel_op_graphs(dir, op_names, instances, generate_individual_graphs, warm_up_end=None,
                                       measurement_end=None):
    global_data = {}
    for i in range(0, len(op_names)):
        this_data = manage_parallel_op_graphs(dir, op_names[i], instances[i], generate_individual_graphs, warm_up_end,
                                              measurement_end)
        global_data.update(this_data)
    return global_data

#
# #### AFTER THIS POINT IS SPECIFIC TO THE USE CASE AND SHOULD BE MOVED SOMEWHERE ELSE
#
# # # Should plot also the interval over which we compute the average!!!
# start_ts = 300
# end_ts = 900
# #
# # ### SHARED MEMORY
# #
# throughput_sm = []
# latency_sm = []
# data = manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/2ins/',
#     ['sm1_spout', 'sm1_convert', 'sm1_sink'], [2, 1, 1])
# throughput_sm.append(stat.mean(data["sm1_spout_rate"][1][start_ts:end_ts]))
# latency_sm.append(stat.mean(data["sm1_sink_latency"][1][start_ts:end_ts]))
# #
# data = manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/2ins/',
#     ['sm3_spout', 'sm3_convert', 'sm3_sink'], [2, 1, 3])
# throughput_sm.append(stat.mean(data["sm3_spout_rate"][1][start_ts:end_ts]))
# latency_sm.append(stat.mean(data["sm3_sink_latency"][1][start_ts:end_ts]))
# #
# data = manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/2ins/',
#     ['sm5_spout', 'sm5_convert', 'sm5_sink'], [2, 1, 5])
# throughput_sm.append(stat.mean(data["sm5_spout_rate"][1][start_ts:end_ts]))
# latency_sm.append(stat.mean(data["sm5_sink_latency"][1][start_ts:end_ts]))
# #
# data = manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/2ins/',
#     ['sm7_spout', 'sm7_convert', 'sm7_sink'], [2, 1, 7])
# throughput_sm.append(stat.mean(data["sm7_spout_rate"][1][start_ts:end_ts]))
# latency_sm.append(stat.mean(data["sm7_sink_latency"][1][start_ts:end_ts]))
# #
# # ### SHARED NOTHING
# #
# # throughput_sn = []
# # latency_sn = []
# data = manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/2ins/',
#     ['sn1_spout', 'sn1_convert', 'sn1_sink'], [2, 1, 1])
# # throughput_sn.append(stat.mean(data["sn1_spout_rate"][1][start_ts:end_ts]))
# # latency_sn.append(stat.mean(data["sn1_sink_latency"][1][start_ts:end_ts]))
# #
# data = manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/2ins/',
#     ['sn3_spout', 'sn3_convert', 'sn3_sink'], [2, 3, 3])
# # throughput_sn.append(stat.mean(data["sn3_spout_rate"][1][start_ts:end_ts]))
# # latency_sn.append(stat.mean(data["sn3_sink_latency"][1][start_ts:end_ts]))
# #
# # data = manage_topology_parallel_op_graphs(
# #     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/2ins/',
# #     ['sn5_spout', 'sn5_convert', 'sn5_sink'], [2, 5, 1])
# # throughput_sn.append(stat.mean(data["sn5_spout_rate"][1][start_ts:end_ts]))
# # latency_sn.append(stat.mean(data["sn5_sink_latency"][1][start_ts:end_ts]))
# #
# # data = manage_topology_parallel_op_graphs(
# #     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/2ins/',
# #     ['sn7_spout', 'sn7_convert', 'sn7_sink'], [2, 7, 1])
# # throughput_sn.append(stat.mean(data["sn7_spout_rate"][1][start_ts:end_ts]))
# # latency_sn.append(stat.mean(data["sn7_sink_latency"][1][start_ts:end_ts]))
# #
# # data = manage_topology_parallel_op_graphs(
# #     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/2ins/',
# #     ['sn9_spout', 'sn9_convert', 'sn9_sink'], [2, 9, 1])
# # throughput_sn.append(stat.mean(data["sn9_spout_rate"][1][start_ts:end_ts]))
# # latency_sn.append(stat.mean(data["sn9_sink_latency"][1][start_ts:end_ts]))
# #
# f = plt.figure()
#
# plt.plot([2, 4, 6, 8], throughput_sm)
# # plt.plot([6, 10, 14, 18, 22], throughput_sn)
# #
# plt.xlabel('Number of working threads')
# plt.ylabel('Throughput (t/s)')
# plt.grid(True)
# plt.xlim([0, 22])
# plt.ylim([0, 130000])
#
# pp = PdfPages('/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/2ins/global_throughput.pdf')
# pp.savefig(f)
# pp.close()
# plt.close()
# #
# f = plt.figure()
#
# plt.plot([2, 4, 6, 8], latency_sm)
# # plt.plot([6, 10, 14, 18, 22], latency_sn)
#
# plt.xlabel('Number of working threads')
# plt.ylabel('Latency (ms)')
# plt.grid(True)
# plt.xlim([0, 22])
#
# pp = PdfPages('/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/2ins/global_latency.pdf')
# pp.savefig(f)
# pp.close()
# plt.close()
# #
# # start_ts = 300
# # end_ts = 900
# #
# throughput_sm = []
# data = manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/4ins/',
#     ['sm1_spout', 'sm1_convert', 'sm1_sink'], [4, 1, 1], start_ts, end_ts)
# throughput_sm.append(stat.mean(data["sm1_spout_rate"][1][start_ts:end_ts]))
#
# data = manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/4ins/',
#     ['sm3_spout', 'sm3_convert', 'sm3_sink'], [4, 1, 3], start_ts, end_ts)
# throughput_sm.append(stat.mean(data["sm3_spout_rate"][1][start_ts:end_ts]))
#
# data = manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/4ins/',
#     ['sm5_spout', 'sm5_convert', 'sm5_sink'], [4, 1, 5], start_ts, end_ts)
# throughput_sm.append(stat.mean(data["sm5_spout_rate"][1][start_ts:end_ts]))
#
# data = manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/4ins/',
#     ['sm7_spout', 'sm7_convert', 'sm7_sink'], [4, 1, 7], start_ts, end_ts)
# throughput_sm.append(stat.mean(data["sm7_spout_rate"][1][start_ts:end_ts]))
#
# f = plt.figure()
#
# plt.plot([2, 4, 6, 8], throughput_sm)
# # plt.plot([6, 10, 14, 18, 22], latency_sn)
#
# plt.xlabel('Number of working threads')
# plt.ylabel('Throughput (t/s)')
# plt.grid(True)
# plt.xlim([0, 22])
# plt.ylim([0, 200000])
#
# pp = PdfPages(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/4ins/global_throughput.pdf')
# pp.savefig(f)
# pp.close()
# plt.close()
