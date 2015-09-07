__author__ = 'vinmas'
import statistics as stat

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

import create_op_rate_graph as corg

out_folder = '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/stateless/'

# Dummy 2
base_folder = '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/stateless/sn4ins/'
id = 'sn'
generate_individual_files = False
start_ts = 60
end_ts = 240
throughput = []
latency = []

f1 = plt.figure()

rates = range(10000, 210000, 20000)
colors = ['b', 'r', 'k', 'g']
spouts = 4
index = 0
for parallelism in [1, 3, 5, 7]:
    this_rate_throughput = []
    this_rate_latency = []
    for rate in rates:
        this_repetition_throughput = []
        this_repetition_latency = []
        for repetition in [0]:
            spout_id = str(repetition) + '_' + str(parallelism) + '_' + str(rate) + '_' + id + '_spout'
            op_merger_id = str(repetition) + '_' + str(parallelism) + '_' + str(rate) + '_' + id + '_convert_merger'
            op_id = str(repetition) + '_' + str(parallelism) + '_' + str(rate) + '_' + id + '_convert'
            sink_merger_id = str(repetition) + '_' + str(parallelism) + '_' + str(rate) + '_' + id + '_sink_merger'
            sink_id = str(repetition) + '_' + str(parallelism) + '_' + str(rate) + '_' + id + '_sink'
            data = corg.manage_topology_parallel_op_graphs(base_folder,
                                                           [spout_id, op_merger_id, op_id, sink_merger_id, sink_id],
                                                           [spouts, parallelism, parallelism, parallelism, parallelism],
                                                           generate_individual_files, start_ts, end_ts)
            this_repetition_throughput.append(stat.mean(data[spout_id + "_rate"][1][start_ts:end_ts]))
            this_repetition_latency.append(stat.mean(data[sink_id + "_latency"][1][start_ts:end_ts]))
        this_rate_throughput.append(stat.mean(this_repetition_throughput))
        this_rate_latency.append(stat.mean(this_repetition_latency))

    plt.subplot(211)
    plt.plot(rates, this_rate_throughput, colors[index], label='p' + str(parallelism))
    plt.subplot(212)
    plt.plot(rates, this_rate_latency, colors[index], label='p' + str(parallelism))
    index += 1

# # This should be in loop before
# rates = [50000, 60000, 70000, 80000, 90000, 100000, 110000, 120000, 130000, 140000, 150000, 160000, 170000, 180000]
# for parallelism in [5]:
#     this_rate_throughput = []
#     this_rate_latency = []
#     for rate in rates:
#         this_repetition_throughput = [];
#         this_repetition_latency = [];
#         for repetition in [0]:
#             spout_id = str(repetition) + '_' + str(parallelism) + '_' + str(rate) + '_' + id + '_spout'
#             op_merger_id = str(repetition) + '_' + str(parallelism) + '_' + str(rate) + '_' + id + '_convert_merger'
#             op_id = str(repetition) + '_' + str(parallelism) + '_' + str(rate) + '_' + id + '_convert'
#             sink_merger_id = str(repetition) + '_' + str(parallelism) + '_' + str(rate) + '_' + id + '_sink_merger'
#             sink_id = str(repetition) + '_' + str(parallelism) + '_' + str(rate) + '_' + id + '_sink'
#             data = corg.manage_topology_parallel_op_graphs(base_folder,
#                                                            [spout_id, op_merger_id, op_id, sink_merger_id, sink_id],
#                                                            [2, parallelism, parallelism, parallelism, parallelism],
#                                                            generate_individual_files, start_ts, end_ts)
#             this_repetition_throughput.append(stat.mean(data[spout_id + "_rate"][1][start_ts:end_ts]))
#             this_repetition_latency.append(stat.mean(data[sink_id + "_latency"][1][start_ts:end_ts]))
#         this_rate_throughput.append(stat.mean(this_repetition_throughput))
#         this_rate_latency.append(stat.mean(this_repetition_latency))
#
#     plt.subplot(211)
#     plt.plot(rates, this_rate_throughput, 'r', label='p' + str(parallelism))
#     plt.subplot(212)
#     plt.plot(rates, this_rate_latency, 'r', label='p' + str(parallelism))

ax = plt.subplot(211)
plt.xlabel('Input Rate (t/s)')
plt.ylabel('Throughput (t/s)')
xmin, xmax = ax.get_xlim()
plt.xlim([0, xmax])
plt.ylim([0, xmax])
plt.grid(True)
plt.legend(loc='upper left')

ax = plt.subplot(212)
plt.xlabel('Input Rate (t/s)')
plt.ylabel('Latency (ms)')
plt.xlim([0, xmax])
ymin, ymax = ax.get_ylim()
plt.ylim([0, ymax])
plt.grid(True)
plt.legend(loc='upper left')

pp = PdfPages(out_folder + '4insresult.pdf')
pp.savefig(f1)
pp.close()
plt.close()

#
#
# # Shared Nothing 2
# base_folder = '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/stateless/sn2ins/'
# id = 'sn'
# generate_individual_files = False
# start_ts = 60
# end_ts = 660
# throughput_sn2 = []
# latency_sn2 = []
# # repetitions
# for p in [1, 3, 5]:
#     this_setup_throughput = []
#     this_setup_latency = []
#     for i in [2]:
#         spout_id = str(i) + id + str(p) + '_spout'
#         op_merger_id = str(i) + id + str(p) + '_convert_merger'
#         op_id = str(i) + id + str(p) + '_convert'
#         sink_merger_id = str(i) + id + str(p) + '_sink_merger'
#         sink_id = str(i) + id + str(p) + '_sink'
#         data = corg.manage_topology_parallel_op_graphs(base_folder,
#                                                        [spout_id, op_merger_id, op_id, sink_merger_id, sink_id],
#                                                        [2, p, p, p, p],
#                                                        generate_individual_files, start_ts, end_ts)
#         this_setup_throughput.append(stat.mean(data[spout_id + "_rate"][1][start_ts:end_ts]))
#         this_setup_latency.append(stat.mean(data[sink_id + "_latency"][1][start_ts:end_ts]))
#     throughput_sn2.append(stat.mean(this_setup_throughput))
#     latency_sn2.append(stat.mean(this_setup_latency))
#
# # Dummy 4
# base_folder = '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/stateless/dummy4ins/'
# id = 'dummy'
# generate_individual_files = False
# start_ts = 60
# end_ts = 240
# throughput_dummy4 = []
# latency_dummy4 = []
# # repetitions
# for p in [1, 3, 5, 7]:
#     this_setup_throughput = []
#     this_setup_latency = []
#     for i in [0, 1, 2]:
#         spout_id = str(i) + id + str(p) + '_spout'
#         op_id = str(i) + id + str(p) + '_convert'
#         sink_id = str(i) + id + str(p) + '_sink'
#         data = corg.manage_topology_parallel_op_graphs(base_folder, [spout_id, op_id, sink_id], [4, p, p],
#                                                        generate_individual_files, start_ts, end_ts)
#         this_setup_throughput.append(stat.mean(data[spout_id + "_rate"][1][start_ts:end_ts]))
#         this_setup_latency.append(stat.mean(data[sink_id + "_latency"][1][start_ts:end_ts]))
#     throughput_dummy4.append(stat.mean(this_setup_throughput))
#     latency_dummy4.append(stat.mean(this_setup_latency))
#
# f1 = plt.figure(1)
# f2 = plt.figure(2)
#
# plt.figure(1)
# plt.plot([1, 3, 5, 7], throughput_dummy2, 'b', label='dummy2')
# plt.plot([1, 3, 5], throughput_sn2, 'r', label='sn2')
# plt.plot([1, 3, 5, 7], throughput_dummy4, 'k', label='dummy4')
# plt.xlabel('Tasks')
# plt.ylabel('Throughput (t/s)')
# plt.grid(True)
# plt.legend(loc='upper right')
#
# plt.figure(2)
# plt.plot([1, 3, 5, 7], latency_dummy2, 'b', label='dummy2')
# plt.plot([1, 3, 5], throughput_sn2, 'r', label='sn2')
# plt.plot([1, 3, 5, 7], latency_dummy4, 'k', label='dummy4')
# plt.xlabel('Tasks')
# plt.ylabel('Latency (ms)')
# plt.grid(True)
# plt.legend(loc='upper right')
#
# pp = PdfPages(out_folder + 'throughput.pdf')
# pp.savefig(f1)
# pp.close()
# plt.close()
#
# pp = PdfPages(out_folder + 'latency.pdf')
# pp.savefig(f2)
# pp.close()
# plt.close()
#
# # throughput_2 = []
# # data = corg.manage_topology_parallel_op_graphs(
# #     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy2ins/',
# #     ['dummy1_spout', 'dummy1_convert', 'dummy1_sink'], [2, 1, 1], start_ts, end_ts)
# # throughput_2.append(stat.mean(data["dummy1_spout_rate"][1][start_ts:end_ts]))
# #
# # data = corg.manage_topology_parallel_op_graphs(
# #     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy2ins/',
# #     ['dummy3_spout', 'dummy3_convert', 'dummy3_sink'], [2, 3, 1], start_ts, end_ts)
# # throughput_2.append(stat.mean(data["dummy3_spout_rate"][1][start_ts:end_ts]))
# #
# # data = corg.manage_topology_parallel_op_graphs(
# #     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy2ins/',
# #     ['dummy5_spout', 'dummy5_convert', 'dummy5_sink'], [2, 5, 1], start_ts, end_ts)
# # throughput_2.append(stat.mean(data["dummy5_spout_rate"][1][start_ts:end_ts]))
# #
# # data = corg.manage_topology_parallel_op_graphs(
# #     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy2ins/',
# #     ['dummy7_spout', 'dummy7_convert', 'dummy7_sink'], [2, 7, 1], start_ts, end_ts)
# # throughput_2.append(stat.mean(data["dummy7_spout_rate"][1][start_ts:end_ts]))
# #
# # throughput_4 = []
# # data = corg.manage_topology_parallel_op_graphs(
# #     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy4ins/',
# #     ['dummy1_spout', 'dummy1_convert', 'dummy1_sink'], [4, 1, 1], start_ts, end_ts)
# # throughput_4.append(stat.mean(data["dummy1_spout_rate"][1][start_ts:end_ts]))
# #
# # data = corg.manage_topology_parallel_op_graphs(
# #     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy4ins/',
# #     ['dummy3_spout', 'dummy3_convert', 'dummy3_sink'], [4, 3, 1], start_ts, end_ts)
# # throughput_4.append(stat.mean(data["dummy3_spout_rate"][1][start_ts:end_ts]))
# #
# # data = corg.manage_topology_parallel_op_graphs(
# #     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy4ins/',
# #     ['dummy5_spout', 'dummy5_convert', 'dummy5_sink'], [4, 5, 1], start_ts, end_ts)
# # throughput_4.append(stat.mean(data["dummy5_spout_rate"][1][start_ts:end_ts]))
# #
# # data = corg.manage_topology_parallel_op_graphs(
# #     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy4ins/',
# #     ['dummy7_spout', 'dummy7_convert', 'dummy7_sink'], [4, 7, 1], start_ts, end_ts)
# # throughput_4.append(stat.mean(data["dummy7_spout_rate"][1][start_ts:end_ts]))
