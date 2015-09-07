__author__ = 'vinmas'
import create_op_rate_graph as corg
import statistics as stat
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


out_folder = '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/stateless/'

# Dummy 2
base_folder = '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/stateless/dummy2ins/'
id = 'dummy'
generate_individual_files = False
start_ts = 60
end_ts = 240
throughput_dummy2 = []
latency_dummy2 = []
# repetitions
for p in [1, 3, 5, 7]:
    this_setup_throughput = []
    this_setup_latency = []
    for i in [0, 1, 2]:
        spout_id = str(i) + id + str(p) + '_spout'
        op_id = str(i) + id + str(p) + '_convert'
        sink_id = str(i) + id + str(p) + '_sink'
        data = corg.manage_topology_parallel_op_graphs(base_folder, [spout_id, op_id, sink_id], [2, p, p],
                                                       generate_individual_files, start_ts, end_ts)
        this_setup_throughput.append(stat.mean(data[spout_id + "_rate"][1][start_ts:end_ts]))
        this_setup_latency.append(stat.mean(data[sink_id + "_latency"][1][start_ts:end_ts]))
    throughput_dummy2.append(stat.mean(this_setup_throughput))
    latency_dummy2.append(stat.mean(this_setup_latency))

# Shared Nothing 2
base_folder = '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/stateless/sn2ins/'
id = 'sn'
generate_individual_files = False
start_ts = 60
end_ts = 660
throughput_sn2 = []
latency_sn2 = []
# repetitions
for p in [1, 3, 5]:
    this_setup_throughput = []
    this_setup_latency = []
    for i in [2]:
        spout_id = str(i) + id + str(p) + '_spout'
        op_merger_id = str(i) + id + str(p) + '_convert_merger'
        op_id = str(i) + id + str(p) + '_convert'
        sink_merger_id = str(i) + id + str(p) + '_sink_merger'
        sink_id = str(i) + id + str(p) + '_sink'
        data = corg.manage_topology_parallel_op_graphs(base_folder,
                                                       [spout_id, op_merger_id, op_id, sink_merger_id, sink_id],
                                                       [2, p, p, p, p],
                                                       generate_individual_files, start_ts, end_ts)
        this_setup_throughput.append(stat.mean(data[spout_id + "_rate"][1][start_ts:end_ts]))
        this_setup_latency.append(stat.mean(data[sink_id + "_latency"][1][start_ts:end_ts]))
    throughput_sn2.append(stat.mean(this_setup_throughput))
    latency_sn2.append(stat.mean(this_setup_latency))

# Dummy 4
base_folder = '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/stateless/dummy4ins/'
id = 'dummy'
generate_individual_files = False
start_ts = 60
end_ts = 240
throughput_dummy4 = []
latency_dummy4 = []
# repetitions
for p in [1, 3, 5, 7]:
    this_setup_throughput = []
    this_setup_latency = []
    for i in [0, 1, 2]:
        spout_id = str(i) + id + str(p) + '_spout'
        op_id = str(i) + id + str(p) + '_convert'
        sink_id = str(i) + id + str(p) + '_sink'
        data = corg.manage_topology_parallel_op_graphs(base_folder, [spout_id, op_id, sink_id], [4, p, p],
                                                       generate_individual_files, start_ts, end_ts)
        this_setup_throughput.append(stat.mean(data[spout_id + "_rate"][1][start_ts:end_ts]))
        this_setup_latency.append(stat.mean(data[sink_id + "_latency"][1][start_ts:end_ts]))
    throughput_dummy4.append(stat.mean(this_setup_throughput))
    latency_dummy4.append(stat.mean(this_setup_latency))

f1 = plt.figure(1)
f2 = plt.figure(2)

plt.figure(1)
plt.plot([1, 3, 5, 7], throughput_dummy2, 'b', label='dummy2')
plt.plot([1, 3, 5], throughput_sn2, 'r', label='sn2')
plt.plot([1, 3, 5, 7], throughput_dummy4, 'k', label='dummy4')
plt.xlabel('Tasks')
plt.ylabel('Throughput (t/s)')
plt.grid(True)
plt.legend(loc='upper right')

plt.figure(2)
plt.plot([1, 3, 5, 7], latency_dummy2, 'b', label='dummy2')
plt.plot([1, 3, 5], throughput_sn2, 'r', label='sn2')
plt.plot([1, 3, 5, 7], latency_dummy4, 'k', label='dummy4')
plt.xlabel('Tasks')
plt.ylabel('Latency (ms)')
plt.grid(True)
plt.legend(loc='upper right')

pp = PdfPages(out_folder + 'throughput.pdf')
pp.savefig(f1)
pp.close()
plt.close()

pp = PdfPages(out_folder + 'latency.pdf')
pp.savefig(f2)
pp.close()
plt.close()

# throughput_2 = []
# data = corg.manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy2ins/',
#     ['dummy1_spout', 'dummy1_convert', 'dummy1_sink'], [2, 1, 1], start_ts, end_ts)
# throughput_2.append(stat.mean(data["dummy1_spout_rate"][1][start_ts:end_ts]))
#
# data = corg.manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy2ins/',
#     ['dummy3_spout', 'dummy3_convert', 'dummy3_sink'], [2, 3, 1], start_ts, end_ts)
# throughput_2.append(stat.mean(data["dummy3_spout_rate"][1][start_ts:end_ts]))
#
# data = corg.manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy2ins/',
#     ['dummy5_spout', 'dummy5_convert', 'dummy5_sink'], [2, 5, 1], start_ts, end_ts)
# throughput_2.append(stat.mean(data["dummy5_spout_rate"][1][start_ts:end_ts]))
#
# data = corg.manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy2ins/',
#     ['dummy7_spout', 'dummy7_convert', 'dummy7_sink'], [2, 7, 1], start_ts, end_ts)
# throughput_2.append(stat.mean(data["dummy7_spout_rate"][1][start_ts:end_ts]))
#
# throughput_4 = []
# data = corg.manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy4ins/',
#     ['dummy1_spout', 'dummy1_convert', 'dummy1_sink'], [4, 1, 1], start_ts, end_ts)
# throughput_4.append(stat.mean(data["dummy1_spout_rate"][1][start_ts:end_ts]))
#
# data = corg.manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy4ins/',
#     ['dummy3_spout', 'dummy3_convert', 'dummy3_sink'], [4, 3, 1], start_ts, end_ts)
# throughput_4.append(stat.mean(data["dummy3_spout_rate"][1][start_ts:end_ts]))
#
# data = corg.manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy4ins/',
#     ['dummy5_spout', 'dummy5_convert', 'dummy5_sink'], [4, 5, 1], start_ts, end_ts)
# throughput_4.append(stat.mean(data["dummy5_spout_rate"][1][start_ts:end_ts]))
#
# data = corg.manage_topology_parallel_op_graphs(
#     '/Users/vinmas/repositories/viper_experiments/debs2015/results_maroon/large_dataset/dummy4ins/',
#     ['dummy7_spout', 'dummy7_convert', 'dummy7_sink'], [4, 7, 1], start_ts, end_ts)
# throughput_4.append(stat.mean(data["dummy7_spout_rate"][1][start_ts:end_ts]))
