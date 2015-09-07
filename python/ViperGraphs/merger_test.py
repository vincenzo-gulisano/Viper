__author__ = 'vinmas'
import create_op_rate_graph as corg
import statistics as stat
from scipy import stats as scipystat
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

out_folder = '/Users/vinmas/repositories/viper_experiments/merger_test/'

# THIS PART TO COMPUTE THE UPPER BOUND

base_folder = '/Users/vinmas/repositories/viper_experiments/merger_test/results_maroon/'
id = 'mergertest'
generate_individual_files = False
start_ts = 60
end_ts = 240

f1 = plt.figure()
parallelism_instances = range(1, 21)
this_parallelism_throughput = []
this_parallelism_latency = []
for parallelism in parallelism_instances:
    this_repetition_throughput = []
    this_repetition_latency = []
    for repetition in [0]:
        spout_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_spout'
        op_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_merger'
        sink_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_sink'
        data = corg.manage_topology_parallel_op_graphs(base_folder,
                                                       [spout_id, op_id, sink_id],
                                                       [parallelism, 1, 1],
                                                       generate_individual_files, start_ts, end_ts)
        this_repetition_throughput.append(scipystat.trim_mean(data[spout_id + "_rate"][1][start_ts:end_ts], 0.05))
        this_repetition_latency.append(scipystat.trim_mean(data[sink_id + "_latency"][1][start_ts:end_ts], 0.05))
    this_parallelism_throughput.append(stat.mean(this_repetition_throughput))
    this_parallelism_latency.append(stat.mean(this_repetition_latency))

plt.subplot(211)
plt.plot(parallelism_instances, this_parallelism_throughput)
plt.subplot(212)
plt.plot(parallelism_instances, this_parallelism_latency)

print(str(this_parallelism_throughput))

ax = plt.subplot(211)
plt.xlabel('Injectors')
plt.ylabel('Throughput (t/s)')
xmin, xmax = ax.get_xlim()
plt.xlim([0, xmax])
plt.grid(True)

ax = plt.subplot(212)
plt.xlabel('Injectors')
plt.ylabel('Latency (ms)')
plt.xlim([0, xmax])
ymin, ymax = ax.get_ylim()
plt.ylim([0, ymax])
plt.grid(True)

pp = PdfPages(out_folder + 'result.pdf')
pp.savefig(f1)
pp.close()
plt.close()

# THIS PART TO MEASURE THE ACTUAL SCALABILITY
id = 'mergertestsn'
real_scalability_throughput = []
real_scalability_latency = []
temp_parallelism_instances = range(1, 11)
for parallelism in temp_parallelism_instances:
    this_repetition_throughput = []
    this_repetition_latency = []
    for repetition in [0]:
        spout_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_spout'
        op_merger_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_op_merger'
        op_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_op'
        sink_merger_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_sink_merger'
        sink_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_sink'
        data = corg.manage_topology_parallel_op_graphs(base_folder,
                                                       [spout_id, op_merger_id, op_id, sink_merger_id, sink_id],
                                                       [parallelism, parallelism, parallelism, parallelism, parallelism],
                                                       generate_individual_files, start_ts, end_ts)
        this_repetition_throughput.append(scipystat.trim_mean(data[spout_id + "_rate"][1][start_ts:end_ts], 0.05))
        this_repetition_latency.append(scipystat.trim_mean(data[sink_id + "_latency"][1][start_ts:end_ts], 0.05))
    real_scalability_throughput.append(stat.mean(this_repetition_throughput))
    real_scalability_latency.append(stat.mean(this_repetition_latency))


# THIS PART TO MEASURE THE ACTUAL SCALABILITY
id = 'mergertestsn05'
real_scalability05_throughput = []
real_scalability05_latency = []
temp_parallelism_instances05 = range(1, 9)
for parallelism in temp_parallelism_instances05:
    this_repetition_throughput = []
    this_repetition_latency = []
    for repetition in [0]:
        spout_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_spout'
        op_merger_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_op_merger'
        op_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_op'
        sink_merger_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_sink_merger'
        sink_id = str(repetition) + '_' + str(parallelism) + '_' + id + '_sink'
        data = corg.manage_topology_parallel_op_graphs(base_folder,
                                                       [spout_id, op_merger_id, op_id, sink_merger_id, sink_id],
                                                       [parallelism, parallelism, parallelism, parallelism, parallelism],
                                                       generate_individual_files, start_ts, end_ts)
        this_repetition_throughput.append(scipystat.trim_mean(data[spout_id + "_rate"][1][start_ts:end_ts], 0.05))
        this_repetition_latency.append(scipystat.trim_mean(data[sink_id + "_latency"][1][start_ts:end_ts], 0.05))
    real_scalability05_throughput.append(stat.mean(this_repetition_throughput))
    real_scalability05_latency.append(stat.mean(this_repetition_latency))


# FINAL PLOT

scaling = this_parallelism_throughput / this_parallelism_throughput[0]
scalability_linear = parallelism_instances * this_parallelism_throughput[0]
scalability_upperbound = parallelism_instances * this_parallelism_throughput[0] * scaling

f2 = plt.figure()
ax = plt.subplot(111)
plt.plot(parallelism_instances, scalability_linear, 'k', label='linear')
plt.plot(parallelism_instances, scalability_upperbound, 'b', label='upper bound')
plt.plot(temp_parallelism_instances, real_scalability_throughput, 'r', label='measured')
plt.plot(temp_parallelism_instances05, real_scalability05_throughput, 'g', label='measured05')
plt.xlabel('Parallelism')
plt.ylabel('Throughput (t/s)')
plt.xlim([0, xmax])
ymin, ymax = ax.get_ylim()
plt.ylim([0, ymax])
plt.grid(True)
plt.legend(loc='upper left')
pp = PdfPages(out_folder + 'scalability.pdf')
pp.savefig(f2)
pp.close()
plt.close()
