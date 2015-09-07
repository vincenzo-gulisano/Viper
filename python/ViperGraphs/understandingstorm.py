__author__ = 'vinmas'
import statistics as stat

from scipy import stats as scipystat
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

import create_op_rate_graph as corg

out_folder = '/Users/vinmas/repositories/viper_experiments/understanding_storm/'

# THIS PART TO COMPUTE THE UPPER BOUND

base_folder = '/Users/vinmas/repositories/viper_experiments/understanding_storm/various_configurations/'
id = 'mtndiq'
generate_individual_files = False
start_ts = 60
end_ts = 240

f1 = plt.figure()
parallelism_instances = range(22, 18, -1)
this_parallelism_throughput = []
this_parallelism_latency = []
this_parallelism_spout_cost = []
this_parallelism_spout_invocations = []
this_parallelism_op_cost = []
this_parallelism_op_invocations = []
this_parallelism_sink_cost = []
this_parallelism_sink_invocations = []

injectors_array = range(1, 5)
operators_array = range(22, 18, -1)
sinks = 1
for i in range(len(injectors_array)):

    injectors = injectors_array[i]
    operators = operators_array[i]

    this_repetition_throughput = []
    this_repetition_latency = []
    this_spout_cost = []
    this_spout_invocations = []
    this_op_cost = []
    this_op_invocations = []
    this_sink_cost = []
    this_sink_invocations = []
    for repetition in [0]:
        spout_id = str(repetition) + '_' + str(injectors) + '_' + str(operators) + '_' + str(
            sinks) + '_' + id + '_spout'
        op_id = str(repetition) + '_' + str(injectors) + '_' + str(operators) + '_' + str(
            sinks) + '_' + id + '_op'
        sink_id = str(repetition) + '_' + str(injectors) + '_' + str(operators) + '_' + str(
            sinks) + '_' + id + '_sink'
        data = corg.manage_topology_parallel_op_graphs(base_folder,
                                                       [spout_id, op_id, sink_id],
                                                       [injectors, operators, sinks],
                                                       generate_individual_files, start_ts, end_ts)
        this_repetition_throughput.append(scipystat.trim_mean(data[spout_id + "_rate"][1][start_ts:end_ts], 0.05))
        this_repetition_latency.append(scipystat.trim_mean(data[sink_id + "_latency"][1][start_ts:end_ts], 0.05))
        this_spout_invocations.append(scipystat.trim_mean(data[spout_id + "_invocations"][1][start_ts:end_ts], 0.05))
        this_spout_cost.append(scipystat.trim_mean(data[spout_id + "_cost"][1][start_ts:end_ts], 0.05))
        this_op_invocations.append(scipystat.trim_mean(data[op_id + "_invocations"][1][start_ts:end_ts], 0.05))
        this_op_cost.append(scipystat.trim_mean(data[op_id + "_cost"][1][start_ts:end_ts], 0.05))
        this_sink_invocations.append(scipystat.trim_mean(data[sink_id + "_invocations"][1][start_ts:end_ts], 0.05))
        this_sink_cost.append(scipystat.trim_mean(data[sink_id + "_cost"][1][start_ts:end_ts], 0.05))
    this_parallelism_throughput.append(stat.mean(this_repetition_throughput))
    this_parallelism_latency.append(stat.mean(this_repetition_latency))
    this_parallelism_spout_cost.append(stat.mean(this_spout_cost))
    this_parallelism_spout_invocations.append(stat.mean(this_spout_invocations))
    this_parallelism_op_cost.append(stat.mean(this_op_cost))
    this_parallelism_op_invocations.append(stat.mean(this_op_invocations))
    this_parallelism_sink_cost.append(stat.mean(this_sink_cost))
    this_parallelism_sink_invocations.append(stat.mean(this_sink_invocations))

    print('\nConfiguration: ' + str(injectors) + '_' + str(operators) + '_' + str(sinks))
    print('Throughput: ' + str(this_parallelism_throughput[-1]))
    print('Latency: ' + str(this_parallelism_latency[-1]))
    print('Spout invocations: ' + str(this_parallelism_spout_invocations[-1]))
    print('Spout cost: ' + str(
        this_parallelism_spout_cost[-1] * this_parallelism_spout_invocations[-1] / injectors / pow(10, 9)))
    print('Op invocations: ' + str(this_parallelism_op_invocations[-1]))
    print(
        'Op cost: ' + str(this_parallelism_op_cost[-1] * this_parallelism_op_invocations[-1] / operators / pow(10, 9)))
    print('Sink invocations: ' + str(this_parallelism_sink_invocations[-1]))
    print('Sink cost: ' + str(
        this_parallelism_sink_cost[-1] * this_parallelism_sink_invocations[-1] / sinks / pow(10, 9)))

plt.subplot(311)
plt.plot(parallelism_instances, this_parallelism_throughput)
plt.subplot(312)
plt.plot(parallelism_instances, this_parallelism_latency)
plt.subplot(313)
plt.plot(parallelism_instances, this_parallelism_spout_cost, 'r', label='spout')
plt.plot(parallelism_instances, this_parallelism_op_cost, 'k', label='op')
plt.plot(parallelism_instances, this_parallelism_sink_cost, 'b', label='sink')

print(str(this_parallelism_throughput))

ax = plt.subplot(311)
plt.xlabel('Injectors')
plt.ylabel('Throughput (t/s)')
xmin, xmax = ax.get_xlim()
plt.xlim([0, xmax])
plt.grid(True)

ax = plt.subplot(312)
plt.xlabel('Injectors')
plt.ylabel('Latency (ms)')
plt.xlim([0, xmax])
ymin, ymax = ax.get_ylim()
plt.ylim([0, ymax])
plt.grid(True)

ax = plt.subplot(313)
plt.xlabel('Injectors')
plt.ylabel('Cost (ms)')
plt.xlim([0, xmax])
ymin, ymax = ax.get_ylim()
plt.ylim([0, 1000])
plt.grid(True)
plt.legend(loc='upper right')

pp = PdfPages(out_folder + 'result2.pdf')
pp.savefig(f1)
pp.close()
plt.close()
