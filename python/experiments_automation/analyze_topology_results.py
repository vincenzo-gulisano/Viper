import read_topology_data as corg
import statistics as stat
from scipy import stats as scipystat


def analyze_topology_results(operators, instances, duration, repetitions, stats_folder, id_suffix):
    generate_individual_files = False

    start_ts = duration * 0.1
    end_ts = duration * 0.9

    reps_throughput = {}
    reps_latency = {}
    reps_cost = {}
    reps_invocations = {}
    for o in operators:
        reps_throughput[o] = []
        reps_latency[o] = []
        reps_cost[o] = []
        reps_invocations[o] = []

    for r in range(0, repetitions):

        # compute experiment id
        instances_list = []
        id = str(r) + '_'
        for o in operators:
            id += str(instances[o]) + '_'
            instances_list.append(instances[o])
        id += id_suffix + '_'

        # remove illegal characters
        id = id .replace('.','-')

        data = corg.read_topology_parallel_op_data(stats_folder,
                                                       [id + op for op in operators],
                                                       instances_list,
                                                       start_ts, end_ts)
        for o in operators:
            reps_throughput[o].append(scipystat.trim_mean(data[id + o + "_rate"][1][start_ts:end_ts], 0.05))
            if id + o + "_latency" in data:
                reps_latency[o].append(scipystat.trim_mean(data[id + o + "_latency"][1][start_ts:end_ts], 0.05))
            reps_invocations[o].append(scipystat.trim_mean(data[id + o + "_invocations"][1][start_ts:end_ts], 0.05))
            reps_cost[o].append(scipystat.trim_mean(data[id + o + "_cost"][1][start_ts:end_ts], 0.05))

    throughput = []
    latency = []
    cost = []
    print('')
    for o in operators:

        throughput.append(stat.mean(reps_throughput[o]))
        if len(reps_latency[o]) > 0:
            latency.append(stat.mean(reps_latency[o]))
        else:
            latency.append(0.0)
        cost.append(stat.mean(reps_cost[o]) * stat.mean(reps_invocations[o]) / instances[o] / pow(10, 9))

        print(o + ': T ' + str(throughput[-1]) + ' L ' + str(latency[-1])
              + ' C ' + str(cost[-1]))

    print('')
    return [throughput, latency, cost]
