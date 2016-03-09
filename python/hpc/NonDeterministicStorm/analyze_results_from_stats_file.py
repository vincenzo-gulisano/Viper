import json
from NonDeterministicStorm.create_single_exp_graphs import create_single_exp_graphs
from NonDeterministicStorm.create_single_exp_graphs import create_graph_multiple_time_value
from os import listdir
from os.path import isfile, join

stats_data = dict()
stats_data.update(json.load(open('/Users/vinmas/repositories/viper_experiments/experiments99/stats_data.json', 'r')))
stats_data.update(json.load(open('/Users/vinmas/repositories/viper_experiments/experiments1215/stats_data.json', 'r')))

max_throughput_keys = []
max_throughput = dict()
max_throughput_latency = dict()
max_throughput_consumption = dict()
max_throughput_selectivity = dict()

for load in [1.0, 0.1, 0]:

    keys = []

    threads = dict()
    throughput_avg = dict()
    latency_avg = dict()
    consumption_avg = dict()

    for system in ['storm', 'viper']:

        max_throughput_id = system + 'L' + str(load);

        max_throughput_keys.append(max_throughput_id)
        max_throughput[max_throughput_id] = []
        max_throughput_latency[max_throughput_id] = []
        max_throughput_consumption[max_throughput_id] = []
        max_throughput_selectivity[max_throughput_id] = []

        for selectivity in [1.0, 0.1, 0.01]:

            graph_id = system + '_S_' + str(selectivity)

            keys.append(graph_id)
            threads[graph_id] = []
            throughput_avg[graph_id] = []
            latency_avg[graph_id] = []

            consumption_avg[graph_id] = []

            for thread in range(0, 11):
                for repetition in range(0, 1):
                    id = system + '_load_' + str(load) + '_selectivity_' + str(selectivity) + '_thread_' + str(
                        thread) + '_repetition_' + str(repetition)
                    threads[graph_id].append(
                        stats_data[id + '_spout_parallelism'] + stats_data[id + '_op_parallelism'] + stats_data[
                            id + '_sink_parallelism'])
                    throughput_avg[graph_id].append(stats_data[id + '_throughput'])
                    latency_avg[graph_id].append(stats_data[id + '_latency'])
                    consumption_avg[graph_id].append(stats_data[id + '_consumption'])

            max_throughput[max_throughput_id].append(max(throughput_avg[graph_id]))
            max_throughput_latency[max_throughput_id].append(
                latency_avg[graph_id][throughput_avg[graph_id].index(max(throughput_avg[graph_id]))])
            max_throughput_consumption[max_throughput_id].append(
                consumption_avg[graph_id][throughput_avg[graph_id].index(max(throughput_avg[graph_id]))])
            max_throughput_selectivity[max_throughput_id].append(selectivity)

    # Other graphs
    create_graph_multiple_time_value(threads, throughput_avg, keys, 'Throughput (Load: ' + str(load) + ')', 'Threads',
                                     'Throughput (t/s)',
                                     '/Users/vinmas/repositories/viper_experiments/stats_dir/L' + str(
                                         load) + 'throughput.pdf')
    create_graph_multiple_time_value(threads, latency_avg, keys, 'Latency (Load: ' + str(load) + ')', 'Threads',
                                     'Latency (ms)',
                                     '/Users/vinmas/repositories/viper_experiments/stats_dir/L' + str(
                                         load) + 'latency.pdf')
    create_graph_multiple_time_value(threads, consumption_avg, keys, 'Consumption (Load: ' + str(load) + ')', 'Threads',
                                     'Consumption (W/t)',
                                     '/Users/vinmas/repositories/viper_experiments/stats_dir/L' + str(
                                         load) + 'consumption.pdf')

# Max throughput graphs
create_graph_multiple_time_value(max_throughput_selectivity, max_throughput, max_throughput_keys, 'Max Throughput', 'Selectivity',
                                 'Throughput (t/s)',
                                 '/Users/vinmas/repositories/viper_experiments/stats_dir/max_throughput.pdf')
create_graph_multiple_time_value(max_throughput_selectivity, max_throughput_latency, max_throughput_keys, 'Latency (at max throughput)', 'Selectivity',
                                 'Latency (ms)',
                                 '/Users/vinmas/repositories/viper_experiments/stats_dir/max_latency.pdf')
create_graph_multiple_time_value(max_throughput_selectivity, max_throughput_consumption, max_throughput_keys, 'Consumption (at max throughput)', 'Selectivity',
                                 'Consumption (W/t)',
                                 '/Users/vinmas/repositories/viper_experiments/stats_dir/max_consumption.pdf')
