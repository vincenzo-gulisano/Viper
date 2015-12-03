import json
from NonDeterministicStorm.create_single_exp_graphs import create_single_exp_graphs
from NonDeterministicStorm.create_single_exp_graphs import create_graph_multiple_time_value
from os import listdir
from os.path import isfile, join

state_folder = '/Users/vinmas/repositories/viper_experiments/151203/'
results_base_folder = '/Users/vinmas/repositories/viper_experiments/151203'

threads = dict()
throughput_avg = dict()
latency_avg = dict()
consumption_avg = dict()

state = json.load(open(state_folder + 'state.json', 'r'))

exp_num = 1
for load in [1.0, 0.1, 0]:
    for selectivity in [1.0, 0.1, 0.01]:

        id = 'L' + str(load) + 'S' + str(selectivity)
        threads[id] = []
        throughput_avg[id] = []
        latency_avg[id] = []
        consumption_avg[id] = []

        for thread in range(0, 4):
            for repetition in range(0, 1):
                result_path = state['exp_' + str(exp_num) + '_results_folder']
                result_path = result_path.split('/')[-2]
                exp_id = state['exp_' + str(exp_num) + '_id']
                spout_parallelism = int(exp_id.split('_')[1])
                op_parallelism = int(exp_id.split('_')[2])
                sink_parallelism = int(exp_id.split('_')[3])
                results_folder = results_base_folder + '/' + result_path + '/'
                onlyfiles = [f for f in listdir(results_folder) if isfile(join(results_folder, f)) and 'RAPL' in f]
                print('Analyzing result folder ' + results_folder + ' (experiment ' + str(exp_num) + ')')
                [throughput, latency, consumption] = create_single_exp_graphs(state_folder, results_folder,
                                                                              onlyfiles[0],
                                                                              spout_parallelism, op_parallelism,
                                                                              sink_parallelism)

                threads[id].append(spout_parallelism + op_parallelism + sink_parallelism)
                throughput_avg[id].append(throughput)
                latency_avg[id].append(latency)
                consumption_avg[id].append(consumption)

                exp_num += 1

create_graph_multiple_time_value(threads, throughput_avg, 'Throughput', 'Threads', 'Throughput (t/s)',
                                 results_base_folder + '/throughput.pdf')
create_graph_multiple_time_value(threads, latency_avg, 'Latency', 'Threads', 'Latency (?)',
                                 results_base_folder + '/latency.pdf')
create_graph_multiple_time_value(threads, consumption_avg, 'Consumption', 'Threads', 'Consumption (?)',
                                 results_base_folder + '/consumption.pdf')

# for exp_num in range(1, 37):
#     result_path = state['exp_' + str(exp_num) + '_results_folder']
#     result_path = result_path.split('/')[-2]
#     exp_id = state['exp_' + str(exp_num) + '_id']
#     spout_parallelism = int(exp_id.split('_')[1])
#     op_parallelism = int(exp_id.split('_')[2])
#     sink_parallelism = int(exp_id.split('_')[3])
#     results_folder = results_base_folder + '/' + result_path + '/'
#     onlyfiles = [f for f in listdir(results_folder) if isfile(join(results_folder, f)) and 'RAPL' in f]
#     print('Analyzing result folder ' + results_folder + ' (experiment ' + str(exp_num) + ')')
#     [throughput, latency, consumption] = create_single_exp_graphs(state_folder, results_folder, onlyfiles[0],
#                                                                   spout_parallelism, op_parallelism, sink_parallelism)
#
#     throughput_avg.append(throughput)
#     latency_avg.append(latency)
#     consumption_avg.append(consumption)
#
# create_graph_time_value(range(1, 37), throughput_avg, 'Throughput', 'Threads', 'Throughput (t/s)',
#                         results_base_folder + '/throughput.pdf')
# create_graph_time_value(range(1, 37), latency_avg, 'Latency', 'Threads', 'Latency (?)',
#                         results_base_folder + '/latency.pdf')
# create_graph_time_value(range(1, 37), consumption_avg, 'Consumption', 'Threads', 'Consumption (?)',
#                         results_base_folder + '/consumption.pdf')
