import json
from LinearRoad.create_single_exp_graphs import create_single_exp_graphs
from LinearRoad.create_single_exp_graphs import create_graph_multiple_time_value
from os import listdir
from os.path import isfile, join

state_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/160114/'
results_base_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/160114'
main_title = 'Storm '

state = json.load(open(state_folder + 'state.json', 'r'))

stats_data = dict()
exp_num = 1
# for type in ['storm', 'viper']:
for type in ['storm']:

    keys = []

    threads = dict()
    throughput_avg = dict()
    latency_avg = dict()
    consumption_avg = dict()
    op_cost_avg = dict()
    selectivity_avg = dict()

    for main_class in ['LRStateless', 'LRStatelessConvertingToo', 'LRStatelessConvertingFilteringToo']:

        id = main_class + '_' + type
        keys.append(id)
        threads[id] = []
        throughput_avg[id] = []
        latency_avg[id] = []
        consumption_avg[id] = []
        op_cost_avg[id] = []
        selectivity_avg[id] = []

        for thread in range(0, 21):
            for repetition in range(0, 1):

                if exp_num == 77:
                    x = 2
                result_path = state['exp_' + str(exp_num) + '_results_folder']
                result_path = result_path.split('/')[-2]
                exp_id = state['exp_' + str(exp_num) + '_id']
                spout_parallelism = int(exp_id.split('_')[1])
                op_parallelism = int(exp_id.split('_')[2])
                sink_parallelism = int(exp_id.split('_')[3])
                results_folder = results_base_folder + '/' + result_path + '/'
                onlyfiles = [f for f in listdir(results_folder) if isfile(join(results_folder, f)) and 'RAPL' in f]
                print('Analyzing result folder ' + results_folder + ' (experiment ' + str(exp_num) + ')')
                [throughput, latency, consumption] = create_single_exp_graphs(state_folder,
                                                                                                    results_folder,
                                                                                                    onlyfiles[0],
                                                                                                    spout_parallelism,
                                                                                                    op_parallelism,
                                                                                                    sink_parallelism)

                stats_data[
                    id + '_thread_' + str(thread) + '_repetition_' + str(
                        repetition) + '_spout_parallelism'] = spout_parallelism
                stats_data[id + '_thread_' + str(thread) + '_repetition_' + str(
                    repetition) + '_op_parallelism'] = op_parallelism
                stats_data[id + '_thread_' + str(thread) + '_repetition_' + str(
                    repetition) + '_sink_parallelism'] = sink_parallelism
                stats_data[id + '_thread_' + str(thread) + '_repetition_' + str(
                    repetition) + '_throughput'] = throughput
                stats_data[id + '_thread_' + str(thread) + '_repetition_' + str(
                    repetition) + '_latency'] = latency
                stats_data[id + '_thread_' + str(thread) + '_repetition_' + str(
                    repetition) + '_consumption'] = consumption
                # stats_data[id + '_thread_' + str(thread) + '_repetition_' + str(
                #     repetition) + '_op_cost'] = op_cost
                # stats_data[id + '_thread_' + str(thread) + '_repetition_' + str(
                #     repetition) + '_selectivity'] = selectivity

                threads[id].append(spout_parallelism + op_parallelism + sink_parallelism)
                throughput_avg[id].append(throughput)
                latency_avg[id].append(latency)
                consumption_avg[id].append(consumption)
                # op_cost_avg[id].append(op_cost)
                # selectivity_avg[id].append(selectivity)

                exp_num += 1

        create_graph_multiple_time_value(threads, throughput_avg, keys, main_title + 'Throughput', 'Threads',
                                         'Throughput (t/s)', results_base_folder + '/' + id + '_throughput.pdf')
        create_graph_multiple_time_value(threads, latency_avg, keys, main_title + 'Latency', 'Threads', 'Latency (ms)',
                                         results_base_folder + '/' + id + '_latency.pdf')
        create_graph_multiple_time_value(threads, consumption_avg, keys, main_title + 'Consumption', 'Threads',
                                         'Consumption (W/t)', results_base_folder + '/' + id + '_consumption.pdf')
        # create_graph_multiple_time_value(threads, op_cost_avg, keys, main_title + 'Operator Cost', 'Threads',
        #                                  'Cost (nanoseconds)', results_base_folder + '/' + id + '_opcost.pdf')
        # create_graph_multiple_time_value(threads, selectivity_avg, keys, main_title + 'Operator Selectivity', 'Threads',
        #                                  'Selectivity', results_base_folder + '/' + id + '_selectivity.pdf')

json.dump(stats_data, open(results_base_folder + '/stats_data.json', 'w'))
