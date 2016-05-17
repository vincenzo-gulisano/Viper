import json
from LinearRoadStateful.create_single_exp_graphs import create_single_exp_graphs
from LinearRoad.create_single_exp_graphs import create_graph_multiple_time_value
from os import listdir
from os.path import isfile, join
import statistics
from LinearRoadStateful.storm_vs_viper_paper_graph import create_overview_graph
import numpy

state_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/StatefulVehicleEnteringNewSegment/'
results_base_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/StatefulVehicleEnteringNewSegment'

# state_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/stateful/completerun_nostats1_2/'
# results_base_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/stateful/completerun_nostats1_2'

main_title = 'Storm '

stats_data = json.load(open(results_base_folder + '/summary.json', 'r'))

run_ranges = range(0, 1)


def compute_top_results(keys, throughput, latency, consumption):
    results = [[0 for x in range(3)] for x in range(3)]

    index = -1
    results[0][0] = 0
    k = ''

    for key in keys:
        for i in range(0, len(throughput[key])):
            if index == -1 or throughput[key][i] > results[0][0]:
                results[0][0] = throughput[key][i]
                index = i
                k = key

    results[0][1] = latency[k][index]
    results[0][2] = consumption[k][index]

    index = -1
    results[1][1] = 0
    k = ''

    for key in keys:
        for i in range(0, len(latency[key])):
            if index == -1 or latency[key][i] < results[1][1]:
                results[1][1] = latency[key][i]
                index = i
                k = key

    results[1][0] = throughput[k][index]
    results[1][2] = consumption[k][index]

    index = -1
    results[2][2] = 0
    k = ''

    for key in keys:
        for i in range(0, len(consumption[key])):
            if index == -1 or consumption[key][i] < results[2][2]:
                results[2][2] = consumption[key][i]
                index = i
                k = key

    results[2][0] = throughput[k][index]
    results[2][1] = latency[k][index]

    return results


markers = ['x', '+', 's', '*']

# for main_class in ['StatefulVehicleDetectAccident']:
# for main_class in ['StatelessForwardPositionReportsOnly', 'StatelessForwardStoppedCarsOnly',
#                    'StatefulVehicleEnteringNewSegment', 'StatefulVehicleDetectAccident']:
for main_class in ['StatefulVehicleEnteringNewSegment']:

    keys = []
    throughput_x = dict()
    throughput_y = dict()
    latency_x = dict()
    latency_y = dict()
    consumption_x = dict()
    consumption_y = dict()
    keys_markers = dict()
    keys_legend = dict()

    for type in ['storm', 'viper']:

        marker_index = 0

        for spout_parallelism in [1, 2, 4, 6]:
            key = type + '_' + str(spout_parallelism)
            keys.append(key)
            throughput_x[key] = []
            throughput_y[key] = []
            latency_x[key] = []
            latency_y[key] = []
            consumption_x[key] = []
            consumption_y[key] = []

            keys_markers[key] = markers[marker_index]
            keys_legend[key] = str(spout_parallelism) + ' Inj.'
            marker_index += 1

            for op_parallelism in [1, 2, 4, 6]:
                throughput_temp = []
                latency_temp = []
                consumption_temp = []
                for run in run_ranges:
                    throughput_temp.append(stats_data[
                                               type + '_' + main_class + '_S' + str(spout_parallelism) + 'vsO' + str(
                                                       op_parallelism) + '_' + str(run)][0])
                    latency_temp.append(stats_data[
                                            type + '_' + main_class + '_S' + str(spout_parallelism) + 'vsO' + str(
                                                    op_parallelism) + '_' + str(run)][1])
                    consumption_temp.append(stats_data[
                                                type + '_' + main_class + '_S' + str(spout_parallelism) + 'vsO' + str(
                                                        op_parallelism) + '_' + str(run)][2])
                throughput_x[key].append(op_parallelism)
                throughput_y[key].append(statistics.mean(throughput_temp))
                # print('Throughput for key ' + key + ' is ' + str(throughput_y[key][-1]))
                latency_x[key].append(op_parallelism)
                latency_y[key].append(statistics.mean(latency_temp))
                consumption_x[key].append(op_parallelism)
                consumption_y[key].append(statistics.mean(consumption_temp) / statistics.mean(throughput_temp))

                # Adding temp keys to print stats

        create_graph_multiple_time_value(throughput_x, throughput_y, keys, type + ' ' + main_class + ' Throughput',
                                         '# of ops', 'Throughput (t/s)',
                                         results_base_folder + '/' + type + '_' + main_class + '_throughput.pdf')
        create_graph_multiple_time_value(latency_x, latency_y, keys, type + ' ' + main_class + ' Latency',
                                         '# of ops', 'Latency (ms)',
                                         results_base_folder + '/' + type + '_' + main_class + '_latency.pdf')
        create_graph_multiple_time_value(consumption_x, consumption_y, keys, type + ' ' + main_class + ' Consumption',
                                         '# of ops', 'Consumption (w/t)',
                                         results_base_folder + '/' + type + '_' + main_class + '_consumption.pdf')
        # Print stats
        print(type + '_' + main_class)
        for i in range(0, 3):
            for spout_parallelism in [1, 2, 4, 6]:
                for op_parallelism in [1, 2, 4, 6]:
                    temp = [];
                    for run in run_ranges:
                        temp.append(stats_data[type + '_' + main_class + '_S' + str(spout_parallelism) + 'vsO' + str(
                                op_parallelism) + '_' + str(run)][i])
                    print(str(int(statistics.mean(temp))) + '\t', end='')
                print('')
            print('')

    storm_keys = [value for index, value in enumerate(keys) if 'storm' in value]
    viper_keys = [value for index, value in enumerate(keys) if 'viper' in value]

    create_overview_graph(storm_keys, viper_keys, keys_markers, keys_legend, [1, 2, 4, 6],
                          throughput_y, latency_y, consumption_y, throughput_y, latency_y, consumption_y,
                          [0.9, 1, 2, 4, 6, 6.1], ['', '1', '2', '4', '6', ''], [0, 250000, 500000, 750000, 1000000],
                          ['0', '250K', '500K', '750K', '1M'], [-50, 0, 1000, 2000, 3000, 4000],
                          ['', '0', '1000', '2000', '3000', ''], [0.00000, 0.00020, 0.00040, 0.00060, 0.0008, 0.001],
                          ['0.0', '0.2', '0.4', '0.6', '0.8', ''],
                          state_folder + main_class + '.pdf')

    # Summary results
    resultsStorm = compute_top_results(storm_keys, throughput_y, latency_y, consumption_y)
    print(str(resultsStorm))
    resultsViper = compute_top_results(viper_keys, throughput_y, latency_y, consumption_y)
    print(str(resultsViper))

    print(str(numpy.divide(resultsViper, resultsStorm)))
