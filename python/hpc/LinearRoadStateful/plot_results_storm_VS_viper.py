import json
from LinearRoadStateful.create_single_exp_graphs import create_single_exp_graphs
from LinearRoad.create_single_exp_graphs import create_graph_multiple_time_value
from os import listdir
from os.path import isfile, join
import statistics

state_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/stateful/completerun_nostats1_2/'
results_base_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/stateful/completerun_nostats1_2'
main_title = 'Storm '

stats_data = json.load(open(results_base_folder + '/summary.json', 'r'))

for type in ['storm', 'viper']:
    for main_class in ['StatefulVehicleEnteringNewSegment', 'StatelessForwardPositionReportsOnly',
                       'StatelessForwardStoppedCarsOnly']:
        keys = []
        throughput_x = dict()
        throughput_y = dict()
        latency_x = dict()
        latency_y = dict()
        consumption_x = dict()
        consumption_y = dict()

        for spout_parallelism in [1, 2, 4, 6]:
            key = str(spout_parallelism)
            keys.append(key)
            throughput_x[key] = []
            throughput_y[key] = []
            latency_x[key] = []
            latency_y[key] = []
            consumption_x[key] = []
            consumption_y[key] = []

            for op_parallelism in [1, 2, 4, 6]:
                throughput_temp = []
                latency_temp = []
                consumption_temp = []
                for run in range(0, 3):
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
                    for run in range(0, 3):
                        temp.append(stats_data[type + '_' + main_class + '_S' + str(spout_parallelism) + 'vsO' + str(
                                op_parallelism) + '_' + str(run)][i])
                    print(str(int(statistics.mean(temp))) + '\t', end='')
                print('')
            print('')
