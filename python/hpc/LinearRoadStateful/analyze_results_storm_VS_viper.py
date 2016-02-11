import json
from LinearRoadStateful.create_single_exp_graphs import create_single_exp_graphs
from LinearRoad.create_single_exp_graphs import create_graph_multiple_time_value
from os import listdir
from os.path import isfile, join

state_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/stateful/completerun_multiplesinks/'
results_base_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/stateful/completerun_multiplesinks'
main_title = 'Storm '

state = json.load(open(state_folder + 'state.json', 'r'))
# json_out_id = '2_viper'

stats_data = dict()

exp_num = 1
# for type in ['storm', 'viper']:
for type in ['storm', 'viper']:
    for main_class in ['StatefulVehicleEnteringNewSegment', 'StatelessForwardPositionReportsOnly',
                       'StatelessForwardStoppedCarsOnly']:
        for spout_parallelism in [1, 2, 4, 6]:
            for op_parallelism in [1, 2, 4, 6]:

                # if spout_parallelism==2 and op_parallelism==1 and type in 'viper' and main_class in 'StatelessForwardPositionReportsOnly':

                result_path = state['exp_' + str(exp_num) + '_results_folder']
                result_path = result_path.split('/')[-2]
                exp_id = state['exp_' + str(exp_num) + '_id']
                spout_parallelism = int(exp_id.split('_')[1])
                op_parallelism = int(exp_id.split('_')[2])
                sink_parallelism = int(exp_id.split('_')[3])
                results_folder = results_base_folder + '/' + result_path + '/'
                onlyfiles = [f for f in listdir(results_folder) if isfile(join(results_folder, f)) and 'RAPL' in f]
                print('Analyzing result folder ' + results_folder + ' (experiment ' + str(exp_num) + ')')

                [throughput, latency, consumption, highest_throughput_stat] = create_single_exp_graphs(state_folder,
                                                                                                       results_folder,
                                                                                                       onlyfiles[0],
                                                                                                       spout_parallelism,
                                                                                                       op_parallelism,
                                                                                                       sink_parallelism,
                                                                                                       True)

                stats_data[type + '_' + main_class + '_' + str(spout_parallelism) + 'vs' + str(
                        op_parallelism)] = highest_throughput_stat

                number_of_threads = spout_parallelism + op_parallelism + sink_parallelism
                if spout_parallelism > 1:
                    number_of_threads += op_parallelism
                if op_parallelism > 1:
                    number_of_threads += sink_parallelism

                exp_num += 1

        # Print stats
        print(type + '_' + main_class)
        for i in range(0, 3):
            for spout_parallelism in [1, 2, 4, 6]:
                for op_parallelism in [1, 2, 4, 6]:
                    print(str(
                            stats_data[
                                type + '_' + main_class + '_' + str(spout_parallelism) + 'vs' + str(op_parallelism)][
                                i]) + '\t', end='')
                print('')
            print('')
