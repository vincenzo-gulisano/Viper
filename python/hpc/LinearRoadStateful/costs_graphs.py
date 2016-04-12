import json
from LinearRoadStateful.create_single_exp_graphs import create_single_exp_graphs
from LinearRoad.create_single_exp_graphs import create_graph_multiple_time_value
from os import listdir
from os.path import isfile, join
import statistics
from LinearRoadStateful.storm_vs_viper_paper_graph import create_overview_graph
import numpy
from matplotlib import rcParams
from matplotlib.backends.backend_pdf import PdfPages
from scipy import stats as scipystat
import matplotlib.pyplot as plt

state_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/costs_plots/'
results_base_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/costs_plots'

# state_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/stateful/completerun_nostats1_2/'
# results_base_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/stateful/completerun_nostats1_2'

main_title = 'Storm '

stats_data = json.load(open(results_base_folder + '/summary.json', 'r'))

run_ranges = range(0, 1)

keys = []
throughput_x = dict()
throughput_y = dict()
latency_x = dict()
latency_y = dict()
consumption_x = dict()
consumption_y = dict()
keys_markers = dict()
keys_legend = dict()


def get_operations_durations_and_operators_costs(state_json_file, experiment_json_file, operators, instances,
                                                 result_boundaries_left,
                                                 result_boundaries_right):
    data = json.load(open(state_json_file, 'r'))

    start_ts = result_boundaries_left  # int(int(data['duration']) * 0.1)
    end_ts = result_boundaries_right  # int(int(data['duration']) * 0.9)

    operators_found = []
    instances_found = []
    operation_duration = []
    operator_cost = []

    results_json = json.load(open(experiment_json_file, 'r'))

    for o in operators:

        if str(o + "_cost_value") in results_json.keys():
            operators_found.append(o)
            instances_found.append(instances[o])
            operation_duration.append(
                    scipystat.trim_mean(results_json[o + "_cost_value"][start_ts:end_ts], 0) / pow(10, 3))

            operator_cost.append(
                    scipystat.trim_mean(results_json[o + "_cost_value"][start_ts:end_ts], 0) * scipystat.trim_mean(
                            results_json[o + "_invocations_value"][start_ts:end_ts], 0) / int(instances[o]) / pow(10,
                                                                                                                  9))

    return operators_found, instances_found, operation_duration, operator_cost


def analyze_experiments_in_state_file(state_json_file, exps, results_base_folder):
    data = json.load(open(state_json_file, 'r'))

    ncols = 4
    nrows = 4

    fig_number = 1
    subfig_index = 1

    rcParams.update({'figure.autolayout': False})
    pp = PdfPages(results_base_folder + 'durations' + str(fig_number) + '.pdf')

    f = plt.figure()
    f.set_size_inches(20, 10)

    for exp_num in range(1, exps + 1):
        result_path = data['exp_' + str(exp_num) + '_results_folder']
        result_path = result_path.split('/')[-2]
        exp_id = data['exp_' + str(exp_num) + '_id']
        spout_parallelism = int(exp_id.split('_')[1])
        op_parallelism = int(exp_id.split('_')[2])
        sink_parallelism = int(exp_id.split('_')[3])
        result_boundaries_left = data['exp_' + str(exp_num) + '_result_boundaries_left']
        result_boundaries_right = data['exp_' + str(exp_num) + '_result_boundaries_right']

        [operators, instances, operation_duration, operator_cost] = get_operations_durations_and_operators_costs(
                state_json_file, results_base_folder + '/' + result_path + '/exp_result.json',
                ['spout', 'op_merger', 'op', 'sink_merger', 'sink'],
                {'spout': spout_parallelism, 'op_merger': op_parallelism, 'op': op_parallelism,
                 'sink_merger': sink_parallelism, 'sink': sink_parallelism}, result_boundaries_left,
                result_boundaries_right)

        print(operators)
        print(instances)
        print(operation_duration)
        print(operator_cost)
        print()

        bar_width = 0.35

        ax = plt.subplot(ncols, nrows, subfig_index)
        #plt.bar(numpy.arange(len(operators)), operator_cost, bar_width)
        plt.bar(numpy.arange(len(operators)), operation_duration, bar_width)

        plt.title(exp_id)
        plt.xticks(numpy.arange(len(operators)), operators)
        plt.ylim(0, 10)

        if subfig_index >= ncols * nrows:
            # plt.subplots_adjust(left=0.16, right=0.99, top=0.93, bottom=0.1)
            plt.close()
            pp.savefig(f)
            pp.close()

            fig_number += 1
            subfig_index = 1
            pp = PdfPages(results_base_folder + 'durations' + str(fig_number) + '.pdf')

            f = plt.figure()
            f.set_size_inches(20, 10)
        else:
            subfig_index += 1

analyze_experiments_in_state_file(
        '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/costs_plots_2/state.json',
        32,
        '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/costs_plots_2/')
