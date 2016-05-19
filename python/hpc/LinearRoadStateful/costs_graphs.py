import json
import numpy
from matplotlib import rcParams
from matplotlib.backends.backend_pdf import PdfPages
from scipy import stats as scipystat
import matplotlib.pyplot as plt
from statistics import median


# PLEASE NOTICE: THIS SCRIPT WORKS AS LONS AS THE QUERY IS A STRAIGHT LINE OF OPERATORS (EVEN PARALLEL OPERATORS).
# IF AN OPERATOR FED BY TWO DISTINCT OPERATORS EXISTS, THE RESULTS OF THE SCRIPT MAKE NO SENSE!

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

    for index in range(0, len(operators)):

        o = operators[index]

        if str(o + "_cost_value") in results_json.keys():
            operators_found.append(o)
            instances_found.append(instances[o])

            # operation_duration.append(
            #         scipystat.trim_mean(results_json[o + "_cost_value"][start_ts:end_ts], 0))
            operation_duration.append(median(results_json[o + "_cost_value"][start_ts:end_ts]))

            if index == 0:
                # operator_cost.append(
                #         scipystat.trim_mean(results_json[o + "_cost_value"][start_ts:end_ts], 0) * scipystat.trim_mean(
                #                 results_json[o + "_invocations_value"][start_ts:end_ts], 0) / int(
                #                 instances[o]) / pow(10, 9))
                operator_cost.append(median(results_json[o + "_cost_value"][start_ts:end_ts]) * median(
                        results_json[o + "_invocations_value"][start_ts:end_ts]) / int(
                        instances[o]) / pow(10, 9))

            else:

                prev_index = index - 1
                if 'merger' in operators[prev_index]:
                    prev_index -= 1

                print(
                        'in order to compute the cost of operator ' + o + ' the script is going to use the rate of operator ' +
                        operators[prev_index])

                # operator_cost.append(
                #         scipystat.trim_mean(results_json[o + "_cost_value"][start_ts:end_ts], 0) * scipystat.trim_mean(
                #                 results_json[operators[prev_index] + "_rate_value"][start_ts:end_ts], 0) / int(
                #                 instances[o]) / pow(10,
                #                                     9))
                # operator_cost.append(
                #         scipystat.trim_mean(results_json[o + "_cost_value"][start_ts:end_ts], 0) * scipystat.trim_mean(
                #                 results_json[operators[prev_index] + "_rate_value"][start_ts:end_ts], 0) / int(
                #                 instances[o]) / pow(10, 9))
                operator_cost.append(median(results_json[o + "_cost_value"][start_ts:end_ts]) * median(
                        results_json[operators[prev_index] + "_rate_value"][start_ts:end_ts]) / int(
                        instances[o]) / pow(10, 9))

            index += 1

    return operators_found, instances_found, operation_duration, operator_cost


def analyze_experiments_in_state_file(state_json_file, exps, results_base_folder, type_of_stat):
    data = json.load(open(state_json_file, 'r'))

    ncols = 4
    nrows = 4

    fig_number = 1
    subfig_index = 1

    rcParams.update({'figure.autolayout': False})
    if type_of_stat == 'durations':
        pp = PdfPages(results_base_folder + 'durations' + str(fig_number) + '.pdf')
    elif type_of_stat == 'costs':
        pp = PdfPages(results_base_folder + 'costs' + str(fig_number) + '.pdf')
    elif type_of_stat == 'individualmaxrates':
        pp = PdfPages(results_base_folder + 'individualmaxrates' + str(fig_number) + '.pdf')
    elif type_of_stat == 'maxrates':
        pp = PdfPages(results_base_folder + 'maxrates' + str(fig_number) + '.pdf')
    else:
        print('Unkown type_of_stat value')
        exit()

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

        # TEMP HACK TO CREATE GRAPHS
        if spout_parallelism == 6 and op_parallelism == 6 and sink_parallelism == 6:

            print('Experiment id: ' + exp_id + ' (' + data['exp_' + str(exp_num) + '_main_class'] + ')')
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

            if type_of_stat == 'durations':
                plt.bar(numpy.arange(len(operators)), operation_duration, bar_width)
                for i in numpy.arange(len(operators)):
                    ax.text(i, operation_duration[i] + .01, str("{0:.2f}".format(operation_duration[i])), color='blue',
                            fontweight='bold')
                plt.ylim(0, 20)
            elif type_of_stat == 'costs':
                plt.bar(numpy.arange(len(operators)), operator_cost, bar_width)
                for i in numpy.arange(len(operators)):
                    ax.text(i, operator_cost[i] + .01, str("{0:.2f}".format(operator_cost[i])), color='blue',
                            fontweight='bold')
                plt.ylim(0, 10)
            elif type_of_stat == 'individualmaxrates':
                individual_rates = list(numpy.array(operator_cost) * 1000000 / numpy.array(operation_duration))
                plt.bar(numpy.arange(len(operators)), individual_rates, bar_width)
                for i in numpy.arange(len(operators)):
                    ax.text(i, individual_rates[i] + .01, str("{0:.2f}".format(individual_rates[i])), color='blue',
                            fontweight='bold')
                plt.ylim(0, 700000)
            elif type_of_stat == 'maxrates':
                rates = list(numpy.array(operator_cost) * 1000000 / numpy.array(operation_duration)) * numpy.array(
                        instances)
                plt.bar(numpy.arange(len(operators)), rates, bar_width)
                for i in numpy.arange(len(operators)):
                    ax.text(i, rates[i] + .01, str("{0:.2f}".format(rates[i])), color='blue', fontweight='bold')
                plt.ylim(0, 700000)
            else:
                print('Unkown type_of_stat value')
                exit()

            plt.title(exp_id)
            plt.xticks(numpy.arange(len(operators)), operators)

            if subfig_index >= ncols * nrows:
                plt.close()
                pp.savefig(f)
                pp.close()

                fig_number += 1
                subfig_index = 1
                if type_of_stat == 'durations':
                    pp = PdfPages(results_base_folder + 'durations' + str(fig_number) + '.pdf')
                elif type_of_stat == 'costs':
                    pp = PdfPages(results_base_folder + 'costs' + str(fig_number) + '.pdf')
                elif type_of_stat == 'individualmaxrates':
                    pp = PdfPages(results_base_folder + 'individualmaxrates' + str(fig_number) + '.pdf')
                elif type_of_stat == 'maxrates':
                    pp = PdfPages(results_base_folder + 'maxrates' + str(fig_number) + '.pdf')
                else:
                    print('Unkown type_of_stat value')
                    exit()

                f = plt.figure()
                f.set_size_inches(20, 10)
            else:
                subfig_index += 1


# analyze_experiments_in_state_file(
#         '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/costs_plots_4/state.json',
#         32,
#         '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/costs_plots_4/',
#         'durations')
#
# analyze_experiments_in_state_file(
#         '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/costs_plots_4/state.json',
#         32,
#         '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/costs_plots_4/',
#         'costs')
#
# analyze_experiments_in_state_file(
#         '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/costs_plots_4/state.json',
#         32,
#         '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/costs_plots_4/',
#         'individualmaxrates')
#
# analyze_experiments_in_state_file(
#         '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/costs_plots_4/state.json',
#         32,
#         '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/costs_plots_4/',
#         'maxrates')

# analyze_experiments_in_state_file(
#         '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/all/run2/state.json',
#         128,
#         '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/all/run2/',
#         'durations')

analyze_experiments_in_state_file(
        '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/StatefulOps/run2/state.json',
        64,
        '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/ticks_smartqueues/StatefulOps/run2/',
        'durations')