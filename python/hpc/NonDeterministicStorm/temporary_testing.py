__author__ = 'vinmas'
from NonDeterministicStorm.create_json_for_experiment_results import read_topology_parallel_op_data_and_store_json
import json
from scipy import stats as scipystat
from optparse import OptionParser
from create_script_and_schedule_job import create_script_and_schedule_job

# data = dict()
# data['experiment_number'] = "1"
# data['repetition'] = "1"
# data['repetitions_per_experiment'] = "1"
# data['duration'] = "300"
# data['spout_parallelism'] = "1"
# data['op_parallelism'] = "1"
# data['sink_parallelism'] = "1"
# data['available_threads'] = "10"
# data['assigned_threads'] = "0"
# data['max_selectivity'] = "1.0"
# data['min_selectivity'] = "0.1"
# data['selectivity_step'] = "0.1"
# data['selectivity'] = "1.0"
# data['max_load'] = "1.0"
# data['min_load'] = "0.0"
# data['load_step'] = "0.1"
# data['load'] = "1.0"
#
#
#
# data = json.dump(data,open('/Users/vinmas/Downloads/ImprovedParallelStorm_2015-11-06_17.49/state.json', 'w'))

data = dict()

exp_num = 1
for load in [1.0, 0.1, 0]:
    for selectivity in [1.0, 0.1, 0.01]:
        for thread in range(0, 10):
            for repetition in range(0, 1):
                # data['exp_' + str(exp_num) + '_num'] = str(exp_num)
                data['exp_' + str(exp_num) + '_spout_parallelism'] = "1"
                data['exp_' + str(exp_num) + '_op_parallelism'] = "1"
                data['exp_' + str(exp_num) + '_sink_parallelism'] = "1"
                data['exp_' + str(exp_num) + '_load'] = str(load)
                data['exp_' + str(exp_num) + '_selectivity'] = str(selectivity)
                data['exp_' + str(exp_num) + '_rep'] = str(repetition)
                data['exp_' + str(exp_num) + '_config_next'] = "True"
                exp_num += 1
        for repetition in range(0, 1):
            # data['exp_' + str(exp_num) + '_num'] = str(exp_num)
            data['exp_' + str(exp_num) + '_spout_parallelism'] = "1"
            data['exp_' + str(exp_num) + '_op_parallelism'] = "1"
            data['exp_' + str(exp_num) + '_sink_parallelism'] = "1"
            data['exp_' + str(exp_num) + '_load'] = str(load)
            data['exp_' + str(exp_num) + '_selectivity'] = str(selectivity)
            data['exp_' + str(exp_num) + '_rep'] = str(repetition)
            data['exp_' + str(exp_num) + '_config_next'] = "False"
            exp_num += 1

data = json.load(open('/Users/vinmas/repositories/viper_experiments/151202/state.json', 'r'))
for exp_num in range(1, 37):
    configure_next_exp_parallelim = data['exp_' + str(exp_num) + '_config_next'] in ['True']
    print('exp_' + str(exp_num) + '_config_next: ' + data['exp_' + str(exp_num) + '_config_next'] + ' --> ' + str(
        configure_next_exp_parallelim))
    print('exp_' + str(exp_num) + '_results_folder: ' + data['exp_' + str(exp_num) + '_results_folder'])
    #
    # parser = OptionParser()
    # parser.add_option("-r", "--resultsfolder", dest="resultsfolder",
    #                   help="folder containing experiment results", metavar="FOLDER")
    # parser.add_option("-S", "--statefolder", dest="statefolder",
    #                   help="folder to keep current state of the experiment", metavar="STATE")
    #
    # (options, args) = parser.parse_args()
    #
    # if options.resultsfolder is None:
    #     print('A mandatory option (-r, --resultsfolder) is missing\n')
    #     parser.print_help()
    #     exit(-1)
    # if options.statefolder is None:
    #     print('A mandatory option (-S, --statefolder) is missing\n')
    #     parser.print_help()
    #     exit(-1)
    #
    # data = json.load(open(options.statefolder + 'state.json', 'r'))
    #
    # data['exp_id'] = '0_1_1_1_1_0_NonDeterministicStorm201511061749'
    # # data['command'] = command
    #
    # print(data)
    #
    # read_topology_parallel_op_data_and_store_json(
    #     options.resultsfolder + data['exp_id'] + '_',
    #     ['spout', 'op', 'sink'], [int(data['spout_parallelism']), int(data['op_parallelism']), int(data['sink_parallelism'])],
    #     options.resultsfolder + 'exp_result.json')
    #
    #
    # results_json = json.load(open(options.resultsfolder + 'exp_result.json', 'r'))
    #
    # start_ts = int(int(data['duration']) * 0.1)
    # end_ts = int(int(data['duration']) * 0.9)
    #
    # # Compute average throughput, latency and cost
    #
    # throughput = []
    # latency = []
    # cost = []
    # print('')
    # operators = ['spout', 'op', 'sink']
    # for o in operators:
    #
    #     throughput.append(scipystat.trim_mean(results_json[o + "_rate_value"][start_ts:end_ts], 0.05))
    #     if str(o + "_latency_value") in results_json.keys():
    #         latency.append(scipystat.trim_mean(results_json[o + "_latency_value"][start_ts:end_ts], 0.05))
    #     else:
    #         latency.append(0.0)
    #     cost.append(scipystat.trim_mean(results_json[o + "_cost_value"][start_ts:end_ts], 0.05) * scipystat.trim_mean(
    #         results_json[o + "_invocations_value"][start_ts:end_ts], 0.05) / int(data[o + '_parallelism']) / pow(10, 9))
    #
    #     print(o + ': T ' + str(throughput[-1]) + ' L ' + str(latency[-1])
    #           + ' C ' + str(cost[-1]))
    #
    # threshold = 0.9
    # operators_above_threshold = 0
    # rightmost_operator_above_threshold = 0
    # index = 0
    # for c in cost:
    #     if c > threshold:
    #         operators_above_threshold += 1
    #         rightmost_operator_above_threshold = index
    #         print('Operator ' + operators[index] + ' is above threshold')
    #     index += 1
    #
    # highest_cost_op = operators[cost.index(max(cost))]
    # print('Operator with highest cost is ' + highest_cost_op)
    # if operators_above_threshold > 0:
    #     print('But, since there is at least one operator above threshold ' + str(threshold) + '...')
    #     highest_cost_op = operators[rightmost_operator_above_threshold]
    #     print('The thread goes to ' + highest_cost_op)
    #
    # print('\n\n')
    #
    # if int(data['available_threads']) > 0:
    #     data['available_threads'] = str(int(data['available_threads'])-1)
    #     data[highest_cost_op+'_parallelism'] = str(int(data[highest_cost_op+'_parallelism'])+1)
    #
    #     exp_id = data['repetition'] + '_' + data['spout_parallelism'] + '_' + data['op_parallelism'] + '_' + data[
    #         'sink_parallelism'] + '_' + data['selectivity'] + '_' + data['load'] + '_NonDeterministicStorm'
    #     exp_id = exp_id.replace('.', '-')
    #
    #     command = 'usecases.debs2015.MergerTestNonDeterministic false true \$LOGDIR \$kill_id ' + str(
    #         data['duration']) + ' ' + str(data['spout_parallelism']) + ' ' + str(data['op_parallelism']) + ' ' + str(
    #         data['sink_parallelism']) + ' ' + str(data['selectivity']) + ' ' + str(data['load']) + ' 1'
    #
    #     data['exp_id'] = exp_id
    #     data['command'] = command
    #
    #     json.dump(data, open(options.statefolder + '/state.json', 'w'))
    #     create_script_and_schedule_job(options.scriptsfolder, options.header, options.body,
    #                                                               options.script, exp_id, command, options.runner)
