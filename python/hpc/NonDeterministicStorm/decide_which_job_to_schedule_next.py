__author__ = 'vinmas'

import sys

# sys.path.append('/nas_home/goud/.local/lib64/python2.6/site-packages/')
sys.path.insert(0, '/nas_home/goud/.local/lib64/python2.6/site-packages/')

from create_json_for_experiment_results import read_topology_parallel_op_data_and_store_json
import json
from scipy import stats as scipystat
from optparse import OptionParser
from create_script_and_schedule_job import create_script_and_schedule_job

parser = OptionParser()
parser.add_option("-r", "--resultsfolder", dest="resultsfolder",
                  help="folder containing experiment results", metavar="FOLDER")
parser.add_option("-S", "--statefolder", dest="statefolder",
                  help="folder to keep current state of the experiment", metavar="STATE")

(options, args) = parser.parse_args()

if options.resultsfolder is None:
    print('A mandatory option (-r, --resultsfolder) is missing\n')
    parser.print_help()
    exit(-1)
if options.statefolder is None:
    print('A mandatory option (-S, --statefolder) is missing\n')
    parser.print_help()
    exit(-1)

data = json.load(open(options.statefolder + 'state.json', 'r'))

# print(data)

exp_num = data['experiment_number']
configure_next_exp_parallelim = data['exp_' + exp_num + '_config_next'] in ['True']

read_topology_parallel_op_data_and_store_json(
    options.resultsfolder + data['exp_' + exp_num + '_id'] + '_',
    ['spout', 'op', 'sink'],
    [int(data['exp_' + exp_num + '_spout_parallelism']), int(data['exp_' + exp_num + '_op_parallelism']),
     int(data['exp_' + exp_num + '_sink_parallelism'])], options.resultsfolder + 'exp_result.json')

results_json = json.load(open(options.resultsfolder + 'exp_result.json', 'r'))

start_ts = int(int(data['duration']) * 0.1)
end_ts = int(int(data['duration']) * 0.9)

# Compute average throughput, latency and cost

throughput = []
latency = []
cost = []
print('')
operators = ['spout', 'op', 'sink']
for o in operators:

    throughput.append(scipystat.trim_mean(results_json[o + "_rate_value"][start_ts:end_ts], 0.05))
    if str(o + "_latency_value") in results_json.keys():
        latency.append(scipystat.trim_mean(results_json[o + "_latency_value"][start_ts:end_ts], 0.05))
    else:
        latency.append(0.0)
    cost.append(scipystat.trim_mean(results_json[o + "_cost_value"][start_ts:end_ts], 0.05) * scipystat.trim_mean(
        results_json[o + "_invocations_value"][start_ts:end_ts], 0.05) / int(
        data['exp_' + exp_num + '_' + o + '_parallelism']) / pow(10, 9))

    print(o + ': T ' + str(throughput[-1]) + ' L ' + str(latency[-1])
          + ' C ' + str(cost[-1]))

threshold = 0.9
operators_above_threshold = 0
rightmost_operator_above_threshold = 0
index = 0
for c in cost:
    if c > threshold:
        operators_above_threshold += 1
        rightmost_operator_above_threshold = index
        print('Operator ' + operators[index] + ' is above threshold')
    index += 1

highest_cost_op = operators[cost.index(max(cost))]
print('Operator with highest cost is ' + highest_cost_op)
if operators_above_threshold > 0:
    print('But, since there is at least one operator above threshold ' + str(threshold) + '...')
    highest_cost_op = operators[rightmost_operator_above_threshold]
    print('The thread goes to ' + highest_cost_op)

# Keep some stats...
data['exp_' + exp_num + '_spout_throughput'] = str(throughput[0])
data['exp_' + exp_num + '_spout_latency'] = str(latency[0])
data['exp_' + exp_num + '_spout_cost'] = str(cost[0])
data['exp_' + exp_num + '_op_throughput'] = str(throughput[1])
data['exp_' + exp_num + '_op_latency'] = str(latency[1])
data['exp_' + exp_num + '_op_cost'] = str(cost[1])
data['exp_' + exp_num + '_sink_throughput'] = str(throughput[2])
data['exp_' + exp_num + '_sink_latency'] = str(latency[2])
data['exp_' + exp_num + '_sink_cost'] = str(cost[2])
data['exp_' + exp_num + '_highest_cost_op'] = highest_cost_op
# data[data['experiment_number'] + '_exp_id'] = data['exp_id']
# data[data['experiment_number'] + '_command'] = data['command']
data['exp_' + exp_num + '_results_folder'] = options.resultsfolder

print('\n\n')

prev_exp_num = exp_num
exp_num = str(int(exp_num) + 1)
data['experiment_number'] = exp_num

# if str('exp_' + exp_num + '_config_next') in data.keys():
if configure_next_exp_parallelim:
    # if int(data['available_threads']) > 0:
    #     data['available_threads'] = str(int(data['available_threads']) - 1)
    #     data['experiment_number'] = str(int(data['experiment_number']) + 1)
    # data[highest_cost_op + '_parallelism'] = str(int(data[highest_cost_op + '_parallelism']) + 1)

    # copy previous parallelism
    data['exp_' + exp_num + '_spout_parallelism'] = data['exp_' + prev_exp_num + '_spout_parallelism']
    data['exp_' + exp_num + '_op_parallelism'] = data['exp_' + prev_exp_num + '_op_parallelism']
    data['exp_' + exp_num + '_sink_parallelism'] = data['exp_' + prev_exp_num + '_sink_parallelism']
    # Update parallelism
    data['exp_' + exp_num + '_' + highest_cost_op + '_parallelism'] = str(
        int(data['exp_' + exp_num + '_' + highest_cost_op + '_parallelism']) + 1)

# Update exp_id and command
exp_id = data['exp_' + exp_num + '_rep'] + '_' + data['exp_' + exp_num + '_spout_parallelism'] + '_' + data[
    'exp_' + exp_num + '_op_parallelism'] + '_' + data['exp_' + exp_num + '_sink_parallelism'] + '_' + data[
             'exp_' + exp_num + '_selectivity'] + '_' + data[
             'exp_' + exp_num + '_load'] + '_NonDeterministicStorm'
exp_id = exp_id.replace('.', '-')

command = 'usecases.debs2015.MergerTestNonDeterministic false true \$LOGDIR \$kill_id ' + str(
    data['duration']) + ' ' + str(data['exp_' + exp_num + '_spout_parallelism']) + ' ' + str(
    data['exp_' + exp_num + '_op_parallelism']) + ' ' + str(
    data['exp_' + exp_num + '_sink_parallelism']) + ' ' + str(
    data['exp_' + exp_num + '_selectivity']) + ' ' + str(data['exp_' + exp_num + '_load']) + ' 1'

data['exp_' + exp_num + '_id'] = exp_id
data['exp_' + exp_num + '_command'] = command

json.dump(data, open(options.statefolder + '/state.json', 'w'))
create_script_and_schedule_job(data['scriptsfolder'], data['header'], data['body'],
                               data['script'], exp_id, command, data['runner'])
