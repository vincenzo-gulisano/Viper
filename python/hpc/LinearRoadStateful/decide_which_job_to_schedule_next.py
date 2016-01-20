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

spout_instances = int(data['exp_' + exp_num + '_spout_parallelism'])
op_instances = int(data['exp_' + exp_num + '_op_parallelism'])
sink_instances = int(data['exp_' + exp_num + '_sink_parallelism'])

operators = ['spout']
instances = [spout_instances]
if spout_instances > 1:
    operators.append('op_merger')
    instances.append(op_instances)
operators.append('op')
instances.append(op_instances)
if op_instances > 1:
    operators.append('sink_merger')
    instances.append(sink_instances)
operators.append('sink')
instances.append(sink_instances)

read_topology_parallel_op_data_and_store_json(
        options.resultsfolder + data['exp_' + exp_num + '_id'] + '_', operators, instances,
        options.resultsfolder + 'exp_result.json')

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
if highest_cost_op == 'op_merger':
    print('Since the operator is the op_merger, the thread actually goes to op')
    highest_cost_op = 'op'
if highest_cost_op == 'sink_merger':
    print('Since the operator is the sink_merger, the thread actually goes to sink')
    highest_cost_op = 'sink'

# Keep some stats...

index = 0

data['exp_' + exp_num + '_spout_throughput'] = str(throughput[index])
data['exp_' + exp_num + '_spout_latency'] = str(latency[index])
data['exp_' + exp_num + '_spout_cost'] = str(cost[index])
index += 1

if spout_instances > 1:
    data['exp_' + exp_num + '_op_merger_throughput'] = str(throughput[index])
    data['exp_' + exp_num + '_op_merger_latency'] = str(latency[index])
    data['exp_' + exp_num + '_op_merger_cost'] = str(cost[index])
    index += 1

data['exp_' + exp_num + '_op_throughput'] = str(throughput[index])
data['exp_' + exp_num + '_op_latency'] = str(latency[index])
data['exp_' + exp_num + '_op_cost'] = str(cost[index])
index += 1

if op_instances > 1:
    data['exp_' + exp_num + '_sink_merger_throughput'] = str(throughput[index])
    data['exp_' + exp_num + '_sink_merger_latency'] = str(latency[index])
    data['exp_' + exp_num + '_sink_merger_cost'] = str(cost[index])
    index += 1

data['exp_' + exp_num + '_sink_throughput'] = str(throughput[index])
data['exp_' + exp_num + '_sink_latency'] = str(latency[index])
data['exp_' + exp_num + '_sink_cost'] = str(cost[index])

data['exp_' + exp_num + '_highest_cost_op'] = highest_cost_op
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
exp_id = data['exp_' + data['experiment_number'] + '_rep'] + '_' + data[
    'exp_' + data['experiment_number'] + '_spout_parallelism'] + '_' + data[
             'exp_' + data['experiment_number'] + '_op_parallelism'] + '_' + data[
             'exp_' + data['experiment_number'] + '_sink_parallelism'] + '_' + data[
             'exp_' + data['experiment_number'] + '_useoptimizedqueues'] + '_' + data[
             'exp_' + data['experiment_number'] + '_type']
exp_id = exp_id.replace('.', '-')

command = 'usecases.linearroad.' + data[
    'exp_' + data['experiment_number'] + '_main_class'] + ' false true \$LOGDIR \$kill_id ' + str(
    data['duration']) + ' ' + str(
    data['exp_' + data['experiment_number'] + '_spout_parallelism']) + ' ' + str(
    data['exp_' + data['experiment_number'] + '_op_parallelism']) + ' ' + str(
    data['exp_' + data['experiment_number'] + '_sink_parallelism']) + ' ' + str(
    data['input_file']) + ' ' + str(data['exp_' + data['experiment_number'] + '_useoptimizedqueues']) + ' 1'

data['exp_' + exp_num + '_id'] = exp_id
data['exp_' + exp_num + '_command'] = command

json.dump(data, open(options.statefolder + '/state.json', 'w'))
create_script_and_schedule_job(data['scriptsfolder'], data['header'], data['body'],
                               data['script'], exp_id, command, data['runner'])
