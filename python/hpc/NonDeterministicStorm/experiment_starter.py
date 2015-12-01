__author__ = 'vinmas'

from optparse import OptionParser
# from NonDeterministicStorm.create_script_and_schedule_job import create_script_and_schedule_job
from create_script_and_schedule_job import create_script_and_schedule_job
import json

###############################

parser = OptionParser()
parser.add_option("-f", "--scriptsfolder", dest="scriptsfolder",
                  help="folder containing header and body parts of the scripts", metavar="FOLDER")
parser.add_option("-t", "--theader", dest="header",
                  help="header part of the script", metavar="HEADER")
parser.add_option("-b", "--body", dest="body",
                  help="body part of the script", metavar="BODY")
parser.add_option("-s", "--script", dest="script",
                  help="name of the script to run", metavar="BODY")
parser.add_option("-r", "--runner", dest="runner",
                  help="run script to schedule a job", metavar="RUNNER")
parser.add_option("-S", "--statefolder", dest="statefolder",
                  help="folder to keep current state of the experiment", metavar="STATE")

(options, args) = parser.parse_args()

if options.scriptsfolder is None:
    print('A mandatory option (-f, --scriptsfolder) is missing\n')
    parser.print_help()
    exit(-1)
if options.header is None:
    print('A mandatory option (-h, --header) is missing\n')
    parser.print_help()
    exit(-1)
if options.body is None:
    print('A mandatory option (-b, --body) is missing\n')
    parser.print_help()
    exit(-1)
if options.script is None:
    print('A mandatory option (-s, --script) is missing\n')
    parser.print_help()
    exit(-1)
if options.runner is None:
    print('A mandatory option (-r, --runner) is missing\n')
    parser.print_help()
    exit(-1)
if options.statefolder is None:
    print('A mandatory option (-S, --statefolder) is missing\n')
    parser.print_help()
    exit(-1)

data = dict()

exp_num = 1
for load in [1.0, 0.1, 0]:
    for selectivity in [1.0, 0.1, 0.01]:
        for thread in range(0, 3):
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

data['experiment_number'] = "1"
# data['repetition'] = "1"
# data['repetitions_per_experiment'] = "1"
data['duration'] = "300"
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
data['scriptsfolder'] = options.scriptsfolder
data['header'] = options.header
data['body'] = options.body
data['script'] = options.script
data['runner'] = options.runner

exp_id = data['exp_' + data['experiment_number'] + '_rep'] + '_' + data[
    'exp_' + data['experiment_number'] + '_spout_parallelism'] + '_' + data[
             'exp_' + data['experiment_number'] + '_op_parallelism'] + '_' + data[
             'exp_' + data['experiment_number'] + '_sink_parallelism'] + '_' + data[
             'exp_' + data['experiment_number'] + '_selectivity'] + '_' + data[
             'exp_' + data['experiment_number'] + '_load'] + '_NonDeterministicStorm'
exp_id = exp_id.replace('.', '-')

command = 'usecases.debs2015.MergerTestNonDeterministic false true \$LOGDIR \$kill_id ' + str(
    data['duration']) + ' ' + str(
    data['exp_' + data['experiment_number'] + '_spout_parallelism']) + ' ' + str(
    data['exp_' + data['experiment_number'] + '_op_parallelism']) + ' ' + str(
    data['exp_' + data['experiment_number'] + '_sink_parallelism']) + ' ' + str(
    data['exp_' + data['experiment_number'] + '_selectivity']) + ' ' + str(
    data['exp_' + data['experiment_number'] + '_load']) + ' 1'

data['exp_' + data['experiment_number'] + '_id'] = exp_id
data['exp_' + data['experiment_number'] + '_command'] = command

json.dump(data, open(options.statefolder + '/state.json', 'w'))
create_script_and_schedule_job(options.scriptsfolder, options.header, options.body,
                               options.script, exp_id, command, options.runner)
