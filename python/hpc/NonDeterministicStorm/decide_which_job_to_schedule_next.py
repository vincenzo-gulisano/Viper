__author__ = 'vinmas'
import json
import os


def schedule_job(repetition, spout_parallelism, op_parallelism, sink_parallelism, selectivity, load, duration):
    # Cat the head part of the file
    os.system('cat head.sh > pyjob.sh')

    #  Create the name of the job
    exp_id = str(repetition) + '_' + str(spout_parallelism) + '_' + str(op_parallelism) + '_' + str(
        sink_parallelism) + '_' + str(selectivity) + '_' + str(load) + '_NonDeterministicStorm'
    exp_id = exp_id.replace('.', '-')

    # add the lines that need to be added
    os.system('echo kill_id="' + exp_id + '"`date +%Y%m%d%H%M` >> pyjob.sh')
    os.system('echo command=usecases.debs2015.MergerTestNonDeterministic false true $LOGDIR $kill_id"' + str(
        duration) + ' ' + str(spout_parallelism) + ' ' + str(op_parallelism) + ' ' + str(sink_parallelism) + ' ' + str(
        selectivity) + ' ' + str(load) + ' 1" >> pyjob.sh')

    # Cat the body part of the file
    os.system('cat tail.sh >> pyjob.sh')

    return


def decide_which_job_to_schedule_next():
    # Read parameters
    parameters = json.load(open("params.txt"))

    # Decide which experiment to run next
    if int(parameters['experiment_number']) == 0:
        parameters['experiment_number'] = 1
        json.dump(parameters, open("params.txt", 'w'))
        schedule_job(0, 1, 1, 1, 1, 0, 300)
    else:
        parameters['experiment_number'] = int(parameters['experiment_number']) + 1
        parameters['remaining_threads_to_assign'] = int(parameters['remaining_threads_to_assign']) - 1
        json.dump(parameters, open("params.txt", 'w'))
        # if parameters['remaining_threads_to_assign'] > 0:
        #
        #     # Find the most expensive operator, schedule the next job
        #     # TODO
        #
        #     schedule_job(2, 1, 1)


parameters = {'experiment_number': 0, 'remaining_threads_to_assign': 10}
json.dump(parameters, open("params.txt", 'w'))
decide_which_job_to_schedule_next()
