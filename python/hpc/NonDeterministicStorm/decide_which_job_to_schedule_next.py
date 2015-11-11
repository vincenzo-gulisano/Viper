__author__ = 'vinmas'
import json
from scipy import stats as scipystat


def decide_which_job_to_schedule_next(results_file, state_file, operators, instances):

    # Read the results json file and check to which operator the thread should be assigned

    results_json = json.load(open(results_file, 'r'))

    state_json = json.load(open(state_file, 'r'))

    start_ts = state_json['duration'] * 0.1
    end_ts = state_json['duration'] * 0.9

    # Compute average throughput, latency and cost

    throughput = []
    latency = []
    cost = []
    print('')
    for o in operators:

        throughput.append(scipystat.trim_mean(results_json[o + "_rate_value"][start_ts:end_ts], 0.05))
        if len(scipystat.trim_mean(results_json[o + "_latency_value"][start_ts:end_ts], 0.05)) > 0:
            latency.append(scipystat.trim_mean(results_json[o + "_latency_value"][start_ts:end_ts], 0.05))
        else:
            latency.append(0.0)
        cost.append(scipystat.trim_mean(results_json[o + "_cost_value"][start_ts:end_ts], 0.05) * scipystat.trim_mean(
            results_json[o + "_invocations_value"][start_ts:end_ts], 0.05) / instances[o] / pow(10, 9))

        print(o + ': T ' + str(throughput[-1]) + ' L ' + str(latency[-1])
              + ' C ' + str(cost[-1]))

    print('')

    return
