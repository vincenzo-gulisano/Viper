__author__ = 'vinmas'
from NonDeterministicStorm.create_json_for_experiment_results import read_topology_parallel_op_data_and_store_json

# data = dict()
# data['experiment_number'] = 1
# data['repetition'] = 1
# data['repetitions_per_experiment'] = 1
# data['duration'] = 300
# data['spout_parallelism'] = 1
# data['op_parallelism'] = 1
# data['sink_parallelism'] = 1
# data['available_threads'] = 10
# data['assigned_threads'] = 0
# data['max_selectivity'] = 1.0
# data['min_selectivity'] = 0.1
# data['selectivity_step'] = 0.1
# data['selectivity'] = 1.0
# data['max_load'] = 1.0
# data['min_load'] = 0.0
# data['load_step'] = 0.1
# data['load'] = 1.0
#
# json.dump(data, open('/Users/vinmas/Downloads/ImprovedParallelStorm_2015-11-06_17.49/state.json', 'w'))

read_topology_parallel_op_data_and_store_json(
    '/Users/vinmas/Downloads/ImprovedParallelStorm_2015-11-06_17.49/0_1_1_1_1_0_NonDeterministicStorm201511061749_', ['spout', 'op', 'sink'], [1, 1, 1],
    '/Users/vinmas/Downloads/ImprovedParallelStorm_2015-11-06_17.49/exp_result.json')
