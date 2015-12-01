import json
from NonDeterministicStorm.create_single_exp_graphs import create_single_exp_graphs
from NonDeterministicStorm.create_single_exp_graphs import create_graph_time_value
from os import listdir
from os.path import isfile, join

state_folder = '/Users/vinmas/repositories/viper_experiments/151201/'
results_base_folder = '/Users/vinmas/repositories/viper_experiments/151201'

throughput_avg = []
latency_avg = []
consumption_avg = []

state = json.load(open(state_folder + 'state.json', 'r'))

for exp_num in range(1, 11):
    result_path = state[str(exp_num) + '_results_folder']
    result_path = result_path.split('/')[-2]
    exp_id = state[str(exp_num) + '_exp_id']
    spout_parallelism = int(exp_id.split('_')[1])
    op_parallelism = int(exp_id.split('_')[2])
    sink_parallelism = int(exp_id.split('_')[3])
    results_folder = results_base_folder + '/' + result_path + '/'
    onlyfiles = [f for f in listdir(results_folder) if isfile(join(results_folder, f)) and 'RAPL' in f]
    print('Analyzing result folder ' + results_folder)
    [throughput, latency, consumption] = create_single_exp_graphs(state_folder, results_folder, onlyfiles[0],
                                                                  spout_parallelism, op_parallelism, sink_parallelism)

    throughput_avg.append(throughput)
    latency_avg.append(latency)
    consumption_avg.append(consumption)

create_graph_time_value(range(1, 11), throughput_avg, 'Throughput', 'Threads', 'Throughput (t/s)',
                        results_base_folder + '/throughput.pdf')
create_graph_time_value(range(1, 11), latency_avg, 'Latency', 'Threads', 'Latency (?)',
                        results_base_folder + '/latency.pdf')
create_graph_time_value(range(1, 11), consumption_avg, 'Consumption', 'Threads', 'Consumption (?)',
                        results_base_folder + '/consumption.pdf')
