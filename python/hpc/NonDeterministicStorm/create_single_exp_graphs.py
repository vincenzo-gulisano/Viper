__author__ = 'vinmas'
import json
import matplotlib

from scipy import stats as scipystat

matplotlib.use('Agg')
from matplotlib import rcParams
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import csv


def create_graph_time_value(x, y, title, x_label, y_label, outFile):
    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(outFile)

    f = plt.figure()
    ax = plt.gca()

    plt.plot(x, y)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True)
    plt.close()

    pp.savefig(f)
    pp.close()

    return

def create_graph_multiple_time_value(xs, ys, title, x_label, y_label, outFile):
    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(outFile)

    f = plt.figure()
    ax = plt.gca()

    for key in xs.keys():
        plt.plot(xs[key], ys[key], label=key)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True)
    plt.legend(loc='upper left')
    plt.close()

    pp.savefig(f)
    pp.close()

    return

def create_single_exp_graphs(state_folder, results_folder, energy_file, spout_parallelim, op_parallelism,
                             sink_parallelism):
    # state_folder = '/Users/vinmas/repositories/viper_experiments/151130/'

    state = json.load(open(state_folder + 'state.json', 'r'))
    start_ts = int(int(state['duration']) * 0.1)
    end_ts = int(int(state['duration']) * 0.9)

    # results_folder = '/Users/vinmas/repositories/viper_experiments/151130/ImprovedParallelStorm_2015-11-24_15.26/'

    results = json.load(open(results_folder + 'exp_result.json', 'r'))

    earliest_ts = results['spout_rate_ts'][0]

    # SPOUT
    results['spout_rate_ts'][:] = [x - earliest_ts for x in results['spout_rate_ts']]
    create_graph_time_value(results['spout_rate_ts'][start_ts:end_ts], results['spout_rate_value'][start_ts:end_ts],
                            'Spout throughput', 'time (seconds)', 'throughput (t/s)',
                            results_folder + 'spout.throughput.pdf')

    spout_cost_values = [
        results['spout_cost_value'][i] * results['spout_invocations_value'][i] / spout_parallelim / pow(10, 9) for i in
        range(start_ts, end_ts)]

    results['spout_cost_ts'][:] = [x - earliest_ts for x in results['spout_cost_ts']]
    create_graph_time_value(results['spout_cost_ts'][start_ts:end_ts], spout_cost_values, 'Spout cost',
                            'time (seconds)',
                            'cost', results_folder + 'spout.cost.pdf')

    # OPERATOR

    results['op_rate_ts'][:] = [x - earliest_ts for x in results['op_rate_ts']]
    create_graph_time_value(results['op_rate_ts'][start_ts:end_ts], results['op_rate_value'][start_ts:end_ts],
                            'Operator throughput', 'time (seconds)', 'throughput (t/s)',
                            results_folder + 'op.throughput.pdf')

    operator_cost_values = [
        results['op_cost_value'][i] * results['op_invocations_value'][i] / op_parallelism / pow(10, 9) for i in
        range(start_ts, end_ts)]

    results['op_cost_ts'][:] = [x - earliest_ts for x in results['op_cost_ts']]
    create_graph_time_value(results['op_cost_ts'][start_ts:end_ts], operator_cost_values, 'Operator cost',
                            'time (seconds)',
                            'cost', results_folder + 'op.cost.pdf')

    # SINK

    sink_cost_values = [
        results['sink_cost_value'][i] * results['sink_invocations_value'][i] / sink_parallelism / pow(10, 9) for i in
        range(start_ts, end_ts)]

    results['sink_cost_ts'][:] = [x - earliest_ts for x in results['sink_cost_ts']]
    create_graph_time_value(results['sink_cost_ts'][start_ts:end_ts], sink_cost_values, 'Sink cost', 'time (seconds)',
                            'cost', results_folder + 'sink.cost.pdf')

    results['sink_latency_ts'][:] = [x - earliest_ts for x in results['sink_latency_ts']]
    create_graph_time_value(results['sink_latency_ts'][start_ts:end_ts], results['sink_latency_value'][start_ts:end_ts],
                            'Sink latency', 'time (seconds)',
                            'latency ', results_folder + 'sink.latency.pdf')

    consumption_ts = []
    consumption_value = []
    # watts_socket1 = []

    min_ts = int(results['spout_rate_ts'][1])
    max_ts = int(results['spout_rate_ts'][-1])

    this_row = 1
    prev_ts = -1
    # prev_ts_int = -1
    prev_value = 0
    first_value = True
    with open(results_folder + energy_file, 'r') as inf:
        incsv = csv.reader(inf)
        for row in incsv:
            if this_row >= 2:
                this_row_ts = float(row[0]) - earliest_ts
                this_row_ts_int = int(this_row_ts)
                if this_row_ts_int >= min_ts and this_row_ts_int <= max_ts:
                    if first_value:
                        first_value = False
                        consumption_ts.append(this_row_ts)
                        consumption_value.append(0)
                    elif this_row_ts == prev_ts:
                        this_value = ((float(row[1]) + float(row[2])) - prev_value) / (this_row_ts - prev_ts)
                        consumption_value[-1] += this_value
                    else:
                        consumption_ts.append(this_row_ts)
                        this_value = ((float(row[1]) + float(row[2])) - prev_value) / (this_row_ts - prev_ts)
                        consumption_value.append(this_value)
                    prev_ts = this_row_ts
                    # prev_ts_int = this_row_ts_int
                    prev_value = float(row[1]) + float(row[2])
            this_row += 1

    # consumption_ts[:] = [x - earliest_ts for x in consumption_ts]
    consumption_start_ts = int(consumption_ts[-1] * 0.1)
    consumption_end_ts = int(consumption_ts[-1] * 0.9)
    consumption_start_ts_index = [ n for n,i in enumerate(consumption_ts) if i>consumption_start_ts ][0]
    consumption_end_ts_index = [ n for n,i in enumerate(consumption_ts) if i<consumption_end_ts ][-1]
    create_graph_time_value(consumption_ts[consumption_start_ts_index:consumption_end_ts_index],
                            consumption_value[consumption_start_ts_index:consumption_end_ts_index], 'Consumption', 'time (seconds)',
                            'Consumption (watts) ', results_folder + 'consumption.pdf')

    throughput = scipystat.trim_mean(results['spout_rate_value'][start_ts:end_ts], 0.05)
    latency = scipystat.trim_mean(results['sink_latency_value'][start_ts:end_ts], 0.05)
    consumption = scipystat.trim_mean(consumption_value[start_ts:end_ts], 0.05)

    return [throughput, latency, consumption]
