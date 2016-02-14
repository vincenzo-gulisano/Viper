__author__ = 'vinmas'
import json
import matplotlib

from scipy import stats as scipystat

matplotlib.use('Agg')
from matplotlib import rcParams
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import csv
import statistics


# import plotly.plotly as py
# import plotly.graph_objs as go


# def create_bar_plot(traces, outFile):
#
#     layout = go.Layout(
#         barmode='group'
#     )
#
#     fig = go.Figure(data=traces, layout=layout)
#     plot_url = py.plot(fig, filename='grouped-bar')
#
#     py.image.save_as({'data': traces}, outFile)
#
#
#     return


def create_graph_time_value(x, y, title, x_label, y_label, outFile):
    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(outFile)

    f = plt.figure()
    ax = plt.gca()

    plt.plot(x, y)

    step = 20
    counter = 1
    # text_file = open(
    #         "/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/stateful/storm_stoppedcarsonly/summaries.csv",
    #         "a")
    # text_file.write(title)
    for i in range(int(x[0]) + step, int(x[-1]), step):
        plt.plot([i, i], [min(y), max(y)], color='r')
        indexes = [index for index, value in enumerate(x) if value >= i - step and value < i]
        plt.text(i, min(y), str(counter) + '\n' + str(int(statistics.median(y[indexes[0]:indexes[-1]]))),
                 verticalalignment='bottom', horizontalalignment='right', fontsize=7)
        # text_file.write('\t' + str(counter) + ':' + str(int(statistics.median(y[indexes[0]:indexes[-1]]))))
        counter += 1
    # text_file.write('\n')
    # text_file.close()

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True)
    plt.close()

    pp.savefig(f)
    pp.close()

    return


def create_overview_graph(spout_rate_x, spout_rate_y, op_rate_x, op_rate_y, sink_latency_x, sink_latency_y,
                          cons_x, cons_y, outFile):
    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(outFile)

    f = plt.figure()
    f.set_size_inches(20, 10)

    plt.subplot(2, 3, 1)
    plt.plot(spout_rate_x, spout_rate_y)
    plt.xlabel('time (seconds)')
    plt.ylabel('throughput (t/s)')
    plt.title('Spout throughput')
    plt.grid(True)

    step = 10

    # Here I am assuming all x lists have the same size!!!
    text_values = [['' for x in range(3)] for x in range(int(spout_rate_x[0]) + step, int(spout_rate_x[-1]), step)]
    int_values = [[-1 for x in range(3)] for x in range(int(spout_rate_x[0]) + step, int(spout_rate_x[-1]), step)]

    counter = 0
    medians_throughput_x = []
    medians_throughput_y = []
    for i in range(int(spout_rate_x[0]) + step, int(spout_rate_x[-1]), step):
        plt.plot([i, i], [min(spout_rate_y), max(spout_rate_y)], color='r')
        indexes = [index for index, value in enumerate(spout_rate_x) if value >= i - step and value < i]
        int_values[counter][0] = int(statistics.median(spout_rate_y[indexes[0]:indexes[-1]]))
        text_values[counter][0] = str(int_values[counter][0])
        medians_throughput_x.append(i - step / 2)
        medians_throughput_y.append(int(statistics.median(spout_rate_y[indexes[0]:indexes[-1]])))
        plt.text(i, min(spout_rate_y), str(counter), verticalalignment='bottom', horizontalalignment='center',
                 fontsize=7)
        counter += 1
    plt.plot(medians_throughput_x, medians_throughput_y, color='k')

    plt.subplot(2, 3, 2)
    plt.plot(op_rate_x, op_rate_y)
    plt.xlabel('time (seconds)')
    plt.ylabel('throughput (t/s)')
    plt.title('Op throughput')
    plt.grid(True)

    counter = 0
    for i in range(int(op_rate_x[0]) + step, int(op_rate_x[-1]), step):
        plt.plot([i, i], [min(op_rate_y), max(op_rate_y)], color='r')
        plt.text(i, min(op_rate_y), str(counter), verticalalignment='bottom', horizontalalignment='center',
                 fontsize=7)
        counter += 1

    plt.subplot(2, 3, 4)
    plt.plot(sink_latency_x, sink_latency_y)
    plt.xlabel('time (seconds)')
    plt.ylabel('latency')
    plt.title('Sink latency')
    plt.grid(True)

    counter = 0
    medians_latency_x = []
    medians_latency_y = []
    for i in range(int(sink_latency_x[0]) + step, int(sink_latency_x[-1]), step):
        plt.plot([i, i], [min(sink_latency_y), max(sink_latency_y)], color='r')
        indexes = [index for index, value in enumerate(sink_latency_x) if value >= i - step and value < i]
        int_values[counter][1] = int(statistics.median(sink_latency_y[indexes[0]:indexes[-1]]))
        text_values[counter][1] = str(int_values[counter][1])
        medians_latency_x.append(i - step / 2)
        medians_latency_y.append(int(statistics.median(sink_latency_y[indexes[0]:indexes[-1]])))
        plt.text(i, min(sink_latency_y), str(counter), verticalalignment='bottom', horizontalalignment='center',
                 fontsize=7)
        counter += 1
    plt.plot(medians_latency_x, medians_latency_y, color='k')

    plt.subplot(2, 3, 5)
    plt.plot(cons_x, cons_y)
    plt.xlabel('time (seconds)')
    plt.ylabel('Consumption (watts/second)')
    plt.title('Consumption')
    plt.grid(True)

    counter = 0
    medians_cons_x = []
    medians_cons_y = []
    for i in range(int(cons_x[0]) + step, int(cons_x[-1]), step):
        plt.plot([i, i], [min(cons_y), max(cons_y)], color='r')
        indexes = [index for index, value in enumerate(cons_x) if value >= i - step and value < i]
        int_values[counter][2] = int(statistics.median(cons_y[indexes[0]:indexes[-1]]))
        text_values[counter][2] = str(int_values[counter][2])
        medians_cons_x.append(i - step / 2)
        medians_cons_y.append(int(statistics.median(cons_y[indexes[0]:indexes[-1]])))
        plt.text(i, min(cons_y), str(counter), verticalalignment='bottom', horizontalalignment='center',
                 fontsize=7)
        counter += 1
        if  counter >= len(int_values):
            break
    plt.plot(medians_cons_x, medians_cons_y, color='k')

    # FIND THE HIGHEST THROUGHPUT FOR LATENCY BELOW THRESHOLD
    threshold = 1000
    latency_indexes = [index for index, value in enumerate(medians_latency_y) if value <= threshold]
    if len(latency_indexes)>0:
        max_throughput = max([medians_throughput_y[i] for i in latency_indexes])
        max_throuhgput_positions = [i for i in latency_indexes if medians_throughput_y[i] == max_throughput]
    else:
        max_throuhgput_positions = []

    if len(max_throuhgput_positions) > 1:
        print('!!!! MORE THAN ONE MAX WAS FOUND !!!!')

    while len(max_throuhgput_positions) == 0:
        threshold += 100
        print('... increasing threshold to ' + str(threshold))

        if threshold > 2000:
            print('THRESHOLD EXCEEDED 2000. Killing the process!')
            exit(-1)

        latency_indexes = [index for index, value in enumerate(medians_latency_y) if value <= threshold]
        if len(latency_indexes)>0:
            max_throughput = max([medians_throughput_y[i] for i in latency_indexes])
            max_throuhgput_positions = [i for i in latency_indexes if medians_throughput_y[i] == max_throughput]
        else:
            max_throuhgput_positions = []

    ax = plt.subplot2grid((2, 3), (0, 2), rowspan=2)
    range_length = len(range(int(cons_x[0]) + step, int(cons_x[-1]), step))
    counter = 0
    for i in range(int(cons_x[0]) + step, int(cons_x[-1]), step):
        text_ = str(counter) + ': ' + str(text_values[counter])
        if counter in max_throuhgput_positions:
            text_ += ' ***'
        plt.text(0, 1 - counter * (1 / range_length), text_, verticalalignment='top', horizontalalignment='left',
                 fontsize=10)
        counter += 1
        if counter >= len(int_values):
            break

    plt.close()
    pp.savefig(f)
    pp.close()

    # Returning first highest throughput found (and relative latency and consumption)
    return int_values[max_throuhgput_positions[0]]


def create_graph_multiple_time_value(xs, ys, keys, title, x_label, y_label, outFile):
    markers = ['.', ',', 'o', 'v', '^', '<', '>', '1', '2', '3', '4', '8', 's', 'p', '*', 'h', 'H', '+', 'x', 'D', 'd',
               '|', '_']

    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(outFile)

    f = plt.figure()
    ax = plt.gca()

    marker_i = 0
    for key in keys:
        plt.plot(xs[key], ys[key], label=key, marker=markers[marker_i])
        marker_i = (marker_i + 1) % len(markers)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True)
    plt.legend(loc='best')

    ymin, ymax = plt.ylim()
    plt.ylim(0, ymax)

    plt.close()

    pp.savefig(f)
    pp.close()

    return


def create_single_exp_graphs(state_folder, results_folder, energy_file, spout_parallelim, op_parallelism,
                             sink_parallelism, savePDF):
    # state_folder = '/Users/vinmas/repositories/viper_experiments/151130/'

    state = json.load(open(state_folder + 'state.json', 'r'))
    start_ts = int(int(state['duration']) * 0.1)
    end_ts = int(int(state['duration']) * 0.9)

    # results_folder = '/Users/vinmas/repositories/viper_experiments/151130/ImprovedParallelStorm_2015-11-24_15.26/'

    results = json.load(open(results_folder + 'exp_result.json', 'r'))

    earliest_ts = results['spout_rate_ts'][0]

    # SPOUT
    results['spout_rate_ts'][:] = [x - earliest_ts for x in results['spout_rate_ts']]
    if savePDF:
        create_graph_time_value(results['spout_rate_ts'][start_ts:end_ts], results['spout_rate_value'][start_ts:end_ts],
                            'Spout throughput', 'time (seconds)', 'throughput (t/s)',
                            results_folder + 'spout.throughput.pdf')

    spout_cost_values = [
        results['spout_cost_value'][i] * results['spout_invocations_value'][i] / spout_parallelim / pow(10, 9) for i in
        range(start_ts, end_ts)]

    results['spout_cost_ts'][:] = [x - earliest_ts for x in results['spout_cost_ts']]
    if savePDF:
        create_graph_time_value(results['spout_cost_ts'][start_ts:end_ts], spout_cost_values, 'Spout cost',
                            'time (seconds)',
                            'cost', results_folder + 'spout.cost.pdf')

    # OPERATOR

    results['op_rate_ts'][:] = [x - earliest_ts for x in results['op_rate_ts']]
    if savePDF:
        create_graph_time_value(results['op_rate_ts'][start_ts:end_ts], results['op_rate_value'][start_ts:end_ts],
                            'Operator throughput', 'time (seconds)', 'throughput (t/s)',
                            results_folder + 'op.throughput.pdf')

    operator_cost_values = [
        results['op_cost_value'][i] * results['op_invocations_value'][i] / op_parallelism / pow(10, 9) for i in
        range(start_ts, end_ts)]

    results['op_cost_ts'][:] = [x - earliest_ts for x in results['op_cost_ts']]
    if savePDF:
        create_graph_time_value(results['op_cost_ts'][start_ts:end_ts], operator_cost_values, 'Operator cost',
                            'time (seconds)',
                            'cost', results_folder + 'op.cost.pdf')

    # SINK

    sink_cost_values = [
        results['sink_cost_value'][i] * results['sink_invocations_value'][i] / sink_parallelism / pow(10, 9) for i in
        range(start_ts, end_ts)]

    results['sink_cost_ts'][:] = [x - earliest_ts for x in results['sink_cost_ts']]
    if savePDF:
        create_graph_time_value(results['sink_cost_ts'][start_ts:end_ts], sink_cost_values, 'Sink cost', 'time (seconds)',
                            'cost', results_folder + 'sink.cost.pdf')

    results['sink_latency_ts'][:] = [x - earliest_ts for x in results['sink_latency_ts']]
    if savePDF:
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
    consumption_start_ts_index = [n for n, i in enumerate(consumption_ts) if i > consumption_start_ts][0]
    consumption_end_ts_index = [n for n, i in enumerate(consumption_ts) if i < consumption_end_ts][-1]
    if savePDF:
        create_graph_time_value(consumption_ts[consumption_start_ts_index:consumption_end_ts_index],
                            consumption_value[consumption_start_ts_index:consumption_end_ts_index], 'Consumption',
                            'time (seconds)',
                            'Consumption (watts/second) ', results_folder + 'consumption.pdf')

    throughput = scipystat.trim_mean(results['spout_rate_value'][start_ts:end_ts], 0.05)
    latency = scipystat.trim_mean(results['sink_latency_value'][start_ts:end_ts], 0.05)
    consumption = scipystat.trim_mean(consumption_value[start_ts:end_ts], 0.05) / throughput

    highest_throughput_stat = create_overview_graph(results['spout_rate_ts'][start_ts:end_ts], results['spout_rate_value'][start_ts:end_ts],
                          results['op_rate_ts'][start_ts:end_ts], results['op_rate_value'][start_ts:end_ts],
                          results['sink_latency_ts'][start_ts:end_ts], results['sink_latency_value'][start_ts:end_ts],
                          consumption_ts[consumption_start_ts_index:consumption_end_ts_index],
                          consumption_value[consumption_start_ts_index:consumption_end_ts_index],
                          results_folder + 'summary.pdf')

    # This is the average processing tuple cost (in nanoseconds)
    # op_cost = scipystat.trim_mean(results['op_cost_value'][start_ts:end_ts], 0.05)

    # This is the average selectivity
    # selectivity_tmp = [results['op_rate_value'][i] / results['spout_rate_value'][i] for i in range(start_ts, end_ts)]
    # selectivity = scipystat.trim_mean(selectivity_tmp[start_ts:end_ts], 0.05)

    return [throughput, latency, consumption, highest_throughput_stat]  # , op_cost, selectivity]
