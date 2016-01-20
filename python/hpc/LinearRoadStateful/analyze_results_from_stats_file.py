import json
from LinearRoad.create_single_exp_graphs import create_graph_multiple_time_value
from LinearRoad.create_single_exp_graphs import create_bar_plot
import numpy
import plotly.graph_objs as go

results_base_folder = '/Users/vinmas/repositories/viper_experiments/linear_road/hpc_results/summary/160118'

statistics_y_labels = dict()
statistics_y_labels['throughput'] = 'Throughput (t/s)'
statistics_y_labels['latency'] = 'Latency (ms)'
statistics_y_labels['consumption'] = 'Consumption (W/t)'

# This is to draw the bar plots
for statistic in ['throughput', 'latency', 'consumption']:

    traces = []

    for type in ['storm', 'viper']:

        threads = dict()
        this_stat = dict()

        for main_class in ['LRStateless', 'LRStatelessConvertingToo', 'LRStatelessConvertingFilteringToo']:

            for exp_rep in range(0, 3):

                id = main_class + '_' + type

                stats_data = json.load(
                    open(results_base_folder + '/' + str(exp_rep) + '_' + type + '.json', 'r'))

                threads['rep_' + str(exp_rep)] = []
                this_stat['rep_' + str(exp_rep)] = []

                # Notice that repetition is hard-coded!!!
                for thread in range(0, 21):
                    spout_parallelism = stats_data[
                        id + '_thread_' + str(thread) + '_repetition_0_spout_parallelism']
                    op_parallelism = stats_data[id + '_thread_' + str(thread) + '_repetition_0_op_parallelism']
                    sink_parallelism = stats_data[id + '_thread_' + str(thread) + '_repetition_0_sink_parallelism']
                    stat = stats_data[id + '_thread_' + str(thread) + '_repetition_0_' + statistic]

                    threads['rep_' + str(exp_rep)].append(spout_parallelism + op_parallelism + sink_parallelism)
                    this_stat['rep_' + str(exp_rep)].append(stat)


            # Now computing averages for all runs
            threads[type + '_avg'] = []
            this_stat[type + '_avg'] = []

            for thread in range(0, 21):
                # Taking the same number of threads as rep_0 (they should alwyas be the same, in any case
                threads[type + '_avg'].append(threads['rep_0'][thread])
                temp = []
                for exp_rep in range(0, 3):
                    temp.append(this_stat['rep_' + str(exp_rep)][thread])
                this_stat[type + '_avg'].append(numpy.mean(temp))

        trace1 = go.Bar(
            x=['LRStateless', 'LRStatelessConvertingToo', 'LRStatelessConvertingFilteringToo'],
            y=[20, 14, 23],
            name=type
        )

        trace2 = go.Bar(
            x=['giraffes', 'orangutans', 'monkeys'],
            y=[12, 18, 29],
            name='LA Zoo'
        )
    create_bar_plot([trace1, trace2],results_base_folder + '/barplot.pdf')