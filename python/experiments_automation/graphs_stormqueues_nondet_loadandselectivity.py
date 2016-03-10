__author__ = 'vinmas'

from find_best_configuration import create_graphs
from collections import defaultdict
import matplotlib

matplotlib.use('Agg')
from matplotlib import rcParams
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt


def create_graph_x_vs_dictionary_of_y(x, ys, title, x_label, y_label, outFile):
    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(outFile)

    f = plt.figure()
    ax = plt.gca()

    for key in ys:
        plt.plot(x, ys[key], label=key)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True)
    plt.legend()
    plt.close()

    pp.savefig(f)
    pp.close()

    return


base_folder = '/Users/vinmas/repositories/viper_experiments/internalqueues/'
id = 'mtnd'

max_throughput_load = defaultdict(list)
max_throughput_selectivity = defaultdict(list)
loads = [0.0, 0.01, 0.05, 0.1]
selectivities = [0.1, 0.25, 0.5, 0.75, 1.0]
for l in loads:
    for s in selectivities:
        # the id means mtnd_0-1_0-0_1 selectivity 0.1, load 0.0 and 1 worker
        [threads, data] = create_graphs(
            base_folder + 'mtnd_' + str(s).replace('.', '-') + '_' + str(l).replace('.', '-') + '_1.csv',
            ['spout', 'op', 'sink'], 'spout', 'sink')
        max_throughput_load['Load '+ str(l)].append(max(data['spout_throughput']))
        max_throughput_selectivity['Selectivity '+ str(s)].append(max(data['spout_throughput']))

# TODO Fix the order in which keys should be accessed

create_graph_x_vs_dictionary_of_y(selectivities, max_throughput_load, 'Throughput for increasing load', 'Selectivity',
                                  'Throughput',
                                  '/Users/vinmas/repositories/viper_experiments/internalqueues/selectivity_vs_throughput_different_loads.pdf')

create_graph_x_vs_dictionary_of_y(loads, max_throughput_selectivity, 'Throughput for increasing selectivity', 'Load',
                                  'Throughput',
                                  '/Users/vinmas/repositories/viper_experiments/internalqueues/load_vs_throughput_different_selectivities.pdf')
