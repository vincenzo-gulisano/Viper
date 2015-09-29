__author__ = 'vinmas'
import numpy as np
from optparse import OptionParser
from find_best_configuration import create_graph_multiple_time_value
from find_best_configuration import read_float_csv

parser = OptionParser()
parser.add_option("-s", "--statsfolder", dest="stats_folder", help="folder to which experiment results are written",
                  metavar="STATSFOLDER")

(options, args) = parser.parse_args()

if options.stats_folder is None:
    print('A mandatory option (-s, --stats_folder) is missing\n')
    parser.print_help()
    exit(-1)

operators = ['spout', 'op', 'sink']
instances = {'spout': 1, 'op': 1, 'sink': 1}

base_throughput = []
viper_throughput = []

for selectivity in np.arange(1, 0, -0.1):

    data1 = read_float_csv(options.stats_folder + 'mtnd' + str(selectivity).replace('.', '-') + '.csv')
    data2 = read_float_csv(options.stats_folder + 'mtndiq' + str(selectivity).replace('.', '-') + '.csv')

    base_throughput.append(max(data1['spout_throughput']))
    viper_throughput.append(max(data2['spout_throughput']))

create_graph_multiple_time_value({'Storm': np.arange(1, 0, -0.1), 'Viper': np.arange(1, 0, -0.1)},
                                 {'Storm': base_throughput, 'Viper': viper_throughput},
                                 'Throughput', 'Selectivity', 'Throughput (t/s)',
                                 options.stats_folder + '_selectivity_comparison.pdf')
