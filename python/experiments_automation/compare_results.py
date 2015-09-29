__author__ = 'vinmas'

from find_best_configuration import create_comparison_graph
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-s", "--statsfolder", dest="stats_folder", help="folder to which experiment results are written",
                  metavar="STATSFOLDER")
parser.add_option("-a", "--id1", dest="id1",
                  help="id1 prefix for the experiment", metavar="ID1")
parser.add_option("-b", "--id2", dest="id2",
                  help="id2 prefix for the experiment", metavar="ID2")
parser.add_option("-x", "--selectivity", dest="selectivity",
                  help="selectivity for operator", metavar="SELECTIVITY")

(options, args) = parser.parse_args()

if options.stats_folder is None:
    print('A mandatory option (-s, --stats_folder) is missing\n')
    parser.print_help()
    exit(-1)
if options.id1 is None:
    print('A mandatory option (-u, --id1) is missing\n')
    parser.print_help()
    exit(-1)
if options.id2 is None:
    print('A mandatory option (-b, --id2) is missing\n')
    parser.print_help()
    exit(-1)
if options.selectivity is None:
    print('A mandatory option (-x, --selectivity) is missing\n')
    parser.print_help()
    exit(-1)

operators = ['spout', 'op', 'sink']
instances = {'spout': 1, 'op': 1, 'sink': 1}

create_comparison_graph(options.stats_folder, options.id1, options.id2, options.selectivity,
                        operators, 'spout', 'sink')
