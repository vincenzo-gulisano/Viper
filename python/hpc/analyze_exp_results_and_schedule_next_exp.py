__author__ = 'vinmas'
from optparse import OptionParser
import csv
from find_most_expensive_op_and_propose_next_exp import find_most_expensive_op
import read_topology_data_and_create_graphs
import os
import time
import find_most_expensive_op_and_propose_next_exp
import sys


# Read the configuration


parser = OptionParser()
parser.add_option("-f", "--folder", dest="folder",
                  help="folder to which experiment results are written", metavar="STATSFOLDER")
parser.add_option("-i", "--id", dest="id",
                  help="id of the experiment", metavar="JAR")

(options, args) = parser.parse_args()

with open(options.folder + options.id + '_configuration.txt', newline='') as c:
    reader_c = csv.reader(c, delimiter=',', quoting=csv.QUOTE_NONE)

    operators = reader_c.__next__()
    operators_instances_str = reader_c.__next__()
    operators_instances = [int(i) for i in operators_instances_str]
    instances = {}
    for i in range(0, len(operators)):
        instances[operators[i]] = int(operators_instances[i])


    # Find the most expensive operator (Optionally, create graphs in the meantime)
    find_most_expensive_op(options.folder, options.id, 60, 1, operators, instances,
                               selectivity, load, workers)
    # print(operators)
    # print(operators_instances)
    # print(instances)

    # Create the graphs
