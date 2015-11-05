__author__ = 'vinmas'
from optparse import OptionParser
import csv
import read_topology_data
import os
import time
import analyze_topology_results
import sys


# Read the configuration


def find_most_expensive_op(stats_folder, id_prefix, duration, repetitions, operators, instances,
                           selectivity):

    #  TODO this seems to be duplicated logic!
    [throughput, latency, cost] = \
        analyze_topology_results.analyze_topology_results(operators, instances, duration, repetitions, stats_folder,
                                                          id_prefix, selectivity)

    threshold = 0.9
    operators_above_threshold = 0
    rightmost_operator_above_threshold = 0
    index = 0
    for c in cost:
        if c > threshold:
            operators_above_threshold += 1
            rightmost_operator_above_threshold = index
            print('Operator ' + operators[index] + ' is above threshold')
        index += 1

    highest_cost_op = operators[cost.index(max(cost))]
    print('Operator with highest cost is ' + highest_cost_op)
    if operators_above_threshold > 0:
        print('But, since there is at least one operator above threshold ' + str(threshold) + '...')
        highest_cost_op = operators[rightmost_operator_above_threshold]
        print('The thread goes to ' + highest_cost_op)

    print('\n\n')

    with open(stats_folder + id_prefix + str(selectivity).replace('.', '-') + '.csv', 'a') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')

        row = []
        index = 0
        for o in operators:
            row.append(str(instances[o]))
            row.append(str(throughput[index]))
            row.append(str(latency[index]))
            row.append(str(cost[index]))
            index += 1
        writer.writerow(row)

    instances[highest_cost_op] += 1

    return instances


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

    # Read the data and create the graphs
    # TODO Notice that this is optional, no need to to do this on the HPC machine directly, we can do it afterwards

    data = read_topology_data.read_topology_parallel_op_data(options.folder + options.id + '_', operators,
                                                             operators_instances)

    # Find the most expensive operator

    # print(operators)
    # print(operators_instances)
    # print(instances)

    # Create the graphs
