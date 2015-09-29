__author__ = 'vinmas'
from optparse import OptionParser
import csv
import os
import time
import analyze_topology_results


def run_exp(stats_folder, jar, main, id_prefix, duration, repetitions, operators, instances, selectivity, workers):
    for r in range(0, repetitions):

        exp_id = str(r) + '_'
        for o in operators:
            exp_id += str(instances[o]) + '_'
        exp_id += str(selectivity) + '_' + str(workers) + '_' + id_prefix

        command = '/home/vincenzo/storm/apache-storm-0.9.5/bin/storm jar ' + jar + ' ' + main + ' false true ' + \
                  stats_folder + ' ' + exp_id.replace('.', '-') + ' ' + str(duration) + ' '
        for o in operators:
            command += str(instances[o]) + ' '
        command += str(selectivity) + ' ' + str(workers)

        print('\n\nExecuting command ' + command)

        os.system(command)
        time.sleep(duration + 60)
        print('Killing topology')
        os.system('/home/vincenzo/storm/apache-storm-0.9.5/bin/storm kill ' + exp_id.replace('.', '-'))
        time.sleep(10)

    return


def find_most_expensive_op(stats_folder, jar, main, id_prefix, duration, repetitions, operators, instances,
                           selectivity, workers):

    suffix = str(selectivity) + '_' + str(workers) + '_'

    run_exp(stats_folder, jar, main, id_prefix, duration, repetitions, operators, instances, selectivity, workers)

    [throughput, latency, cost] = \
        analyze_topology_results.analyze_topology_results(operators, instances, duration, repetitions, stats_folder,
                                                          suffix + id_prefix)

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

    with open(stats_folder + id_prefix + suffix + '.csv', 'a') as csvfile:
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


###############################

parser = OptionParser()
parser.add_option("-s", "--statsfolder", dest="stats_folder",
                  help="folder to which experiment results are written", metavar="STATSFOLDER")
parser.add_option("-j", "--jar", dest="jar",
                  help="Jar to submit to Storm", metavar="JAR")
parser.add_option("-m", "--main", dest="main",
                  help="Mina class in jar", metavar="MAIN")
parser.add_option("-i", "--id", dest="id",
                  help="id prefix for the experiment", metavar="ID")
parser.add_option("-d", "--duration", dest="duration",
                  help="experiment duration", metavar="DURATION")
parser.add_option("-r", "--repetitions", dest="repetitions",
                  help="experiment repetitions", metavar="REPETITIONS")
parser.add_option("-x", "--selectivity", dest="selectivity",
                  help="selectivity for operator", metavar="SELECTIVITY")
parser.add_option("-w", "--workers", dest="workers",
                  help="number of workers", metavar="WORKERS")
parser.add_option("-t", "--threads", dest="threads",
                  help="available threads", metavar="THREADS")

(options, args) = parser.parse_args()

operators = ['spout', 'op', 'sink']
instances = {'spout': 1, 'op': 1, 'sink': 1}

available_threads = int(options.threads)

suffix = '_' + str(options.selectivity) + '_' + str(options.workers)

with open(options.stats_folder + options.id + suffix.replace('.', '-') + '.csv', 'w') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    row = []
    for o in operators:
        row.append(o + '_instances')
        row.append(o + '_throughput')
        row.append(o + '_latency')
        row.append(o + '_cost')
    writer.writerow(row)

instances = find_most_expensive_op(options.stats_folder, options.jar, options.main, options.id, int(options.duration),
                                   int(options.repetitions), operators, instances, float(options.selectivity),
                                   int(options.workers))
available_threads -= 1

while available_threads > 0:
    instances = find_most_expensive_op(options.stats_folder, options.jar, options.main, options.id,
                                       int(options.duration),
                                       int(options.repetitions), operators, instances, float(options.selectivity),
                                       int(options.workers))
    available_threads -= 1

# create_graphs(options.stats_folder, options.id, float(options.selectivity), operators, 'spout', 'sink')
