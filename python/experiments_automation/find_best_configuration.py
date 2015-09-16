import analyze_topology_results
import os
import time
import csv
from optparse import OptionParser
from collections import defaultdict
import matplotlib

matplotlib.use('Agg')
from matplotlib import rcParams
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt


def run_exp(stats_folder, jar, main, id_prefix, duration, repetitions, operators, instances, selectivity):
    for r in range(0, repetitions):

        exp_id = str(r) + '_'
        for o in operators:
            exp_id += str(instances[o]) + '_'
        exp_id += str(selectivity) + '_' + id_prefix

        command = '/home/vincenzo/storm/apache-storm-0.9.5/bin/storm jar ' + jar + ' ' + main + ' false true ' + \
                  stats_folder + ' ' + exp_id.replace('.', '-') + ' ' + str(duration) + ' '
        for o in operators:
            command += str(instances[o]) + ' '
        command += str(selectivity)

        print('\n\nExecuting command ' + command)

        os.system(command)
        time.sleep(duration + 60)
        print('Killing topology')
        os.system('/home/vincenzo/storm/apache-storm-0.9.5/bin/storm kill ' + exp_id.replace('.', '-'))
        time.sleep(10)

    return


def find_most_expensive_op(stats_folder, jar, main, id_prefix, duration, repetitions, operators, instances,
                           selectivity):
    run_exp(stats_folder, jar, main, id_prefix, duration, repetitions, operators, instances, selectivity)

    [throughput, latency, cost] = \
        analyze_topology_results.analyze_topology_results(operators, instances, duration, repetitions, stats_folder,
                                                          id_prefix, selectivity)

    highest_cost_op = operators[cost.index(max(cost))]
    print('Operator with highest cost is ' + highest_cost_op + '\n\n')

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


def create_op_graph_time_value(x, y, title, x_label, y_label, outFile):
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


def create_graphs(stats_folder, id, selectivity, operators, spout_op, sink_op):
    columns = defaultdict(list)  # each value in each column is appended to a list

    with open(stats_folder + id + str(selectivity).replace('.', '-') + '.csv') as f:
        reader = csv.DictReader(f)  # read rows into a dictionary format
        for row in reader:  # read a row as {column1: value1, column2: value2,...}
            for (k, v) in row.items():  # go over each column name and value
                columns[k].append(v)  # append the value into the appropriate list
                # based on column name k

    threads = [0] * len(columns[spout_op + '_throughput'])
    for i in range(0, len(columns[spout_op + '_throughput'])):
        for o in operators:
            threads[i] += int(columns[o + '_instances'][i])
    create_op_graph_time_value(threads, columns[spout_op + '_throughput'], 'Throughput', 'Threads', 'Throughput (t/s)',
                               stats_folder + id + str(selectivity).replace('.', '-') + '_throughput.pdf')
    create_op_graph_time_value(threads, columns[sink_op + '_latency'], 'Latency', 'Threads', 'Latency (ms)',
                               stats_folder + id + str(selectivity).replace('.', '-') + '_latency.pdf')

    return


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
parser.add_option("-t", "--threads", dest="threads",
                  help="available threads", metavar="THREADS")

(options, args) = parser.parse_args()

# stats_folder = '/home/vincenzo/storm_experiments/understanding_storm/results/'
# jar = '/home/vincenzo/Viper/target/Viper-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
# main = 'usecases.debs2015.MergerTestNonDeterministic'
# id_prefix = 'mtnd'
# duration = 60
# repetitions = 1

operators = ['spout', 'op', 'sink']
instances = {'spout': 1, 'op': 1, 'sink': 1}

available_threads = int(options.threads)

with open(options.stats_folder + options.id + str(options.selectivity).replace('.', '-') + '.csv', 'w') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    row = []
    for o in operators:
        row.append(o + '_instances')
        row.append(o + '_throughput')
        row.append(o + '_latency')
        row.append(o + '_cost')
    writer.writerow(row)

instances = find_most_expensive_op(options.stats_folder, options.jar, options.main, options.id, int(options.duration),
                                   int(options.repetitions), operators, instances, float(options.selectivity))
available_threads -= 1

while available_threads > 0:
    instances = find_most_expensive_op(options.stats_folder, options.jar, options.main, options.id,
                                       int(options.duration),
                                       int(options.repetitions), operators, instances, float(options.selectivity))
    available_threads -= 1

create_graphs(options.stats_folder, options.id, float(options.selectivity), operators, 'spout', 'sink')
