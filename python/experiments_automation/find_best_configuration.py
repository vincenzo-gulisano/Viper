import analyze_topology_results
import os
import time
import csv
from optparse import OptionParser

def run_exp(stats_folder, jar, main, id_prefix, duration, repetitions, operators, instances):
    for r in range(0, repetitions):

        exp_id = str(r) + '_'
        for o in operators:
            exp_id += str(instances[o]) + '_'
        exp_id += id_prefix

        command = '/home/vincenzo/storm/apache-storm-0.9.5/bin/storm jar ' + jar + ' ' + main + ' false true ' + \
                  stats_folder + ' ' + exp_id + ' ' + str(duration) + ' '
        for o in operators:
            command += str(instances[o]) + ' '

        print('Executing command ' + command)

        os.system(command)
        time.sleep(duration + 60)
        print('Killing topology')
        os.system('/home/vincenzo/storm/apache-storm-0.9.5/bin/storm kill ' + exp_id)
        time.sleep(10)

    return


def find_most_expensive_op(stats_folder, jar, main, id_prefix, duration, repetitions, operators, instances):
    # run_exp(stats_folder, jar, main, id_prefix, duration, repetitions, operators, instances)

    [throughput, latency, cost] = \
        analyze_topology_results.analyze_topology_results(operators, instances, duration, repetitions, stats_folder,
                                                          id_prefix)

    highest_cost_op = operators[cost.index(max(cost))]
    print('Operator with highest cost is ' + highest_cost_op)
    instances[highest_cost_op] += 1

    with open(id_prefix+'.csv', 'a') as csvfile:

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

    return instances

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

(options, args) = parser.parse_args()


stats_folder = '/home/vincenzo/storm_experiments/understanding_storm/results/'
jar = '/home/vincenzo/Viper/target/Viper-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
main = 'usecases.debs2015.MergerTestNonDeterministic'
id_prefix = 'mtnd'
duration = 60
repetitions = 1

operators = ['spout', 'op', 'sink']
instances = {'spout': 1, 'op': 1, 'sink': 1}


find_most_expensive_op('/Users/vinmas/repositories/viper_experiments/internalqueues/', '', '', 'mtnd', 60, 1, operators, instances)


available_threads = 10

instances = find_most_expensive_op(stats_folder, jar, main, id_prefix, duration, repetitions, operators, instances)
available_threads -= 1

while available_threads > 0:
    instances = find_most_expensive_op(stats_folder, jar, main, id_prefix, duration, repetitions, operators, instances)
    available_threads -= 1
