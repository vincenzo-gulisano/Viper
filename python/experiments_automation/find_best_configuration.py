import analyze_topology_results

stats_folder = '/home/vincenzo/storm_experiments/understanding_storm/results/'
jar = '/home/vincenzo/Viper/target/Viper-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
main = 'usecases.debs2015.MergerTestNonDeterministic'
id_prefix = 'mtndiq'
duration = 120
repetitions = 1

operators = ['spout', 'op', 'sink']
instances = {'spout': 1, 'op': 1, 'sink': 1}

available_threads = 10

for r in range(0, repetitions):

    id = str(r) + '_'
    for o in operators:
        id += str(instances[o]) + '_'
    id += id_prefix

    command = '/home/vincenzo/storm/apache-storm-0.9.5/bin/storm jar ' + jar + ' ' + main + ' false true ' + \
              stats_folder + ' ' + id + ' ' + str(duration) + ' '
    for o in operators:
        command += str(instances[o]) + ' '

    print('Executing command ' + command)

    # os.system(command)
    # time.sleep(duration + 60)
    # print('Killing topology')
    # os.system('/home/vincenzo/storm/apache-storm-0.9.5/bin/storm kill ' + id)
    # time.sleep(10)

[throughput, latency, cost, invocations] = \
    analyze_topology_results.analyze_topology_results(operators, instances, duration, repetitions, stats_folder, id_prefix)

highest_cost_op = operators[cost.index(max(cost))]
print('Operator with highest cost is ' + highest_cost_op)
instances[highest_cost_op] += 1
available_threads -= 1


# for it in range(0, available_threads)
