import os
import time

stats_folder = '/home/vincenzo/storm_experiments/understanding_storm/results/'
jar = '/home/vincenzo/Viper/target/Viper-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
main = 'usecases.debs2015.MergerTestNonDeterministic'
id_prefix = 'mtndiq'
duration = 120
repetitions = 1

operators = ['spout', 'op', 'sink']
instances = {'spout': 1, 'op': 1, 'sink': 1}

available_threads = 10

# Run experiments with initial configurations

# boolean local = Boolean.valueOf(args[0]);
# boolean logStats = Boolean.valueOf(args[1]);
# String statsPath = args[2];
# String topologyName = args[3];
# final long duration = Long.valueOf(args[4]);
# final int spout_parallelism = Integer.valueOf(args[5]);
# final int op_parallelism = Integer.valueOf(args[6]);
# final int sink_parallelism = Integer.valueOf(args[7]);

for r in range(0, repetitions):

    id = str(r) + '_'
    for o in operators:
        id += o + '_'
    id += id_prefix

    command = '/home/vincenzo/storm/apache-storm-0.9.5/bin/storm jar ' + jar + ' ' + main + ' false true ' + \
              stats_folder + ' ' + id + ' ' + str(duration) + ' '
    for o in operators:
        command += str(instances[o]) + ' '

    print('Executing command ' + command)

    os.system(command)
    time.sleep(duration + 60)
    print('Killing topology')
    os.system('/home/vincenzo/storm/apache-storm-0.9.5/bin/storm kill ' + id)
    time.sleep(10)

# for it in range(0, available_threads)
