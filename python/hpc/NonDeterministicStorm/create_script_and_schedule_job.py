__author__ = 'vinmas'

import os


def create_script_and_schedule_job(scriptsfolder, header, body, script, id, command, runner):
    print('Running ' + 'cat ' + scriptsfolder + '/' + header + ' > ' + script)
    os.system('cat ' + scriptsfolder + '/' + header + ' > ' + script)

    print('Adding lines specific for the experiment')
    os.system('echo " " >> ' + script)
    os.system('echo kill_id=' + id + '`date +%Y%m%d%H%M` >> ' + script)
    os.system('echo command=\\"' + command + '\\" >> ' + script)
    os.system('echo KILLDURATION=365 >> ' + script)
    os.system('echo SAMPLEDURATION=365 >> ' + script)
    os.system('echo sleep_time=90 >> ' + script)
    os.system('echo " " >> ' + script)

    print('Running ' + 'cat ' + scriptsfolder + '/' + body + ' > ' + script)
    os.system('cat ' + scriptsfolder + '/' + body + ' > ' + script)

    print('Running ' + runner + script)
    os.system(runner + script)

    return


    # parser = OptionParser()
    # parser.add_option("-f", "--scriptsfolder", dest="scriptsfolder",
    #                   help="folder containing header and body parts of the scripts", metavar="FOLDER")
    # parser.add_option("-h", "--header", dest="header",
    #                   help="header part of the script", metavar="HEADER")
    # parser.add_option("-b", "--body", dest="body",
    #                   help="body part of the script", metavar="BODY")
    # parser.add_option("-s", "--script", dest="script",
    #                   help="name of the script to run", metavar="BODY")
    # parser.add_option("-r", "--runner", dest="runner",
    #                   help="run script to schedule a job", metavar="RUNNER")
    # parser.add_option("-i", "--id", dest="id",
    #                   help="id of the experiment", metavar="ID")
    # parser.add_option("-c", "--command", dest="command",
    #                   help="command to execute the experiment", metavar="COMMAND")
    #
    # (options, args) = parser.parse_args()
    #
    # if options.scriptsfolder is None:
    #     print('A mandatory option (-f, --scriptsfolder) is missing\n')
    #     parser.print_help()
    #     exit(-1)
    # if options.header is None:
    #     print('A mandatory option (-h, --header) is missing\n')
    #     parser.print_help()
    #     exit(-1)
    # if options.body is None:
    #     print('A mandatory option (-b, --body) is missing\n')
    #     parser.print_help()
    #     exit(-1)
    # if options.script is None:
    #     print('A mandatory option (-s, --script) is missing\n')
    #     parser.print_help()
    #     exit(-1)
    # if options.runner is None:
    #     print('A mandatory option (-r, --runner) is missing\n')
    #     parser.print_help()
    #     exit(-1)
    # if options.id is None:
    #     print('A mandatory option (-i, --id) is missing\n')
    #     parser.print_help()
    #     exit(-1)
    # if options.command is None:
    #     print('A mandatory option (-c, --command) is missing\n')
    #     parser.print_help()
    #     exit(-1)
    #
    # print('Running ' + 'cat ' + options.scriptsfolder + '/' + options.header + ' > ' + options.script)
    # os.system('cat ' + options.scriptsfolder + '/' + options.header + ' > ' + options.script)
    #
    # print('Adding lines specific for the experiment')
    # os.system('echo " " >> ' + options.script)
    # os.system('echo kill_id=' + options.id + '`date +%Y%m%d%H%M` >> ' + options.script)
    # os.system('echo command=\\"' + options.command + '\\" >> ' + options.script)
    # os.system('echo KILLDURATION=365 >> ' + options.script)
    # os.system('echo SAMPLEDURATION=365 >> ' + options.script)
    # os.system('echo sleep_time=90 >> ' + options.script)
    # os.system('echo " " >> ' + options.script)
    #
    # print('Running ' + 'cat ' + options.scriptsfolder + '/' + options.body + ' > ' + options.script)
    # os.system('cat ' + options.scriptsfolder + '/' + options.body + ' > ' + options.script)
    #
    # print('Running ' + options.runner + options.script)
    # os.system(options.runner + options.script)
