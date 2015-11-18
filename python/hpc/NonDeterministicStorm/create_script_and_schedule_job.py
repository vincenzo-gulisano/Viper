__author__ = 'vinmas'

import os


def create_script_and_schedule_job(scriptsfolder, header, body, script, id, command, runner):
    print('Running ' + 'cat ' + scriptsfolder + '/' + header + ' > ' + script)
    os.system('cat ' + scriptsfolder + '/' + header + ' > ' + script)

    print('Adding lines specific for the experiment')
    os.system('echo " " >> ' + script)
    os.system('echo kill_id=' + id + ' >> ' + script)
    os.system('echo command=\\"' + command + '\\" >> ' + script)
    os.system('echo KILLDURATION=365 >> ' + script)
    os.system('echo SAMPLEDURATION=365 >> ' + script)
    os.system('echo sleep_time=90 >> ' + script)
    os.system('echo " " >> ' + script)

    print('Running ' + 'cat ' + scriptsfolder + '/' + body + ' >> ' + script)
    os.system('cat ' + scriptsfolder + '/' + body + ' >> ' + script)

    print('Running ' + runner + " " + script)
    os.system(runner + " " + script)

    return
