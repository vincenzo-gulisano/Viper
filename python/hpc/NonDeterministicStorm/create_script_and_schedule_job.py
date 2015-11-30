__author__ = 'vinmas'

import os


def create_script_and_schedule_job(scriptsfolder, header, body, script, id, command, runner):
    print('Running ' + 'cat ' + scriptsfolder + '/' + header + ' > ' + script)
    payload = scriptsfolder + '/payload.sh'
    # os.system('cat ' + scriptsfolder + '/' + header + ' > ' + script)
    os.system('cat /dev/null > ' + payload)

    print('Adding lines specific for the experiment')
    os.system('echo " " >> ' + payload)
    os.system('echo kill_id=' + id + ' >> ' + payload)
    os.system('echo command=\\"' + command + '\\" >> ' + payload)
    os.system('echo KILLDURATION=365 >> ' + payload)
    os.system('echo SAMPLEDURATION=365 >> ' + payload)
    os.system('echo sleep_time=90 >> ' + payload)
    os.system('echo " " >> ' + payload)
    # os.system('echo source '+payload+' >> ' + script)

    # print('Running ' + 'cat ' + scriptsfolder + '/' + body + ' >> ' + script)
    # os.system('cat ' + scriptsfolder + '/' + body + ' >> ' + script)
    # os.system('cd '+ scriptsfolder)
    # print('Running ' + runner +" "+ script)
    # os.system(runner + " "+ script)
    return
