import paramiko
import os
import time
import commands
import sys
import signal
import errno
import threading
import pdb
import pprint


ips = ['''100.82.36.226''','''100.82.36.239''','''100.82.36.213''','''100.82.36.205''','''100.82.36.210''','''100.82.36.206''','''100.82.36.219''','''100.82.36.228''','''100.82.36.216''','''100.82.36.223''','''100.82.36.215''','''100.82.36.167''','''100.82.36.180''','''100.82.36.178''','''100.82.36.172''','''100.82.36.173''','''100.82.36.171''','''100.82.36.182''','''100.82.36.175''','''100.82.36.169''','''100.82.36.177''','''100.82.36.179''','''100.82.36.181''','''100.82.36.174''','''100.82.36.176''''''100.82.36.215''','''100.82.36.185''','''100.82.36.184''','''100.82.36.183''','''100.82.36.170''','''100.82.36.201''']

#ips = ['''100.82.36.183''']

user = '''centos'''
pwd = None
key_path = '''/home/osp_admin/NFV_window/nfv-auto/ceph-key.pem'''
results = {}
fio_cmd = ('''sudo fio /home/centos/Data/jobfile.fio --output-format=json  --output=''')


cwd = os.getcwd()
cwd_results = cwd + '''/fio_results/'''
jobfile_path = cwd + '''/jobfile.fio'''


def run_fio_on_vm(ip):
    global user, pwd, key_path, fio_cmd, results, cwd_results, jobfile_path
    out_file = '''Result_of_''' + ip + '''.txt'''
    out = '''/home/centos/Results/Result_of_''' + ip + '''.txt'''
    cmd0 = fio_cmd + out
    # ssh into vm
    print("SSH to VM {}".format(ip))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=ip, username=user, password=pwd, key_filename=key_path)
    if ssh:
	      print("SSH successful to : {}".format(ip))
    
    # check if directories exist or mkdir
    stdin, stdout, stderr = ssh.exec_command('''sudo ls /home/centos/Data''')
    if stderr.readlines():
        print('''/home/centos/Data directory does not exist! Creating now...''' )
        #time.sleep(1)
        stdin, stdout, stderr = ssh.exec_command('''sudo mkdir /home/centos/Data''')
        stdin, stdout, stderr = ssh.exec_command('''sudo chown centos:centos /home/centos/Data''')
    else:
        print('''/home/centos/Data directory already exists!''')
    
    stdin, stdout, stderr = ssh.exec_command('''sudo ls /home/centos/Results''')
    if stderr.readlines():
        print('''/home/centos/Results directory does not exist! Creating now...''' )
        #time.sleep(1)
        stdin, stdout, stderr = ssh.exec_command('''sudo mkdir /home/centos/Results''')
        stdin, stdout, stderr = ssh.exec_command('''sudo chown centos:centos /home/centos/Results''')
    else:
        print('''/home/centos/Results directory already exists!''')
        
    stdin, stdout, stderr = ssh.exec_command('''sudo pkill -9 fio*''')
    exit_status = stdout.channel.recv_exit_status()          # Blocking call
    if exit_status == 0:
        print ("Fio processes killed on {}".format(ip))
    stdin, stdout, stderr = ssh.exec_command('''sudo rm -rf /home/centos/Data/jobfile.fio''')
    exit_status = stdout.channel.recv_exit_status()          # Blocking call
    if exit_status == 0:
        print ("Old jobfile deleted on {}".format(ip))
    #return
##############################################################################################
    # copy job file
    sftp = ssh.open_sftp()
    print('''copying jobfile to : {}'''.format(ip))
    sftp.put(jobfile_path, '''/home/centos/Data/jobfile.fio''')
    time.sleep(5)
    sftp.close()
##############################################################################################
    #execute fio command
    print("Executing Command: %s" %cmd0)
    stdin, stdout, stderr = ssh.exec_command(cmd0)
    #time.sleep(50)
    # wait to finish
    exit_status = stdout.channel.recv_exit_status()          # Blocking call
    if exit_status == 0:
        print ("Fio-Run completed on {}".format(ip))
        sftp = ssh.open_sftp()
        local_result = cwd_results + out_file
        sftp.get(out, local_result)
        sftp.close()

    else:
        print("Error", exit_status)
    print stdout.readlines()
    ssh.close()


def main():
    #pdb.set_trace()
    for ip in ips:
        try:
            #run_fio_on_vm(ip)
            thread = threading.Thread(target=run_fio_on_vm, args=[ip])
            #thread.daemon = True
            thread.start()
            print('Thread started: {} \n'.format(ip))
        except:
            print('Exception starting thread {}'.format(ip))
            print(traceback.print_exc())
            print(sys.exc_info())
            print('Line No: %s' % (sys.exc_info()[2].tb_lineno))
            raise

if __name__ == "__main__":
    try:
        main()
    except:
        print('Exception run_fio.py')
        print('Cause of Exception: %s' %(sys.exc_info()[0]))
        print('Exception: %s' %(sys.exc_info()[1]))
        sys.exit(0)
    finally:
        print('run_fio.py stopped...')


