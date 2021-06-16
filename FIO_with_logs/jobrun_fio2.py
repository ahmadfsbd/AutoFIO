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


ips = ['''100.67.153.70''','''100.67.153.71''','''100.67.153.80''','''100.67.153.90''','''100.67.153.73''','''100.67.153.84''','''100.67.153.72''','''100.67.153.78''','''100.67.153.75''','''100.67.153.74''','''100.67.153.76''','''100.67.153.79''','''100.67.153.85''','''100.67.153.81''','''100.67.153.87''','''100.67.153.92''','''100.67.153.86''','''100.67.153.89''','''100.67.153.91''','''100.67.153.94''','''100.67.153.95''','''100.67.153.96''','''100.67.153.98''','''100.67.153.99''','''100.67.153.97''','''100.67.153.100''','''100.67.153.93''','''100.67.153.77''','''100.67.153.68''','''100.67.153.67''']

"""ips = ['''100.67.153.97''']"""

user = '''centos'''
pwd = None
key_path = '''/home/osp_admin/ceph-key.pem'''
results = {}
fio_cmd = '''sudo fio /home/centos/Data/jobfile.fio'''


cwd = os.getcwd()
cwd_results = cwd + '''/fio_results/'''
jobfile_path = cwd + '''/jobfile.fio'''


def run_fio_on_vm(ip, num):
    global user, pwd, key_path, fio_cmd, results, cwd_results, jobfile_path
    remote_result_files = []
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
        
    #stdin, stdout, stderr = ssh.exec_command('''sudo pkill -9 fio*''')
    #time.sleep(3)

################################# DELETE PREVIOUS FILES ##################################		
    stdin, stdout, stderr = ssh.exec_command('''sudo rm -rf /home/centos/Data/jobfile.fio''')

    stdin, stdout, stderr = ssh.exec_command('''sudo rm -rf /home/centos/Results/*''')

################################## COPY NEW JOB-FILE ######################################
    sftp = ssh.open_sftp()
    print('''copying jobfile to : {}'''.format(ip))
    sftp.put(jobfile_path, '''/home/centos/Data/jobfile.fio''')
    time.sleep(1)
    sftp.close()
########################### EXECUTE FIO CMD AND WAIT TO FINISH #############################
    print("Executing Command: %s" %fio_cmd)
    #add timestamp
    time_file = '''/home/centos/Results/''' + '''output.0_st_end.0.log'''    
    cmdt = '''sudo touch ''' + time_file
    stdin, stdout, stderr = ssh.exec_command(cmdt)
    time.sleep(1)
    cmdt = '''sudo chown centos:centos ''' + time_file
    stdin, stdout, stderr = ssh.exec_command(cmdt)
    time.sleep(1)
    cmdt = '''sudo echo $(($(date +%s%N)/1000000)) >> ''' + time_file
    stdin, stdout, stderr = ssh.exec_command(cmdt)
    time.sleep(1)
    
    stdin, stdout, stderr = ssh.exec_command(fio_cmd)
    # wait to finish
    exit_status = stdout.channel.recv_exit_status()          # Blocking call
    if exit_status == 0:
    
        cmdt = '''sudo echo $(($(date +%s%N)/1000000)) >> ''' + time_file
        stdin, stdout, stderr = ssh.exec_command(cmdt)
        time.sleep(1)
        
        #time_file1 = '''/home/centos/Results/''' + '''output.1_st_end.0.log'''
        #cmdt = '''sudo cp ''' + time_file + ''' ''' + time_file1
        #stdin, stdout, stderr = ssh.exec_command(cmdt)
        
        cmdt = '''sudo chown -r centos:centos /home/centos/Results'''
        stdin, stdout, stderr = ssh.exec_command(cmdt)
        
        print ("Fio-Run completed on {} : Copying Result Files.".format(ip))
############################ COPY RESULTS #######################################
        # get list of files
        sftp = ssh.open_sftp()
        remote_result_files = sftp.listdir(path='''/home/centos/Results/''')
        remote_result_files = [str(item) for item in remote_result_files]
        for filename in remote_result_files:
            remote_file = '''/home/centos/Results/''' + filename
            local_file = cwd_results + filename + '''.''' + str(num)
            sftp.get(remote_file, local_file)
        print("All RESULTS copied from {}".format(ip))
        sftp.close()
    
#####################################################################
    else:
        print("Error", exit_status)
    ssh.close()

def main():
    #pdb.set_trace()
    num = 0
    for ip in ips:
        try:
            #run_fio_on_vm(ip)
            thread = threading.Thread(target=run_fio_on_vm, args=[ip, num])
            #thread.daemon = True
            thread.start()
            num = num + 1
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


