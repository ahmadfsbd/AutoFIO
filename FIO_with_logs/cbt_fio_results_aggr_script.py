#Usage: python cbt_fio_results_aggr_script.py /path/to/cbt/archive
#This code only works for Librbdfio
#This script assumes NO results summarization (bwavgtime, log_avg_msec)
#

# Prints aggregated results

#!/usr/bin/python
import sys
import os
import glob
import json
import yaml
import pandas as pd
import multiprocessing
import math
import numpy as np

print_all_entries = False


#def percentile(data, percentile):
#    size = len(data)
#    return sorted(data)[int(math.ceil((size * percentile) / 100)) - 1]


def get_parameters(file_name):
    my_dict = {}
    with open(f_name, 'r') as stream:
        try:
            data = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    my_dict['num_clients'] = len(data['cluster']['clients'])
    my_dict['osds'] = data['cluster']['osds']
    my_dict['op_size'] = data['benchmarks']['librbdfio']['op_size']
    my_dict['volumes_per_client'] = data['benchmarks']['librbdfio']['volumes_per_client']
    my_dict['vol_size'] = data['benchmarks']['librbdfio']['vol_size']
    my_dict['mode'] = data['benchmarks']['librbdfio']['mode']
    my_dict['total_client_procs'] = len(data['cluster']['clients']) * data['benchmarks']['librbdfio']['volumes_per_client']
    return my_dict

def load_yaml(file_path):
    with open(file_path, 'r') as stream:
        try:
            data = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data

def add_and_average(Prev_Avg, Prev_Count, Add_Value):
    average = ((Prev_Avg * float(Prev_Count)) + Add_Value)/(Prev_Count +1)
    return average


def get_effective_start_stop_time(Path):  # Path to folder containing timestamp files.
    high_exp_start_time = 0
    low_exp_end_time = float("inf")
    
    timestamp_files = glob.glob(Path + '/output*_st_end*')
    for file in timestamp_files:
        f = open(file, "r")
        content = f.read()
        exp_start_time = int(content.split('\n')[0])
        exp_end_time = int(content.split('\n')[1])
        high_exp_start_time = max(high_exp_start_time, exp_start_time)
        if exp_end_time < low_exp_end_time:
                low_exp_end_time = exp_end_time
    ramptime =90  # get_ramp_time(Path)
    high_exp_start_time = high_exp_start_time + ramptime
    return (high_exp_start_time, low_exp_end_time)


def get_filename_from_timestamp_filename(timestamp_file_path, keyword): # Full Json FilePath, keyword(bw, clat, iops)
    file_path_list = timestamp_file_path.split('/')
    timestamp_file_name = file_path_list[-1]
    loadgen_name = timestamp_file_name.split('.')[-1]
    ts_id = timestamp_file_name.split('.')[1]
    process_num = ts_id.split('_')[0]
    file_path_list.pop(-1)
    new_file_name = 'output.' + process_num + '_' + keyword + '.1.log.' + loadgen_name
    file_path_list.append(new_file_name)
    file_path = '/'.join(file_path_list)
    return file_path

def average_list(some_list): # Pop first and last element and return mean of remaining
    summation = sum(some_list)
    average =  summation / float(len(some_list))
    return average

def get_logavg_time(path_to_folder):
    file_path = path_to_folder + '/benchmark_config.yaml'
    with open(file_path, 'r') as stream:
        try:
            data = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    if (data["cluster"]["bwavgtime"] == data["cluster"]["log_avg_msec"]):
        return data["cluster"]["bwavgtime"]
    else:	 # Here we are assuming bwavgtime and log_avg_msec should be equal.
        return 0 # Need to be fixed later, as if bwavgtime and log_avg_msec are not equal, minimum of them is used in BW case.
 
# get_logavg_time('/home/cbt/data_archieve/series/CL100/results/00000000/id-3223387321092103908')
def get_ramp_time(path_to_folder):
    file_path = path_to_folder + '/benchmark_config.yaml'
    with open(file_path, 'r') as stream:
        try:
            data = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    ramp_time = data["cluster"]["ramp"] * 1000
    return ramp_time

def calc_latency(timestamp_file_path, start_time, stop_time, read_lat_queue, write_lat_queue, read_tail_lat_queue, write_tail_lat_queue):
    test_dir_path = timestamp_file_path.split('/')
    test_dir_path.pop(-1)
    test_dir_path = '/'.join(test_dir_path)
    ramptime = 90 # get_ramp_time(test_dir_path)
    if start_time > stop_time:
       return (0,0)
    file_handle = open(timestamp_file_path, "r")
    content = file_handle.read()
    worker_absl_start_time = int(content.split('\n')[0]) + ramptime
    worker_absl_end_time = int(content.split('\n')[1])
    file_name = get_filename_from_timestamp_filename(timestamp_file_path, 'lat')
    r_avg = 0.0
    w_avg = 0.0
    r_tail = 0.0
    w_tail = 0.0

    r_lat_list = []
    w_lat_list = []

    file_object  = open(file_name, 'r')
    for line in file_object:
        line_split = line.split(',')
        rlv_inst_time = int(line_split[0])
        lat_value = int(line_split[1])
        rw_flag = int(line_split[2])
        absl_inst_time = rlv_inst_time + worker_absl_start_time

        if ((absl_inst_time > start_time) and (absl_inst_time < stop_time)):
            # add_and_average(Prev_Avg, Prev_Count, Add_Value)
            if rw_flag == 0: # FOR READ
#                r_avg = add_and_average(Prev_Avg = r_avg, Prev_Count = r_count, Add_Value = lat_value)
                r_lat_list.append(lat_value)
#                r_count += 1
                

            if rw_flag == 1: # FOR WRITE
#                w_avg = add_and_average(Prev_Avg = w_avg, Prev_Count = w_count, Add_Value = lat_value)
                w_lat_list.append(lat_value)
#                w_count += 1
    if len(r_lat_list) > 1:
        r_avg = (sum(r_lat_list)/len(r_lat_list))
        r_tail = np.percentile(r_lat_list, 99.99)
    else:
        r_avg = 0
        r_tail = 0

    if len(w_lat_list) > 1:
        w_avg = (sum(w_lat_list)/len(w_lat_list))
        w_tail = np.percentile(w_lat_list, 99.99)
    else:
        w_avg = 0
        w_tail = 0

    read_lat_queue.put(r_avg)
    write_lat_queue.put(w_avg)
    read_tail_lat_queue.put(r_tail)
    write_tail_lat_queue.put(w_tail)

#(read, write) = calc_latency(timestamp_file_path='/home/cbt/series/fio-2.13-4M-160CL-NoLogAvg/results/00000000/id6272496687746660040/output.0_st_end.loadgen0', start_time=1550212357280, stop_time=1550212470615)
#print read
#print write

#(read, write) = calc_latency(timestamp_file_path='/home/cbt/series/Bal_4K_NoLogAvg/results/00000000/id-1078120976060848440/output.0_st_end.loadgen0', start_time=1550064886010, stop_time=1550065578550)
#print 'Read: ' + str(read)
#print 'Write: ' + str(write)
#print '************************'

def calc_bw(timestamp_file_path, start_time, stop_time, read_bw_queue, write_bw_queue):
    test_dir_path = timestamp_file_path.split('/')
    test_dir_path.pop(-1)
    test_dir_path = '/'.join(test_dir_path)
    ramptime = 90 # get_ramp_time(test_dir_path)
    if start_time > stop_time:
       return (0,0)
    file_handle = open(timestamp_file_path, "r")
    content = file_handle.read()
    worker_absl_start_time = int(content.split('\n')[0]) + ramptime
    worker_absl_end_time = int(content.split('\n')[1])
    file_name = get_filename_from_timestamp_filename(timestamp_file_path, 'bw')
    r_data = 0
    w_data = 0
    r_first = True
    w_first = True
    file_object  = open(file_name, 'r')
    for line in file_object:
        line_split = line.split(',')
        rlv_inst_time = int(line_split[0])
        if r_first == True:
            r_first_inst = rlv_inst_time
        if w_first == True:
            w_first_inst = rlv_inst_time
        absl_inst_time = rlv_inst_time + worker_absl_start_time
        if ((absl_inst_time > start_time) and (absl_inst_time < stop_time)):
            var_value = int(line_split[1])
            rw_flag = int(line_split[2])
            block_size = int(line_split[3])
            # add_and_average(Prev_Avg, Prev_Count, Add_Value)
            if rw_flag == 0: # FOR READ
                r_first = False
                r_last_inst = rlv_inst_time
                r_data += block_size

            if rw_flag == 1: # FOR WRITE
                w_first = False
                w_last_inst = rlv_inst_time
                w_data += block_size
#    print 'write data ' + str(w_data)
#    print 'Last Time ' + str(w_last_inst)
#    print 'First Time ' + str(w_first_inst)
    if r_data == 0:
        r_bw = 0
    else:
        r_bw = (r_data / (r_last_inst - r_first_inst))
    if w_data == 0:
        w_bw = 0
    else:
        w_bw = (w_data / (w_last_inst - w_first_inst))

    read_bw_queue.put(r_bw)
    write_bw_queue.put(w_bw)

#(read, write) = calc_bw(timestamp_file_path='/home/cbt/series/Bal_4K_NoLogAvg/results/00000000/id-1078120976060848440/output.0_st_end.loadgen0', start_time=1550064886010, stop_time=1550065578550)
#print 'Read: ' + str(read)
#print 'Write: ' + str(write)
#print '************************'

def calc_iops(timestamp_file_path, start_time, stop_time, read_iops_queue, write_iops_queue):
    test_dir_path = timestamp_file_path.split('/')
    test_dir_path.pop(-1)
    test_dir_path = '/'.join(test_dir_path)
    ramptime = 90 # get_ramp_time(test_dir_path)
    if start_time > stop_time:
       return (0,0)
    file_handle = open(timestamp_file_path, "r")
    content = file_handle.read()
    worker_absl_start_time = int(content.split('\n')[0]) + ramptime
    worker_absl_end_time = int(content.split('\n')[1])
    file_name = get_filename_from_timestamp_filename(timestamp_file_path, 'iops')
    r_data = 0
    w_data = 0
    r_first = True
    w_first = True
    file_object  = open(file_name, 'r')
    for line in file_object:
        line_split = line.split(',')
        rlv_inst_time = int(line_split[0])
        if r_first == True:
            r_first_inst = rlv_inst_time
        if w_first == True:
            w_first_inst = rlv_inst_time
        absl_inst_time = rlv_inst_time + worker_absl_start_time
        if ((absl_inst_time > start_time) and (absl_inst_time < stop_time)):
            var_value = int(line_split[1])
            rw_flag = int(line_split[2])
            block_size = int(line_split[3])
            # add_and_average(Prev_Avg, Prev_Count, Add_Value)
            if rw_flag == 0: # FOR READ
                r_first = False
                r_last_inst = rlv_inst_time
                r_data += var_value

            if rw_flag == 1: # FOR WRITE
                w_first = False
                w_last_inst = rlv_inst_time
                w_data += var_value
#    print 'Write Data: ' + str(w_data)
#    print 'First Time: ' + str(w_first_inst)
#    print 'Last Time: ' + str(w_last_inst)
    if r_data == 0:
        r_iops = 0
    else:
        r_iops = (r_data / ((r_last_inst - r_first_inst)/1000.0))
    if w_data == 0:
        w_iops = 0
    else:
        w_iops = (w_data / ((w_last_inst - w_first_inst)/1000.0))

    read_iops_queue.put(r_iops)
    write_iops_queue.put(w_iops)

#(read, write) = calc_iops(timestamp_file_path='/home/cbt/series/Bal_4K_NoLogAvg/results/00000000/id-1078120976060848440/output.0_st_end.loadgen0', start_time=1550064886010, stop_time=1550065578550)
#print 'Read: ' + str(read)
#print 'Write: ' + str(write)

def get_activity_start_stop_time(Path):  # Path to folder containing timestamp files.
    low_exp_start_time = 10e20
    high_exp_end_time = 0

    timestamp_files = glob.glob(Path + '/output*_st_end*')
    for file in timestamp_files:
        f = open(file, "r")
        content = f.read()
        exp_start_time = int(content.split('\n')[0])
        exp_end_time = int(content.split('\n')[1])
        high_exp_end_time = max(high_exp_end_time, exp_end_time)
        low_exp_start_time = min(low_exp_start_time, exp_start_time)
    return (low_exp_start_time, high_exp_end_time)


def calculate_throughput(common_start, common_stop, timestamps_list, read_bw_val, write_bw_val):
    read_bw_queue = multiprocessing.Queue()
    write_bw_queue = multiprocessing.Queue()
    jobs = []
    read_bw = 0
    write_bw = 0
    for idx, val in enumerate(timestamps_list):
        p_name = 'proc_bw_' + str(idx)
        exec( p_name + " = multiprocessing.Process(target=calc_bw, args=(\'"+ val + "\',common_start,common_stop,read_bw_queue,write_bw_queue))")
        jobs.append(p_name)
    
    for job in jobs:
        exec(job + ".start()")
    
    for job in jobs:
        exec(job + ".join()")

    while read_bw_queue.empty() is False:
        read_bw += read_bw_queue.get()
    while write_bw_queue.empty() is False:
        write_bw += write_bw_queue.get()

    read_bw_val.value = read_bw
    write_bw_val.value = write_bw
    

def calculate_iops(start, stop, timestamps_list, read_iops, write_iops):
    read_iop_queue = multiprocessing.Queue()
    write_iop_queue = multiprocessing.Queue()
    jobs = []
    read_iop = 0
    write_iop = 0
    for idx, val in enumerate(timestamps_list):
        p_name = 'proc_iop_' + str(idx)
        exec( p_name + " = multiprocessing.Process(target=calc_iops, args=(\'"+ val + "\',start,stop,read_iop_queue,write_iop_queue))")
        jobs.append(p_name)

    for job in jobs:
        exec(job + ".start()")

    for job in jobs:
        exec(job + ".join()")

    while read_iop_queue.empty() is False:
        read_iop += read_iop_queue.get()
    while write_iop_queue.empty() is False:
        write_iop += write_iop_queue.get()

    read_iops.value = read_iop
    write_iops.value = write_iop

def calculate_latency(start, stop, timestamps_list, read_latency, write_latency, read_tail_latency, write_tail_latency):
    read_lat_queue = multiprocessing.Queue()
    write_lat_queue = multiprocessing.Queue()
    read_tail_lat_queue = multiprocessing.Queue()
    write_tail_lat_queue = multiprocessing.Queue()
    jobs = []
    read_lat = 0
    write_lat = 0
    read_tail_lat = 0
    write_tail_lat = 0
#    read_lat_list = list()
#    write_lat_list = list()
    r_count = 0
    w_count = 0
    r_t_count = 0
    w_t_count = 0

    for idx, val in enumerate(timestamps_list):
        p_name = 'proc_lat_' + str(idx)
        exec( p_name + " = multiprocessing.Process(target=calc_latency, args=(\'"+ val + "\',start,stop,read_lat_queue,write_lat_queue, read_tail_lat_queue, write_tail_lat_queue))")
        jobs.append(p_name)

    for job in jobs:
        exec(job + ".start()")

    for job in jobs:
        exec(job + ".join()")

    while read_lat_queue.empty() is False:
#        read_lat_list.append(read_lat_queue.get())
        read_lat = add_and_average(read_lat, r_count, read_lat_queue.get())
        r_count += 1
    while write_lat_queue.empty() is False:
#        write_lat_list.append(write_lat_queue.get())
        write_lat = add_and_average(write_lat, w_count, write_lat_queue.get())
        w_count += 1

    while read_tail_lat_queue.empty() is False:
#        read_lat_list.append(read_lat_queue.get())
        read_tail_lat = add_and_average(read_tail_lat, r_t_count, read_tail_lat_queue.get())
        r_t_count += 1
    while write_tail_lat_queue.empty() is False:
#        write_lat_list.append(write_lat_queue.get())
        write_tail_lat = add_and_average(write_tail_lat, w_t_count, write_tail_lat_queue.get())
        w_t_count += 1


#    read_latency.value = sum(read_lat_list)/len(read_lat_list)
#    write_latency.value = sum(write_lat_list)/len(write_lat_list)
    read_latency.value = read_lat
    write_latency.value = write_lat
    read_tail_latency.value = read_tail_lat
    write_tail_latency.value = write_tail_lat

if len(sys.argv) == 2:
    path = sys.argv[1]
    for x in os.walk(path):

        if len(x[2]) > 4: # Probably Directory With Results
            n = 0
            read_bw_val = multiprocessing.Value('d', 0.0)
            write_bw_val = multiprocessing.Value('d', 0.0)
            read_iops_val = multiprocessing.Value('d', 0.0)
            write_iops_val = multiprocessing.Value('d', 0.0)
            read_lat_val = multiprocessing.Value('d', 0.0)
            write_lat_val = multiprocessing.Value('d', 0.0)
            read_tail_lat_val = multiprocessing.Value('d', 0.0)
            write_tail_lat_val = multiprocessing.Value('d', 0.0)


            (start, stop) = get_effective_start_stop_time(x[0])
            (a_start, a_stop) = get_activity_start_stop_time(x[0])

            timestamp_files = glob.glob(x[0] + '/output*_st_end*')

            

#            if (len(timestamp_files) != (my_dict['volumes_per_client'] * my_dict['num_clients'])):
#                print 'Expected number of timestamp files = ' + str(my_dict['volumes_per_client'] * len(my_dict['clients']))
#                print 'Number of files in folder ' + x[0] + ' = ' + str(len(timestamp_files))
#           calculate_latency(start, stop, timestamps_list, read_latency, write_latency, read_tail_latency, write_tail_latency):
            calculate_throughput(start, stop, timestamp_files, read_bw_val, write_bw_val)
            calculate_iops(start, stop, timestamp_files, read_iops_val, write_iops_val)
            calculate_latency(start, stop, timestamp_files, read_lat_val, write_lat_val, read_tail_lat_val, write_tail_lat_val)

            start_time = pd.to_datetime(start, unit='ms')
            end_time = pd.to_datetime(stop, unit='ms')
            activity_start_time = pd.to_datetime(a_start, unit='ms')
            activity_end_time = pd.to_datetime(a_stop, unit='ms')
            print 'Experiment Start Time: ' + str(start_time)
            print 'Experiment Finish Time: ' + str(end_time)
            print 'Activity happened during: ' + str(activity_start_time) + ' -- ' + str(activity_end_time)
            print '\n\tWrite-IOPS = ' + str('%.0f'%(write_iops_val.value)) + '\t\t\t\t\tRead-IOPS = ' + str('%.0f'%(read_iops_val.value))
            print '\tWrite-Bandwidth = ' + str('%.2f'%(write_bw_val.value/1024)) + 'MiB/s\t\t\t\tRead-Bandwidth = ' + str('%.2f'%(read_bw_val.value/1024)) + 'MiB/s'
            print '\tWrite-Latency = ' + str('%.2f'%(write_lat_val.value/1000000)) + 'ms\t\t\t\t\tRead-Latency = ' + str('%.2f'%(read_lat_val.value/1000000)) + 'ms'
            print '\tWrite-Tail_Latency (99.99) = ' + str('%.2f'%(write_tail_lat_val.value/1000000)) + 'ms\t\t\t\t\tRead-Tail_Latency (99.99) = ' + str('%.2f'%(read_tail_lat_val.value/1000000)) + 'ms'
            print '------------------------------------------------------------------------'
            print '\n'

else:
    print "Please Enter CBT Archive Directory as Argument"


