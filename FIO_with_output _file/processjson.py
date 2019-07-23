import json
import os

# path to the result files (windows)
path = '''C:\\Users\\Ahmad\\Desktop\\test1\\test1\\'''
# path to the result files (linux)
#path = '''/home/osp_admin/fio_results/'''

read_results = {"iops": 0, "bw": 0 , "lat": 0}
write_results = {"iops": 0, "bw": 0 , "lat": 0}

total_jobs = 0

# get list of files
list_files = os.listdir(path)

for filename in list_files:
    file = path + filename
    with open(file, 'r') as f:
        data = json.load(f)
    jobs = data["jobs"]
    for job in jobs:
        total_jobs = total_jobs + 1
        #read section
        job_read = job["read"]
        read_results["iops"] = read_results["iops"] + float(job_read["iops"])
        read_results["bw"] = read_results["bw"] + float(job_read["bw"])
        read_results["lat"] = read_results["lat"] + float(job_read["lat_ns"]["mean"])

        #write section
        job_write = job["write"]
        write_results["iops"] = write_results["iops"] + float(job_write["iops"])
        write_results["bw"] = write_results["bw"] + float(job_write["bw"])
        write_results["lat"] = write_results["lat"] + float(job_write["lat_ns"]["mean"])
    f.close()

read_results["lat"] = read_results["lat"] / total_jobs
write_results["lat"] = write_results["lat"] / total_jobs

result_file = path + "Results.txt"
f = open(result_file,'w')
f.write("read : {} \n write : {}".format(read_results, write_results))
f.close()



