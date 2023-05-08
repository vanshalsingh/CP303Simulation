import threading
import time
import heapq
import random
import concurrent.futures
waitTime=0
TcWaitTime=0
TrWaitTime=0
TpWaitTime=0
completed_jobs=0
timeLock=threading.Lock()
timeLock1=threading.Lock()
timeLock2=threading.Lock()
timeLock3=threading.Lock()
threads=[]
# Job class to represent individual jobs
class Job:
    def __init__(self, job_id, arrival_time, deadline, estimated_time, security_tag):
        self.job_id = job_id
        self.arrival_time = arrival_time
        self.deadline = deadline
        self.estimated_time = estimated_time
        self.security_tag = security_tag
        self.waitTime=0
        self.completed = False
    
    def __lt__(self, other):
        return (self.deadline < other.deadline)

    def __gt__(self, other):
        return (self.deadline > other.deadline)
# DataCenter class to represent individual data centers
class DataCenter:
    def __init__(self,name, time_restriction, space_condition):
        self.name=name
        self.time_restriction = time_restriction
        self.space_condition = space_condition
        self.space_lock = threading.Lock()
        self.current_jobs=[]
        self.completedJobs=0

    def can_schedule_job(self, job):
        return job.estimated_time <= self.time_restriction and len(self.current_jobs) < self.space_condition

    def schedule_job(self, job):
        with self.space_lock:
            self.current_jobs.append(job)
            self.completedJobs = self.completedJobs + 1
        #print(f"Job with tag {job.security_tag} and data centre {self.name} and current length {len(self.current_jobs)}")
        if self.name == 'Fn':
            time.sleep(job.estimated_time)
        if self.name == 'Fd':
            time.sleep(job.estimated_time+0.5)
        if self.name == 'Cnpr':
            time.sleep(job.estimated_time+1)
        if self.name == 'Cnpu':
            time.sleep(job.estimated_time+1)
        if self.name == 'Cdpr':
            time.sleep(job.estimated_time+2)
        if self.name == 'Cdpu':
            time.sleep(job.estimated_time+2)

        with self.space_lock:
            self.current_jobs.remove(job)

    def remove_completed_jobs(self):
        with self.space_lock:
            self.current_jobs = [job for job in self.current_jobs if not job.completed]

# Function to schedule jobs with security tag Tc
def schedule_C(job):
    if job.security_tag == 'Tc':
        for data_center in (Fn, Cnpr):
            if data_center.can_schedule_job(job):
                data_center.schedule_job(job)
                job.completed = True
                break
        # else:
        #     print(f"Job {job.job_id} with security tag Tc could not be scheduled.")

# Function to schedule jobs with security tag Tr
def schedule_R(job):
    if job.security_tag == 'Tr':
        for data_center in (Fn, Fd, Cnpr, Cnpu):
            if data_center.can_schedule_job(job):
                data_center.schedule_job(job)
                job.completed = True
                break
        # else:
        #     print(f"Job {job.job_id} with security tag Tr could not be scheduled.")

# Function to schedule jobs with security tag Tp
def schedule_P(job):
    if job.security_tag == 'Tp':
        for data_center in (Fn, Fd, Cnpr, Cnpu, Cdpr, Cdpu):
            if data_center.can_schedule_job(job):
                data_center.schedule_job(job)
                job.completed = True
                break
        # else:
        #     print(f"Job {job.job_id} with security tag Tp could not be scheduled.")

# Function to simulate job scheduling and execution
def simulate_jobs():
    
   
    #threads.append(t1)
    while len(job_heap):

        _, job = heapq.heappop(job_heap)
        current_time=time.time()
        if job.arrival_time > current_time - clock :
            time.sleep(job.arrival_time - (current_time-clock))
            #print(f"I slept {job.arrival_time - (current_time-clock)} seconds")
        t1 = threading.Thread(target=simulate_jobs)
        t1.start()
            
        sTime=time.time()
        if job:
            if not job.completed:
                if job.security_tag == 'Tc':
                    schedule_C(job)
                elif job.security_tag == 'Tr':
                    schedule_R(job)
                elif job.security_tag == 'Tp':
                    schedule_P(job)

                if job.completed:
                    global waitTime,completed_jobs, TrWaitTime, TcWaitTime, TpWaitTime
                    
                    with timeLock:
                        waitTime = waitTime + (time.time()-sTime)+job.waitTime-job.estimated_time

                    if job.security_tag == 'Tc':
                        with timeLock1:
                            TrWaitTime = TrWaitTime + (time.time()-sTime)+job.waitTime-job.estimated_time
                    if job.security_tag == 'Tr':
                        with timeLock2:
                            TcWaitTime = TcWaitTime + (time.time()-sTime)+job.waitTime-job.estimated_time
                    if job.security_tag == 'Tp':
                        with timeLock3:
                            TpWaitTime = TpWaitTime + (time.time()-sTime)+job.waitTime-job.estimated_time

                    completed_jobs = completed_jobs + 1
                    #print(waitTime)
                    print(f"Job {job.job_id} completed with total time {(time.time()-sTime+job.waitTime):.2f} and estimated time {job.estimated_time}")
                elif job.arrival_time + job.deadline < (time.time()-clock) + job.waitTime + job.estimated_time:
                    print(f"Job {job.job_id} could not be scheduled")
                else:
                    job.waitTime=job.waitTime+(time.time()-sTime)
                    heapq.heappush(job_heap,(0,job))
        
        t1.join()
# Create DataCenter instances

Fn = DataCenter('Fn',time_restriction=6, space_condition=1)
Fd = DataCenter('Fd',time_restriction=6, space_condition=1)
Cnpr = DataCenter('Cnpr',time_restriction=15, space_condition=3)
Cnpu = DataCenter('Cnpu',time_restriction=15, space_condition=3)
Cdpr = DataCenter('Cdpr',time_restriction=15, space_condition=3)
Cdpu = DataCenter('Cdpu',time_restriction=15, space_condition=3)

# Fn = DataCenter('Fn',time_restriction=10, space_condition=1)
# Fd = DataCenter('Fd',time_restriction=10, space_condition=1)
# Cnpr = DataCenter('Cnpr',time_restriction=21, space_condition=3)
# Cnpu = DataCenter('Cnpu',time_restriction=21, space_condition=3)
# Cdpr = DataCenter('Cdpr',time_restriction=21, space_condition=3)
# Cdpu = DataCenter('Cdpu',time_restriction=21, space_condition=3)

# Create a list of jobs (arrival_time, deadline, estimated_time, security_tag)
# jobs = [(0,4,4,'Tc'),(1,4,3,'Tc'),(2,4,2,'Tc'),(3,4,1,'Tc')]
jobs=[]
totalJobs = 450
for _ in range(150):
    arrival_time = random.randint(0, 1080)  # Random arrival time within 24 hours (86400 seconds)
    estimated_time = random.randint(3, 15)  # Random estimated time between 1 and 10 seconds
    deadline = random.randint(estimated_time, estimated_time+5)  # Random deadline within 1 hour (3600 seconds)
    security_tag = 'Tc'  # Random security tag
    jobs.append((arrival_time, deadline, estimated_time, security_tag))
for _ in range(150):
    arrival_time = random.randint(0, 1080)  # Random arrival time within 24 hours (86400 seconds)
    estimated_time = random.randint(3, 15)  # Random estimated time between 1 and 10 seconds
    deadline = random.randint(estimated_time, estimated_time+5)
    security_tag = 'Tr'  # Random security tag
    jobs.append((arrival_time, deadline, estimated_time, security_tag))
for _ in range(150):
    arrival_time = random.randint(0, 1080)  # Random arrival time within 24 hours (86400 seconds)
    estimated_time = random.randint(3, 15)  # Random estimated time between 1 and 10 seconds
    deadline = random.randint(estimated_time, estimated_time+5)
    security_tag = 'Tp'  # Random security tag
    jobs.append((arrival_time, deadline, estimated_time, security_tag))

# totalJobs = 999
# for _ in range(333):
#     arrival_time = random.randint(0, 3600)  # Random arrival time within 24 hours (86400 seconds)
#     estimated_time = random.randint(7, 21)  # Random estimated time between 1 and 10 seconds
#     deadline = random.randint(estimated_time, estimated_time+5)  # Random deadline within 1 hour (3600 seconds)
#     security_tag = 'Tc'  # Random security tag
#     jobs.append((arrival_time, deadline, estimated_time, security_tag))
# for _ in range(333):
#     arrival_time = random.randint(0, 3600)  # Random arrival time within 24 hours (86400 seconds)
#     estimated_time = random.randint(7, 21)  # Random estimated time between 1 and 10 seconds
#     deadline = random.randint(estimated_time, estimated_time+5)
#     security_tag = 'Tr'  # Random security tag
#     jobs.append((arrival_time, deadline, estimated_time, security_tag))
# for _ in range(333):
#     arrival_time = random.randint(0, 3600)  # Random arrival time within 24 hours (86400 seconds)
#     eestimated_time = random.randint(7, 21)  # Random estimated time between 1 and 10 seconds
#     deadline = random.randint(estimated_time, estimated_time+5)
#     security_tag = 'Tp'  # Random security tag
#     jobs.append((arrival_time, deadline, estimated_time, security_tag))
# Create a heap of jobs based on arrival_time
job_heap = [(arrival_time, Job(job_id, arrival_time, deadline, estimated_time, security_tag))
            for job_id, (arrival_time, deadline, estimated_time, security_tag) in enumerate(jobs)]
heapq.heapify(job_heap)
clock=time.time()
simulate_jobs()
# Start the simulation
# completed_jobs = 0

# threadPool = concurrent.futures.ThreadPoolExecutor(max_workers=14)

# for i in (0,14):
#     threadPool.submit(simulate_jobs())

# threadPool.shutdown(wait=True)
threading.Lock()

# for i in threads:
#     i.join()

print(f"Total jobs recieved: {totalJobs}")
print(f"Number of completed jobs: {completed_jobs}")
print(f"Number of rejected jobs: {totalJobs - completed_jobs}")
print(f"Completion ratio: {completed_jobs/totalJobs}")
print(f"Average wait time for jobs: {waitTime/totalJobs}")
print(f"Average wait time for classified jobs: {TcWaitTime/(totalJobs//3)}")
print(f"Average wait time for restricted jobs: {TrWaitTime/(totalJobs//3)}")
print(f"Average wait time for public jobs: {TpWaitTime/(totalJobs//3)}")

print("Number of jobs completed per data centre:-")
print(f"Fn: {Fn.completedJobs}")
print(f"Fd: {Fd.completedJobs}")
print(f"Cnpr: {Cnpr.completedJobs}")
print(f"Cnpu: {Cnpu.completedJobs}")
print(f"Cdpr: {Cdpr.completedJobs}")
print(f"Cdpu: {Cdpu.completedJobs}")

# print(f"Total time for jobs:{waitTime}")

