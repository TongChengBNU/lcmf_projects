import multiprocessing
#from multiprocessing import Process
import time


def hello(first_name, last_name):
    print("Hello, " + first_name + " " + last_name)
    return

p1 = multiprocessing.Process(target = hello, args=('Tong', 'Cheng'))

def monitor(pool):
    local_time = time.localtime(time.time())
    print('Current time: {0}h {1}min {2}s'.format(local_time.tm_hour, local_time.tm_min, local_time.tm_sec))
    print('-------------------------------------')
    for index, process in enumerate(pool):
        print("Condition of Process {0} (pid: {1}): {2}".format(index, process.pid, process.is_alive()))
    return

# Initialize and start a batch of processes, return a list of Process object
def multiprocess_main(num_process=5, target_function, names_list=None: List, args_list: List):
    if names_list is None:
        names_list = ['p'+str(i) for i in range(num_process)]
    pool = [multiprocessing.Process(target = target_function, name=process_name) for _, process_name in zip(range(num_process),names_list)]
    for process, args_item in zip(pool, args_list):
        process._args = args_item
        process.start()
    monitor(pool)
    return pool



