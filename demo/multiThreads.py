def old():
    import time, _thread


    def timer(name=None, group=None):
        print('name: ' + name + ', group: ' + group)
    def test():
        for i in range(0, 10):
            _thread.start_new_thread(timer, ('thread' + str(i), 'group' + str(i)))



    lock = _thread.allocate_lock()
    count = 0

    def test_lock():
        global count, lock
        lock.acquire()
        for i in range(0, 100):
            count += 1
        lock.release()







    if __name__=='__main__':
        # test()
        # time.sleep(10)
        for i in range(0, 10):
            _thread.start_new_thread(test_lock, ())
        time.sleep(5)
        print(count)

from timeit import repeat
import math

def performance_comp_data(func_list, data_list, rep=3, number=1):
    ''' Function to compare the performance of different functions.

    :param func_list: list
        list with function names as strings
    :param data_list: list
        list with data set names as srings
    :param rep: int
        number of repetitions of the comparison
    :param number: int
        number of executions for every function
    :return:
    '''

    res_dict = dict()
    for index, func_name in enumerate(func_list):
        stmt = func_name + '(' + data_list[index] + ')'
        setup = "from __main__ import " + func_name + '; ' + data_list[index]
        results = repeat(stmt=stmt, setup=setup, repeat=rep, number=number)
        res_dict[func_name] = sum(results) / rep


    for func_name, time in res_dict.items():
        print('Function: ' + func_name + ', average runtime sec: %9.5s' % str(time))


def f(x):
    return abs(math.cos(x)) ** .5 + math.sin(2 + 3 * x)

def f1(a):
    res = []
    for x in a:
        res.append(f(x))
    return res

def f2(a):
    return [f(x) for x in a]

def f3(a):
    ex = 'abs(math.cos(x)) ** .5 + math.sin(2 + 3 * x)'
    return [eval(ex) for x in a]

func_list = ['f1', 'f2', 'f3']
data_list = ['a=[x for x in range(100)]'] * 3

performance_comp_data(func_list=func_list, data_list=data_list)


