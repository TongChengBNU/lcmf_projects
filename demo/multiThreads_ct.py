def thread_model():
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








