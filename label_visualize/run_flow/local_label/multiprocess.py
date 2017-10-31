from multiprocessing import Process, Manager
import time
import itertools
import json

def do_work(in_queue, out_list):
    while True:
        item = in_queue.get()

        line_no, line = item
        print('line_no:' , line_no)
        print(line)

        # exit signal
        #if line == None:
        #print(line)
        if 'None' in line :
            #print('exit')
            return

        # fake work
        time.sleep(.5)
        result = (line_no, line)
        #print(result)

        out_list.append(result)


if __name__ == "__main__":
    num_workers = 4

    manager = Manager()
    results = manager.list()
    work = manager.Queue(num_workers)

    # start for workers
    pool = []
    for i in range(num_workers):
        p = Process(target=do_work, args=(work, results))
        p.start()
        pool.append(p)

    # produce data
    #with open("/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON/2017-06-01/ads_creatives_audit_content_2017-06-01.json") as f:
    with open("/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON/2017-06-01/abc.txt") as f:
        work_json = json.load(f)
        #print(type(work_json))
        iters = itertools.chain(work_json['my_json'], ({'None'},)*num_workers)
        #iters = itertools.chain(f, (None,)*num_workers)
        for num_and_line in enumerate(iters):
            #print(num_and_line)
            work.put(num_and_line)

    for p in pool:
        p.join()

    # get the results
    # example:  [(1, "foo"), (10, "bar"), (0, "start")]
    #print(sorted(results))
