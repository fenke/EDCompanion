import threading
import queue
import time

import logging

syslog = logging.getLogger("root." + __name__)

# Our thread-processing model consists of task queues
# with an associated thread to perform these tasks
def create_threaded_worker(workerfunc, **put_kwargs):
    """Creates a thread + task queue, workerfunction
    is called for each item put on the queue

    workerfunc: the function to be executed ofr each item
    **put_kwargs: additional keyword arguments for Queue.put()

    returns an object with the following members:
        start() starts the thread-loop
        put(item, **work_args) puts item and optional keyword arguments
            on the queue for processing by workerfunc()"""


    #syslog.info("Creating threaded worker for %s", str(workerfunc.__code__))

    # utility factory class
    class workerfactory(dict):
        def __getattr__(self, key):
            return self[key]

    stop_event = threading.Event()
    task_queue = queue.Queue()
    done_queue = queue.Queue()

    def put_item(*work_args, **work_kwargs):
        nonlocal task_queue
        task_queue.put(item=(work_args, work_kwargs), **put_kwargs)

    def get_item():
        try:
            item = done_queue.get_nowait()
            done_queue.task_done()
        except queue.Empty:
            item = None

        return item

    def workloop():
        nonlocal task_queue, workerfunc, done_queue, stop_event
        # open contexts for redirection of stdout and stderr into logging
        #syslog.debug("Starting thread loop for %s", str(workerfunc.__code__))

        sleepfor = 0.05
        sleepmax = 0.5
        # indefinately keep getting new items from the queue to process
        while True:
            try:
                item = ()
                try:
                    # by default queue.get() waits for new items
                    item = task_queue.get_nowait()
                    task_queue.task_done()
                    sleepfor = 0.05

                except queue.Empty:
                    if stop_event.is_set():
                        break
                    else:
                        time.sleep(sleepfor)
                        sleepfor = min(sleepfor * 1.2, sleepmax)

                if item:
                    try:
                        result = workerfunc(*item[0], **item[1])
                        if not result is None:
                            done_queue.put_nowait(result)

                    except queue.Full:
                        pass


            except Exception as x:
                syslog.exception("Exception: %s", x, exc_info=True, stack_info=True)

        syslog.debug('Ending thread for %s\n', str(workerfunc.__code__))

    task_processor = threading.Thread(target=workloop)

    def start_processing():
        nonlocal task_processor
        task_processor.start()

    def stop():
        nonlocal stop_event
        syslog.debug('Sending stop to thread for %s\n', str(workerfunc.__code__))
        stop_event.set()

    return workerfactory(
        start=task_processor.start,
        stop=stop,
        put=put_item,
        get_return=get_item,
        join=task_processor.join)

def create_producer_from_iterable(iterable, **put_kwargs):
    """Create a producer thread encapsulating an iterable"""

    class workerfactory(dict):
        def __getattr__(self, key):
            return self[key]

    stop_event = threading.Event()
    product_queue = queue.Queue(maxsize=15)

    def get_item():
        try:
            item = product_queue.get_nowait()
            product_queue.task_done()
        except queue.Empty:
            item = None

        return item
    
    def get_items():
        try:
            while True:
                item = product_queue.get_nowait()
                product_queue.task_done()
                yield item

        except queue.Empty:
            pass
    
    def stop():
        nonlocal stop_event
        syslog.debug('Sending stop to thread for %s\n', str(iterable))
        stop_event.set()

    def workloop():
        nonlocal product_queue, stop_event
        sleepfor = 0.05
        sleepmax = 0.5

        try:

            for item in iterable:
                if stop_event.is_set():
                    break

                item_done = False
                while not item_done:
                    if stop_event.is_set():
                        break

                    try:
                        product_queue.put_nowait(item)
                        item_done = True

                    except queue.Full:
                        time.sleep(sleepfor)
                        sleepfor = min(sleepfor * 1.2, sleepmax)

            syslog.debug('Iterable ended %s\n', str(iterable))

        except Exception as x:
            syslog.exception("Exception: %s", x, exc_info=True, stack_info=True)

        syslog.debug('Ending thread for %s\n', str(iterable))

    task_processor = threading.Thread(target=workloop)

    return workerfactory(
        processor=task_processor,
        start=task_processor.start,
        stop=stop,
        get_item=get_item,
        get_items = get_items,
        join=task_processor.join)
