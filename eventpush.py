import os, sys, logging, requests
from edcompanion.events import edc_list_journals, edc_read_journal
from edcompanion.threadworker import create_threaded_worker

syslog = logging.getLogger("root." + __name__)
receiver = os.getenv("EVENT_RECEIVER_ENDPOINT", "http://localhost:8080")

def create_sending_worker(logfile):

    event_buffer = []
    req_params = dict(
        journal_name=os.path.basename(logfile),
        journal_id=None,
        token=os.getenv("USR_TOKEN")
    )

    def send_event(event):

        nonlocal req_params, event_buffer
        event_buffer.append(event)

        req = requests.post(
            f"{receiver}/event",
            params=req_params,
            json=event_buffer
        )

        if req.status_code == 200:
            answer = req.json()
            answered_id = answer.get('journal_id', '')
            if answered_id:
                req_params['journal_id'] = answered_id
                event_buffer = []

    return create_threaded_worker(send_event)

def push_events(journalpath, backlog=0):

    logfiles = edc_list_journals(edlogspath)
    logfiles = logfiles[-(1+min(backlog, len(logfiles)-1)):]
    syslog.info(f"Reading {len(logfiles)} journals")
    kb_stop = False
    send_queue = None

    for logfile in logfiles:

        if kb_stop:
            break

        send_queue = create_sending_worker(os.path.basename(logfile))
        send_queue.start()
        sys.stdout.write(f"\nStarted send queue for {os.path.basename(logfile)}...\n")

        for event in edc_read_journal(logfile, notail=(backlog > 0)):
            send_queue.put(event.copy())

        send_queue.stop()
        sys.stdout.write(f"\nJoining send queue ...\n")
        send_queue.join()


edlogspath = os.path.join(os.getenv('HOME', os.getenv('USERPROFILE')), 'Saved Games', 'Frontier Developments', 'Elite Dangerous')
syslog.info("Python executable %s running from %s with logs in %s", sys.executable, os.getcwd(), edlogspath)

if __name__ == "__main__":
    try:
        push_events(edlogspath, backlog=0)
    except KeyboardInterrupt as kbi:
        syslog.info(f"Keyboard Interrupt {kbi.info()}")
    print(f"\nDone")
