#pylint: disable=missing-module-docstring
#pylint: disable=missing-function-docstring
#pylint: disable=invalid-name

import os
import time
import glob
import json
import logging
import datetime
import ntpath
from functools import reduce

syslog = logging.getLogger(f"root.{__name__}")

def loads_jsonline(line):
    return json.loads(line[0:-2]) if line[-2] == "," else json.loads(line)

def edc_list_journals(journalpath):
    return sorted(
            [os.path.join(journalpath, f) for f in os.listdir(journalpath) if 'Journal' in f.split('.')[0] and '.log' in f],
            key=lambda f:f.replace('-', '').replace('Journal.20', 'Journal.').replace('T','')
        )

def edc_track_journal(journalpath, backlog=0):
    '''Iterable for Journal events'''

    syslog = logging.getLogger(f"root.{__name__}")
    try:

        #logfiles = sorted(glob.glob(os.path.join(journalpath, journalglob)))
        logfiles = sorted(
            [os.path.join(journalpath, f) for f in os.listdir(journalpath) if 'Journal' in f.split('.')[0] and '.log' in f],
            key=lambda f:f.replace('-', '').replace('Journal.20', 'Journal.').replace('T','')
        )

        if isinstance(backlog, int):
            backlog = min(backlog, len(logfiles)-1)
            syslog.info(f"Reading journals, backlog = {backlog}")
            return edc_read_journal(logfiles[-(1+backlog):])
        elif isinstance(backlog, str):
            syslog.info(f"Reading journals, backlog = {backlog}")
            return edc_read_journal(reduce(
                lambda t, j: t if not t and backlog not in j else t + [j],
                logfiles, []
            )[1:])


    except KeyboardInterrupt as kbi:
        syslog.info(f"Keyboard Interrupt {kbi.info()}")
        pass

def edc_read_journal(journal, notail=False):
    syslog = logging.getLogger(f"root.{__name__}")
    
    try:
        syslog.info(f"\nReading journal: {journal}")
        with open(journal, encoding="utf-8") as journalfile:
            # yield dict(
            #     event='Journal',
            #     timestamp=datetime.datetime.utcnow().isoformat(),
            #     filename=f"{ntpath.basename(journal)}"

            # )
            while True:# not shutdown_seen:
                line = journalfile.readline()
                if not line:
                    if notail:
                        break
                    
                    time.sleep(0.3)
                    continue

                if len(line) < 5:
                    continue

                try:
                    event = json.loads(line)

                except json.decoder.JSONDecodeError as JX:
                    print(JX)
                    print(line)
                    return

                yield event
        
                if event.get('event', '') == 'Shutdown':
                    syslog.info(f"SHUTDOWN {event.get('timestamp'):22} {journal}")
                    break

        yield dict(
            event='JournalFinished',
            timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            filename=f"{ntpath.basename(journal)}"
        )


    except KeyboardInterrupt as kbi:
        syslog.info(f"Keyboard Interrupt")
        yield dict(
            event='KeyboardInterrupt',
            timestamp=datetime.datetime.utcnow().isoformat(),
            filename=f"{ntpath.basename(journal)}"
        )

# 

# Database ===========================================

