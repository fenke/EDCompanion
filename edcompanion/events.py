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


def loads_jsonline(line):
    return json.loads(line[0:-2]) if line[-2] == "," else json.loads(line)

def edc_track_journal(journalpath, backlog=0):
    '''Iterable for Journal events'''

    syslog = logging.getLogger(f"root.{__name__}")
    try:

        #logfiles = sorted(glob.glob(os.path.join(journalpath, journalglob)))
        logfiles = sorted(
            [os.path.join(journalpath, f) for f in os.listdir(journalpath) if 'Journal' in f.split('.')[0] and '.log' in f],
            key=lambda f:f.replace('-', '').replace('Journal.20', 'Journal.').replace('T','')
        )
        backlog = min(backlog, len(logfiles)-1)
        current_journal = logfiles[-(1+backlog):]
        return edc_read_journal(current_journal)

    except KeyboardInterrupt as kbi:
        syslog.info(f"Keyboard Interrupt {kbi.info()}")
        pass

def edc_read_journal(journals):
    syslog = logging.getLogger(f"root.{__name__}")

    if not isinstance(journals, list):
        return edc_read_journal([journals])
    last_journal = journals[-1]
    try:
        for journal in journals:
            syslog.debug(f"\nReading journal: {journal}")
            with open(journal, "rt") as journalfile:
                # yield dict(
                #     event='Journal',
                #     timestamp=datetime.datetime.utcnow().isoformat(),
                #     filename=f"{ntpath.basename(journal)}"

                # )
                while True:# not shutdown_seen:
                    line = journalfile.readline()
                    if not line:
                        if journal != last_journal:
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

                    if event.get('event', '') == 'Shutdown':
                        syslog.debug(f"SHUTDOWN {event.get('timestamp'):22} {journal}")
                        break

                    yield event

    except KeyboardInterrupt as kbi:
        syslog.info(f"Keyboard Interrupt")
        pass