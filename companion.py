
import curses
from curses import wrapper
from edcompanion.events import edc_track_journal

import logging

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d \t%(threadName)s\t%(name)s\t%(lineno)d\t%(levelname)s\t%(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.INFO
)

logging.captureWarnings(True)

def main(stdscr):
    syslog = logging.getLogger(f"root.{__name__}")
    stdscr.clear()
    for i in range(0,11):
        v = i-10
        print(f"10 divide by {v} is {10/v:2}")

    stdscr.refresh()
    stdscr.getkey()
    return

if __name__ == "__main__":
    wrapper(main)