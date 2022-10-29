#pylint: disable=missing-module-docstring
#pylint: disable=missing-function-docstring
#pylint: disable=invalid-name

import os
import json
import logging


def edc_navigationroute(journalpath):
    '''Iterable for Navigation Route files'''

    syslog = logging.getLogger(f"root.{__name__}")
    try:
        navfile = os.path.join(journalpath, "NavRoute.json")
        syslog.debug(f"\Reading Navigation Route from {navfile}")
        with open(navfile, "rt") as jsonfile:

            for item in json.load(jsonfile).get('Route'):
                yield item

    except KeyboardInterrupt as kbi:
        syslog.debug(f"Keyboard Interrupt")
        pass
