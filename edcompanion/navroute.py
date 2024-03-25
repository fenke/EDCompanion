#pylint: disable=missing-module-docstring
#pylint: disable=missing-function-docstring
#pylint: disable=invalid-name

import os
import json
import logging



def edc_navigationroute(journalpath):
    '''Iterable for Navigation Route files'''

    syslog = logging.getLogger(f"root.{__name__}")
    currentdb = {}
    try:
        try:
            with open('collected_neutron_systems.json', "rt") as jsonfile:
                currentdb=json.load(jsonfile)
        except Exception as x:
            currentdb = {}

        navfile = os.path.join(journalpath, "NavRoute.json")
        syslog.debug(f"\Reading Navigation Route from {navfile}")
        if os.path.exists(navfile):
            with open(navfile, "rt") as jsonfile:
                for item in json.load(jsonfile).get('Route'):
                    if item.get('StarClass','') == 'N':
                        currentdb.update({item.get('StarSystem'):item})
                        
                    yield item

        try:
            with open('collected_neutron_systems.json', "wt") as jsonfile:
                json.dump(currentdb, jsonfile)
        except Exception as x:
            pass

    except KeyboardInterrupt as kbi:
        syslog.debug(f"Keyboard Interrupt")
        pass
