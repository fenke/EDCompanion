import sys
import os
import statistics
from functools import reduce
import numpy as np
import pandas as pd
import requests

from playsound import playsound

from edcompanion import init_console_logging
from  edcompanion import navroute
from  edcompanion import events
from edcompanion.events import edc_track_journal
from edcompanion.timetools import make_datetime, make_naive_utc
from edcompanion.edsm_api import get_edsm_info, distance_between_systems

syslog = init_console_logging(__name__)
logpath = "/Users/fenke/Saved Games/Frontier Developments/Elite Dangerous"

planet_values = {
    False: { # was-not-discovered
        False: {# was-not-mapped
            False: { # is-not-terraformable
                'Water world': 1559138,
                'Earthlike body': 4224870,
                'Ammonia world': 2242455,
            },
            True : { # is-terraformable
                'Water world': 4198704,
                "High metal content body": 2562654,
                "Rocky body": 2024270
            }
        },

        True: {# was-mapped, this specic combo is rubbish
        }
    },
    True: { # was-discovered
        False: {# was-not-mapped
            False: { # is-not-terraformable
                'Water world': 1312209,
                'Earthlike body': 3555753,
                'Ammonia world': 1887305,
            },
            True : { # is-terraformable
                'Water world': 3533732,
                "High metal content body": 2156792,
                "Rocky body": 1703675
            }
        },

        True: {# was-mapped
            False: { # is-not-terraformable
                'Water world': 540297,
                'Earthlike body': 1464068,
                'Ammonia world': 777091,
            },
            True : { # is-terraformable
                'Water world': 1455001,
                "High metal content body": 888051,
                "Rocky body": 701482
            }
        }
    }
}


rankings = dict(
    Progress = dict(),
    Rank = dict()

)
modules = []
ships = {}
current_ship = None
my_materials = {}
missions = {}
completed = {}
signals = {}
saascan = {}
fuel_used = []
fuel_level = []
fuel_capacity = 0

def follow_journal():
    starpos = np.asarray([0,0,0])
    navi_route = {item.get('StarSystem'):item for item in navroute.edc_navigationroute(logpath)}
    jumptimes = []
    system_name = ''
    system_factions = []
    entrytime = 0
    body_id = 0
    jumpdistance = 10
    mission_advice = ""
    edastro_poi = {}
    print(f"Reading Points-Of-Interest from EDAstro ...")
    
    req = requests.get('https://edastro.com/gec/json/all')

    _edastro_poi = req.json()
    edastro_poi = {
        item.get('galMapSearch'):{
            c:item.get(k)
            for c,k in zip(['coordinates','name','region','type','summary'], ['coordinates','name','region','type','summary'])
        }
        for item in _edastro_poi
    }    
    
    kb_stop = False
    last_journal = ''
    while not kb_stop:

        for event in edc_track_journal(logpath, backlog=0):
            timestamp = make_datetime(event.pop("timestamp"))
            eventname = event.pop("event")

            if not jumptimes:
                entrytime=timestamp.timestamp()
                jumptimes.append(timestamp.timestamp())
            if eventname == 'Location' or eventname == 'FSDJump':
                system_name = event.get('StarSystem')
                if system_name in navi_route:
                    navi_route.pop(system_name)

            starpos = np.asarray(event.get('StarPos', starpos))

            sys.stdout.write(
                f"\r{str(timestamp)[:-6]:20} "+
                f"{timestamp.timestamp()-jumptimes[-1]:7,.0F} | {eventname:21} | {system_name:28} | ")

            if eventname == 'KeyboardInterrupt':
                sys.stdout.write(f"Keyboard Interrupt {event.get('filename')}\n")
                kb_stop = True
                break

            elif eventname == 'JournalFinished':
                sys.stdout.write(f"Journal read {event.get('filename')}\n")
                last_journal = event.get(event.get('filename',''))
                kb_stop = True

            elif eventname == 'Fileheader':
                sys.stdout.write(f"{'Odyssey' if event.get('Odyssey', False) else 'Horizons' }\n")

            if eventname == 'FSDJump':
                entrytime=timestamp.timestamp()
                body_id = event.get('BodyID')

                if system_name in navi_route:
                    navi_system = navi_route.pop(system_name)
                else:
                    navi_system = {}
                    
                jumptimes.append(entrytime)
                fuel_used.append(event.get('FuelUsed'))
                fuel_level.append(event.get('FuelLevel'))

                if mission_advice and not [True for item in navroute.edc_navigationroute(logpath) if item.get('StarSystem') == mission_advice]:
                    mnames = ', '.join([m.get('LocalisedName', '') for i,m in missions.items() if mission_advice==m.get('DestinationSystem') ])
                    sys.stdout.write(f"Travel to {mission_advice} for \"{mnames}\"\n")
                    continue
                system_factions = [f.get('Name','') for f in sorted(event.get('Factions',[{}]),key=lambda X: -X.get('Influence',0))]
                if len(system_factions) > 1:
                    sys.stdout.write(f"Faction: {system_factions[0]:32}\n")
                    continue

                navi_fuel = {s:np.sqrt(np.sum(np.square(np.asarray(i.get('StarPos'))-starpos))) for s, i in navi_route.items() if i.get('StarClass') in 'KGBFOAM'}
                if navi_fuel:
                    fuel_system = reduce(
                        lambda total, item:item if navi_fuel[item] < navi_fuel[total] else total,
                        list(navi_fuel)
                    )
                    fuel_ratio = round(100*fuel_level[-1] / fuel_capacity,1)
                    est_jumps = round(fuel_level[-1] / (.1+statistics.mean(fuel_used)),1)
                    sys.stdout.write(f"fuel {fuel_ratio}% -> {est_jumps} jumps, fuelstar {fuel_system} at {round(navi_fuel[fuel_system],1)} ly") # FuelCapacity
                    if est_jumps < 3:
                        playsound('./sound88.wav')

                if system_name in edastro_poi:
                    sys.stdout.write(f"Poi: {edastro_poi[system_name].get('name')}, {edastro_poi[system_name].get('type')} \n\t{edastro_poi[system_name].get('summary')}\n")
                else:
                    sys.stdout.write(f"\n")

                continue

            elif "NavRoute" in eventname:
                navi_route = {item.get('StarSystem'):item for item in navroute.edc_navigationroute(logpath)}
                navi_distances = {s:np.sqrt(np.sum(np.square(np.asarray(i.get('StarPos'))-starpos))) for s, i in navi_route.items()}


            elif eventname == 'LoadGame':
                entrytime=timestamp.timestamp()
                jumptimes.append(entrytime)
                fuel_level.append(event.get('FuelLevel'))
                fuel_capacity = event.get('FuelCapacity')
                #sys.stdout.write(f"\n")

            elif eventname == 'StartJump' and event.get("JumpType") == "Hyperspace":
                if system_name in navi_route:
                    navi_route.pop(system_name)
                sys.stdout.write(f"{event.get('StarSystem',''):25} | class {event.get('StarClass','')}\n") # FuelCapacity

            elif eventname == 'FSDTarget':
                sys.stdout.write(f"{event.get('Name',''):25} | class {event.get('StarClass','')}, {event.get('RemainingJumpsInRoute','')} jumps remaining.\n")

            elif eventname == 'FuelScoop':
                fuel_level.append(event.get('Total'))
                fuel_ratio = round(100*event.get('Total') / fuel_capacity,1)
                est_jumps = round(fuel_level[-1] / (.1+statistics.mean(fuel_used)),1)
                sys.stdout.write(f"{fuel_ratio:3}% -> {est_jumps} jumps \n") # FuelCapacity

            elif eventname == 'Scan' and event.get('BodyID') == body_id:
                sys.stdout.write(f"Class {event.get('StarType')}{event.get('Subclass')}")
                if not event.get('WasDiscovered'):
                    sys.stdout.write(f"\t{'Undiscovered'}\n")
                    playsound('./sound88.wav')
                else:
                    sys.stdout.write(f"\t{'Was Discovered'}\n")

                continue

            elif 'FSSSignalDiscovered' == eventname:
                signal = event.get("SignalName_Localised", None)
                if signal:
                    if signal not in signals:
                        signals[signal] = set()
                    signals.get(signal).add((system_name,system_factions[0] if system_factions else '-'))
            elif 'SAAScanComplete' == eventname:
                if system_name not in saascan:
                    saascan[system_name] = {}
                saascan[system_name][event.get('BodyName')] = event

            elif 'scan' in eventname.lower():
                if not event.get("WasDiscovered", True):
                    if event.get('TerraformState') == 'Terraformable' or planet_values.get(
                        event.get("WasDiscovered"),{}).get(
                        event.get("WasMapped"),{}).get(
                        bool(event.get('TerraformState') == 'Terraformable'),{}).get(
                        event.get("PlanetClass"),0) > 0:
                        saas = saascan.get(system_name,{}).get(event.get('BodyName'),{})
                        sys.stdout.write(f"{event.get('BodyName').replace(system_name,'').strip()} {event.get('PlanetClass')} {event.get('TerraformState')} ")
                        if saas:
                            sys.stdout.write(f"{saas.get('ProbesUsed')}/{saas.get('EfficiencyTarget')} probes ")
                        sys.stdout.write('\n')



                continue

            elif 'Screenshot' in eventname:
                sys.stdout.write(f"{event.get('Body')} {event.get('Filename')}\n")
                continue

            elif 'Interdict' in eventname:
                sys.stdout.write(f"{event.get('Interdictor'):22}\n")
                continue

            elif 'Mission' in eventname:
                if eventname == 'Missions':
                    active_missions = set([M.get('MissionID', 0) for M in event.get('Active',[])])
                    for mid in active_missions:
                        if mid not in missions:
                            missions[mid] = {}

                    for mid in set(missions):
                        if mid not in active_missions:
                            missions.get(mid).update({
                                eventname: timestamp.isoformat()
                            })
                            completed[mid] = missions.pop(mid)
                    if not missions:
                        mission_advice = ''
                else:
                    if eventname == 'MissionAbandoned' or  eventname == 'MissionCompleted' :
                        completed[event.get('MissionID')] = missions.pop(event.get('MissionID'))
                        if not missions:
                            mission_advice = ''

                    elif eventname == 'MissionAccepted':
                        missions[event['MissionID']]={
                            k:event.get(k, '').split('$')[0] for k in ['LocalisedName', 'Expiry', 'DestinationSystem', 'DestinationStation']
                        }
                        #sys.stdout.write(f"({len(missions)}) {missions[event['MissionID']].get('LocalisedName')} -> {missions[event['MissionID']].get('DestinationSystem')}\n")

                    elif eventname == 'MissionRedirected':
                        missions[event['MissionID']].update({
                            k:event.get('New'+k) for k in ['DestinationSystem', 'DestinationStation']
                        })
                        #sys.stdout.write(f"({len(missions)}) {missions[event['MissionID']].get('LocalisedName')} -> {missions[event['MissionID']].get('DestinationSystem')}\n")

                    missions.get(event['MissionID'], {}).update({
                        eventname: timestamp.isoformat(),
                        'coords': get_edsm_info(missions.get(event['MissionID'], {}).get('DestinationSystem')).get('coords',[])
                    })
                continue
                ordered_routes = get_mission_routes(system_name, missions, jumpdistance=jumpdistance)
                if ordered_routes:
                    best_route = ordered_routes[0]
                    if best_route:
                        s1, s2 = best_route[0]
                        mission_advice = s2
                        mnames = ', '.join([m.get('LocalisedName', '') for i,m in missions.items() if s2==m.get('DestinationSystem') ])
                        sys.stdout.write(f"Travel to {s2} ({1+math.floor(distance_between_systems(s1, s2)/jumpdistance)}) for \"{mnames}\"\n")
                        continue
                else:
                    mission_advice = ''
                #sys.stdout.write(f"({len(missions)}) {missions.get(event['MissionID']).get('LocalisedName')} -> {missions.get(event['MissionID']).get('DestinationSystem')}\n")

            elif eventname == 'StoredModules' :
                #sys.stdout.write(f"{event.get('StationName',''):22}\n")
                modules = event.get('Items',[{}])
                continue

            elif ('Loadout' == eventname ):
                ships[event.get('ShipID')] = event.copy()
                current_ship = ships[event.get('ShipID')]
                jumpdistance = round(event.get('MaxJumpRange', 10),1)
                sys.stdout.write(f"{event.get('Ship',''):15} {event.get('ShipIdent',''):6} {event.get('MaxJumpRange',''):5,.1F} ly\n")
                continue

            elif (eventname == 'Rank' or eventname == 'Progress'):
                rankings[eventname].update(event)
                continue

            elif eventname == "Materials":
                my_materials = {T:{I.get("Name"):I.get("Count") for I in event.get(T,[])} for T in ['Raw','Encoded','Manufactured']}
                #my_materials = {I.get("Name_Localised", I.get("Name")):I.get("Count") for I in item.get('Raw',[]) + item.get("Encoded",[])}
                continue


if __name__ == "__main__":
    try:
        follow_journal()
    except KeyboardInterrupt as kbi:
        syslog.info(f"Keyboard Interrupt {kbi.info()}")
    print(f"\nDone")
