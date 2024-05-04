

import sys
import os
import time
import json
import statistics
from functools import reduce
import numpy as np
import pandas as pd
import requests

from playsound import playsound as original_play_sound

from edcompanion import init_console_logging
from edcompanion import navroute, events
from edcompanion.events import edc_track_journal
from edcompanion.timetools import make_datetime, make_naive_utc
from edcompanion.edsm_api import get_edsm_info, distance_between_systems, get_edsm_system_risk, get_commander_position, get_systems_in_cube
from edcompanion.threadworker import create_threaded_worker

syslog = init_console_logging(__name__)
edlogspath = os.path.join(os.getenv('HOME', os.getenv('USERPROFILE')), 'Saved Games', 'Frontier Developments', 'Elite Dangerous')
#"/Users/fenke/Saved Games/Frontier Developments/Elite Dangerous"

import math
from itertools import permutations

def parse_route_systems(system_name, mission_db):
    return { **{
        system_name:np.asarray([get_edsm_info(system_name, verbose=False).get('coords',{}).get(k) for k in ['x', 'y', 'z']])
    }, **{
        s.get('DestinationSystem'):np.asarray([get_edsm_info(s.get('DestinationSystem'), verbose=False).get('coords',{}).get(k) for k in ['x', 'y', 'z']])
        for s in mission_db.values() if s.get('coords',{})
    }}

def calculate_total_jumps(*route_points, jumpdistance=25):
    return sum([1+math.floor(distance_between_systems(*rp)/jumpdistance) for rp in route_points])

def get_mission_routes(start_system, mission_db, jumpdistance):
    route_systems = parse_route_systems(start_system, mission_db)
    all_routes = [r for r in permutations(route_systems) if r[0] == start_system]
    return sorted([[(x,y) for x,y in zip(r, r[1:])] for r in all_routes], key=lambda R:calculate_total_jumps(*R, jumpdistance=jumpdistance))


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

def init_poi_search():
    syslog.debug(f"Reading Points-Of-Interest from EDAstro ...")

    req = requests.get('https://edastro.com/gec/json/all')

    edastro_poi = {
        item.get('galMapSearch'):{
            c:item.get(k)
            for c,k in zip(['coordinates','name','region','type','summary'], ['coordinates','name','region','type','summary'])
        }
        for item in  req.json()
    }
    poi_systems = np.asarray([
        i.get('coordinates')
        for k, i in edastro_poi.items() ])
    poi_systemnames = np.asarray([
        (k, i.get('name'),  i.get('summary'))
        for k, i in edastro_poi.items() ])

    def find_nearest_poi(coord1, coord2=None):
        p1 = np.asarray(coord1)
        p2 = np.asarray(coord2) if coord2 is not None else coord1

        poi_distances = np.linalg.norm(poi_systems[:, [0,1,2]] - p1, axis=1) + np.linalg.norm(poi_systems[:, [0,1,2]] - p2, axis=1)
        ordering = poi_distances.argsort()
        return poi_systems[ordering][0], poi_systemnames[ordering][0]

    return edastro_poi, find_nearest_poi

rankings = dict(
    Progress = dict(),
    Rank = dict()

)
modules = []
ships = {}
current_ship = None
my_materials = {}
completed = {}
signals = {}
bodysignals = {}
saascan = {}
fuel_used = []

sound_queue = create_threaded_worker(original_play_sound)
#bigtasks_queue = create_threaded_worker(lambda F, *args, **kwargs: F(*arg, **kwargs))

glines={
   "line_1": {
      "direction": [
         0.7694614082861055,
         0.025650634567776276,
         0.6381780207000499
      ],
      "support": [
         -1394.6640000000002,
         -445.08299999999997,
         13171.01
      ]
   },
   "line_0": {
      "direction": [
         0.9609153520443258,
         0.002797717724735249,
         0.2768282120396372
      ],
      "support": [
         5280.544,
         -227.47200000000004,
         1189.0379999999998
      ]
   }
}
try:
    with open(f"data/guardian/glines.json", 'rt') as jsonfile:
        glines = json.load(jsonfile)
    print(f"Load glines from disk...")
except:
    pass

def distance_0(point):
    return round(np.linalg.norm(np.cross(glines['line_0']['direction'], np.asarray(point)-glines['line_0']['support']))/np.linalg.norm(glines['line_0']['direction']),2)

def distance_1(point):
    return round(np.linalg.norm(np.cross(glines['line_1']['direction'], np.asarray(point)-glines['line_1']['support']))/np.linalg.norm(glines['line_1']['direction']),2)

def nearest_point_on_1(point):
    dp = np.dot(np.asarray(point)-np.asarray(glines['line_1']['support']), np.asarray(glines['line_1']['direction']) )
    return np.round(dp*np.asarray(glines['line_1']['direction']) + np.asarray(glines['line_1']['support']),1)

def nearest_point_on_0(point):
    return np.dot(np.asarray(point)-glines['line_0']['support'], glines['line_0']['direction'] ) + glines['line_0']['support']


def follow_journal(backlog=0, verbose=False):
    sound_queue.start()
    def playsound(*args, **kwargs):
        if verbose:
            sound_queue.put(*args, **kwargs)

    starpos = np.asarray([0,0,0])
    guardian_system = False
    navi_route = {item.get('StarSystem'):item for item in navroute.edc_navigationroute(edlogspath)}

    guardian_systems = {}
    try:
        with open('guardian_systems.json', "rt") as jsonfile:
            guardian_systems=json.load(jsonfile)
    except Exception as x:
        sys.stdout.write(f"Error {x}\n")


    missions = {}
    try:
        with open('missions.json', "rt") as jsonfile:
            missions=json.load(jsonfile)
    except Exception as x:
        sys.stdout.write(f"Error {x}\n")

    completed = {}
    try:
        with open('completed.json', "rt") as jsonfile:
            completed=json.load(jsonfile)
    except Exception as x:
        sys.stdout.write(f"Error {x}\n")

    bodysignals = {}
    try:
        with open('bodysignals.json', "rt") as jsonfile:
            bodysignals=json.load(jsonfile)
    except Exception as x:
        sys.stdout.write(f"Error {x}\n")

    signals = {}
    try:
        with open('signals.json', "rt") as jsonfile:
            signals=json.load(jsonfile)
    except Exception as x:
        sys.stdout.write(f"Error {x}\n")

    current_sector = ''
    sector = {}

    #systems = {}
    # try:
    #     with open('systems.json', "rt") as jsonfile:
    #         systems=json.load(jsonfile)
    # except Exception as x:
    #     sys.stdout.write(f"Error {x}\n")

    eventdump = {}
    signaldump ={}

    jumptimes = []
    system_name = ''
    system_factions = []
    fuel_level = []
    fuel_capacity = 1
    entrytime = 0
    body_id = 0
    jumpdistance = 10
    mission_advice = ""
    edastro_poi, find_nearest_poi = init_poi_search()


    kb_stop = False
    last_journal = ''

    # --------------------------------------------------------------
    while not kb_stop:

        # ----------------------------------------------------------
        for event in edc_track_journal(edlogspath, backlog=backlog):
            eventname = event.pop("event")

            # Finding guardian things to monitor --------------------
            if 'guardian' in str(event).lower():
                if eventname not in [
                    'Materials', 'Loadout', 'StoredModules',
                    'ModuleStore', 'ModuleRetrieve', 'ModuleBuy',
                    'ModuleSellRemote', 'MaterialCollected',
                    'CollectCargo', 'EjectCargo', 'Music', 'Cargo',
                    'Touchdown', 'Liftoff', 'ApproachSettlement',
                    'CodexEntry', 'Location', 'ModuleSell', 'Repair',
                    'TechnologyBroker', 'FetchRemoteModule', 'MaterialDiscovered',
                    'FetchRemoteModule', 'AfmuRepairs', 'JetConeDamage',
                    ''

                ] and not event.get('IsStation') and not 'Guardians of' in str(event):
                    if eventname not in eventdump:
                        eventdump[eventname]=[]
                    eventdump[eventname].append(event.copy())

            timestamp = make_datetime(event.pop("timestamp"))

            # general / state =============================================================
            if not jumptimes:
                entrytime=timestamp.timestamp()
                jumptimes.append(timestamp.timestamp())

            # Current location ------------------------------------------------------------
            if eventname == 'Location' or eventname == 'FSDJump':
                # set current star system
                system_name = event.get('StarSystem')
                if system_name in navi_route:
                    navi_route.pop(system_name)

                sector_name = system_name.split(' ')[0]
                sector_filename = os.path.join(os.getcwd(), 'data', 'sectors', sector_name + '.json')

                if current_sector != sector_filename:
                    if sector:
                        with open(current_sector, 'wt') as jsonfile:
                            json.dump(sector, jsonfile, indent=3)

                    current_sector = sector_filename

                    if os.path.exists(sector_filename):
                        with open(sector_filename, 'rt') as jsonfile:
                            sector = json.load(jsonfile)
                    else:
                        sector = {}


                if system_name not in sector:
                    body_id = str(event.get('BodyID',0))
                    sector[system_name] = dict(
                        StarPos=event.get('StarPos',[]),
                        bodies={body_id:dict(Body=event.get('Body'), BodyType=event.get('BodyType'))},
                        #stars={event.get('BodyID',0):{}} if bool(event.get('BodyType','') == 'Star') else {}
                    )
                    sector[system_name]['stars'] = {i:b for i,b in sector[system_name]['bodies'].items() if b.get('BodyType','') == 'Star'}

                system = sector[system_name]

                guardian_system = bool(event.get('SystemAllegiance', "").lower() == 'guardian')
                if guardian_system:
                    guardian_systems.update({
                        system_name: system.get('StarPos',[])
                    })

            # update state: system coordinates -----------------------------------------------
            starpos = np.asarray(event.get('StarPos', starpos))
            header = f"\r{str(timestamp)[:-6]:20} {timestamp.timestamp()-jumptimes[-1]:7,.0F} | {eventname:21} | {system_name:28} {'>' if guardian_system else '|'} "
            sys.stdout.write(header)

            #
            # A big CASE ===========================================================
            if eventname == 'KeyboardInterrupt':
                sys.stdout.write(f"Keyboard Interrupt {event.get('filename')}\n")
                kb_stop = True
                break

            elif eventname == 'JournalFinished':
                sys.stdout.write(f"Journal read {event.get('filename')}\n")
                last_journal = event.get(event.get('filename',''))
                kb_stop = True

            elif eventname == 'Fileheader':
                sys.stdout.write(f"{'Odyssey' if event.get('Odyssey', False) else 'Horizons' }\n{header}")

            if 'Signals' in event:
                _signals = event.get('Signals', [])
                _bodyname = event.get('BodyName', '')
                if _signals and _bodyname:
                    if eventname not in system:
                        system[eventname] = dict()
                    system[eventname][_bodyname] = _signals

                _counts = dict()
                for n,b in  system[eventname].items():
                    for s in b:
                        _counts[s.get('Type')] = _counts.get(s.get('Type'),0) + s.get('Count', 0)
                system[f"{eventname}_counts"] = _counts

                sys.stdout.write(f"Signals: {sum([c for c in _counts.values()])}")

            if eventname == 'FSDJump':
                entrytime=timestamp.timestamp()
                body_id = str(event.get('BodyID'))
                system["BodyID"] = body_id
                #system['stars'] = list(set(system.get('stars',[])).add(body_id))
                d1 = distance_1(event.get('StarPos'))
                if d1 < 2000:
                    sys.stdout.write(f"Distance to Line 1: {d1:8}")


                if system_name in navi_route:
                    navi_system = navi_route.pop(system_name)
                else:
                    navi_system = {}

                jumptimes.append(entrytime)
                fuel_used.append(event.get('FuelUsed'))
                fuel_level.append(event.get('FuelLevel'))

                # handle mission updates
                if mission_advice and not [True for item in navroute.edc_navigationroute(edlogspath) if item.get('StarSystem') == mission_advice]:
                    mnames = ', '.join(set([m.get('LocalisedName', '') for i,m in missions.items() if mission_advice==m.get('DestinationSystem') ]))
                    sys.stdout.write(f"Travel to {mission_advice} for \"{mnames}\"\n{header}")

                # "SystemAllegiance":"Guardian"

                guardian_system = bool(event.get('SystemAllegiance', "") == 'Guardian')
                if guardian_system:
                    sys.stdout.write(f"Guardian System\n{header}")
                    playsound('./sound88.wav')

                system_factions = [f.get('Name','') for f in sorted(event.get('Factions',[{}]),key=lambda X: -X.get('Influence',0))]
                if len(system_factions) > 1:
                    sys.stdout.write(f"Faction: {system_factions[0]:32}")

                # Points of interest
                if system_name in edastro_poi:
                    sys.stdout.write(f"Poi: {edastro_poi[system_name].get('name')}\n{edastro_poi[system_name].get('type'):20} | {edastro_poi[system_name].get('summary')}\n{header}")
                else:
                    sys.stdout.write(f"\n{header}")

                continue

            elif "NavRoute" in eventname:
                navi_route = {item.get('StarSystem'):item for item in navroute.edc_navigationroute(edlogspath)}
                if system_name in navi_route:
                    navi_system = navi_route.pop(system_name)
                else:
                    navi_system = {}

                #navi_distances = {s:np.sqrt(np.sum(np.square(np.asarray(i.get('StarPos'))-starpos))) for s, i in navi_route.items()}

                navi_distances_l1 =  {s:distance_1(i.get('StarPos')) for s, i in navi_route.items()}

                if len(navi_distances_l1) > 0:
                    min_d1, max_d1 = min(navi_distances_l1.values()), max(navi_distances_l1.values())
                    if min_d1 < 2000:
                        if len(navi_distances_l1) > 5:
                            sys.stdout.write(f"Distances to L1 between {min_d1} and {max_d1} \n{header}")
                        elif min_d1 > 100:
                            min_d1_entry = list(filter(lambda k: not navi_distances_l1[k] > min_d1, navi_distances_l1.keys()))[0]
                            nearest_point = nearest_point_on_1(navi_route.get(min_d1_entry,{}).get('StarPos'))
                            tsg1 = get_systems_in_cube(list(nearest_point), size=100)
                            if tsg1:
                                sys.stdout.write(f"Try aiming at {tsg1[0].get('name')} \n{header}")
                            else:
                                sys.stdout.write(f"Try aiming at {np.round(nearest_point,1)} \n{header}")

                        else:
                            sys.stdout.write(f"Distances to L1: {' / '.join([str(v) for v in navi_distances_l1.values()])} \n{header}")

            elif eventname == 'LoadGame':
                entrytime=timestamp.timestamp()
                jumptimes.append(entrytime)
                fuel_level.append(event.get('FuelLevel'))
                fuel_capacity = max(event.get('FuelCapacity',1),1)
                #sys.stdout.write(f"\n")

            elif eventname == 'StartJump' and event.get("JumpType") == "Hyperspace":
                if system_name in navi_route:
                    navi_route.pop(system_name)
                sys.stdout.write(f"{event.get('StarSystem',''):25} | class {event.get('StarClass','')}\n{header}") # FuelCapacity

            elif eventname == 'FSDTarget':
                sys.stdout.write(f"{event.get('Name',''):25} | class {event.get('StarClass','')}")

                risk = get_edsm_system_risk(event.get('Name',''))
                if risk > 1:
                    sys.stdout.write(f"-> High Risk {round(risk,2)}")

                # Fuel management assistance
                navi_fuel = {s:np.sqrt(np.sum(np.square(np.asarray(i.get('StarPos'))-starpos))) for s, i in navi_route.items() if i.get('StarClass') in 'KGBFOAM'}
                if navi_fuel:
                    fuel_system = reduce(
                        lambda total, item:item if navi_fuel[item] < navi_fuel[total] else total,
                        list(navi_fuel)
                    )
                    fuel_level = [l for l in fuel_level if l is not None]
                    fuel_ratio = round(100*fuel_level[-1] / fuel_capacity,1)
                    est_jumps = round(fuel_level[-1] / (.1+(statistics.mean(fuel_used) if fuel_used else 3)),1)
                    sys.stdout.write(f", fuel {fuel_ratio}% -> {est_jumps} jumps, fuel at {round(navi_fuel[fuel_system],1)} ly\n{header}")
                    if est_jumps < 3:
                        playsound('./sound88.wav')
                else:
                     sys.stdout.write(f", {event.get('RemainingJumpsInRoute','')} jumps remaining\n{header}")


            elif eventname == 'FuelScoop':
                fuel_level.append(event.get('Total'))
                fuel_ratio = round(100*event.get('Total') / fuel_capacity,1)
                est_jumps = round(fuel_level[-1] / (.1+statistics.mean(fuel_used)),1)
                sys.stdout.write(f"{fuel_ratio:3}% -> {est_jumps} jumps \n{header}") # FuelCapacity

            elif eventname == 'FSSDiscoveryScan':
                if eventname not in system:
                    system[eventname] = {}
                system[eventname].update({k:event.get(k) for k in ['BodyCount','NonBodyCount','Progress']})

                sys.stdout.write(f"Discovery {', '.join([str(k) + ': ' + str(v) for k,v in system[eventname].items() ])} \n{header}")

            elif eventname == 'Scan':
                scan_body_id = str(event.get('BodyID'))
                if scan_body_id not in system["bodies"]:
                    system["bodies"].update({scan_body_id:event})
                else:
                    system["bodies"][scan_body_id].update(event)

                if scan_body_id == body_id:
                    sys.stdout.write(f"Class {event.get('StarType')}{event.get('Subclass')} ")
                    if not event.get('WasDiscovered', True):
                        sys.stdout.write(f" \t{'Undiscovered'} \n{header}")
                        playsound('./sound88.wav')

                elif event.get('StarType'):
                    if scan_body_id not in system["stars"]:
                        system["stars"].update({scan_body_id:system["bodies"][scan_body_id]})
                    if event.get('StarType') not in 'KGBFOAM' and not event.get("WasDiscovered", True):
                        sys.stdout.write(f"{event.get('BodyName')}, class {event.get('StarType')}-{event.get('Subclass')}, solar-mass: {round(event.get('StellarMass'),1)} \n{header}")
                    elif len(system['stars'])>1:
                        sys.stdout.write(f"Stars: {'/'.join([system['bodies'].get(i,{}).get('StarType','')  for i in system['stars']])}\n{header}")

                elif not event.get("WasDiscovered", True):
                    if event.get('TerraformState') == 'Terraformable' or planet_values.get(
                        event.get("WasDiscovered"),{}).get(
                        event.get("WasMapped"),{}).get(
                        bool(event.get('TerraformState') == 'Terraformable'),{}).get(
                        event.get("PlanetClass"),0) > 0:

                        saas = saascan.get(system_name,{}).get(event.get('BodyName'),{})
                        sys.stdout.write(f"{event.get('BodyName').replace(system_name,'').strip()} {event.get('PlanetClass')} {event.get('TerraformState')} ")
                        if saas:
                            sys.stdout.write(f"{saas.get('ProbesUsed')}/{saas.get('EfficiencyTarget')} probes ")
                        else:
                            playsound('./sound88.wav')

                        sys.stdout.write(f'\n{header}')

            elif 'FSSBodySignals' == eventname:
                if system_name not in bodysignals:
                    bodysignals[system_name] = []
                bodysignals[system_name].append(event)

            # SAASignalsFound
            elif 'SAASignalsFound' == eventname:

                if 'guardian' in [S.get('Type_Localised').lower() for S in event.get('Signals',[]) if S.get('Type_Localised')]:
                    guardian_system = True
                    guardian_systems.update({
                        system_name: system.get('StarPos',[])
                    })

                    sys.stdout.write(f"Guardian Signals: {event.get('BodyName')}, count= {[S.get('Count') for S in event.get('Signals',[]) if S.get('Type_Localised')=='Guardian'][0]}\n{header}")

                    playsound('sonar-ping.wav')


            elif 'FSSSignalDiscovered' == eventname:
                signal = event.get("SignalName_Localised", None)
                if signal:
                    if signal not in signals:
                        signals[signal] = dict()
                    if system_name not in signals[signal]:
                        signals[signal][system_name] = []
                    signals.get(signal).get(system_name).append(event)

                    #.add((system_name,system_factions[0] if system_factions else '-'))

            elif 'SAAScanComplete' == eventname:
                if system_name not in saascan:
                    saascan[system_name] = {}
                saascan[system_name][event.get('BodyName')] = event

            elif 'Screenshot' in eventname:
                sys.stdout.write(f"{event.get('Body')} {event.get('Filename')}\n{header}")
                continue

            elif 'Interdict' in eventname:
                sys.stdout.write(f"{event.get('Interdictor', '??'):22}\n{header}")
                continue

            elif eventname == 'Music': # "event":"Music", "MusicTrack":"Combat_Unknown"
                sys.stdout.write(f"{event.get('MusicTrack'):22}")
                if event.get('MusicTrack') == 'Combat_Unknown':
                    sys.stdout.write(f"Uh-oh\n{header}")
                    playsound('./sound88.wav')
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
                        sys.stdout.write(f"({len(missions)}) {missions[event['MissionID']].get('LocalisedName')} -> {missions[event['MissionID']].get('DestinationSystem')}\n{header}")

                    elif eventname == 'MissionRedirected':
                        missions[event['MissionID']].update({
                            k:event.get('New'+k) for k in ['DestinationSystem', 'DestinationStation']
                        })
                        sys.stdout.write(f"({len(missions)}) {missions[event['MissionID']].get('LocalisedName')} -> {missions[event['MissionID']].get('DestinationSystem')}\n{header}")

                    missions.get(event['MissionID'], {}).update({
                        eventname: timestamp.isoformat(),
                        'coords': get_edsm_info(missions.get(event['MissionID'], {}).get('DestinationSystem'), verbose=False).get('coords',[])
                    })

                ordered_routes = get_mission_routes(system_name, missions, jumpdistance=jumpdistance)
                if ordered_routes:
                    best_route = ordered_routes[0]
                    if best_route:
                        s1, s2 = best_route[0]
                        mission_advice = s2
                        mnames = ', '.join(set([m.get('LocalisedName', '') for i,m in missions.items() if s2==m.get('DestinationSystem') ]))
                        sys.stdout.write(f"Travel to {s2} ({1+math.floor(distance_between_systems(s1, s2)/jumpdistance)}) for \"{mnames: <12}\"\n{header}")
                        continue
                else:
                    mission_advice = ''
                #sys.stdout.write(f"({len(missions)}) {missions.get(event['MissionID']).get('LocalisedName')} -> {missions.get(event['MissionID']).get('DestinationSystem')}\n")

            elif eventname == 'StoredModules' :
                #sys.stdout.write(f"{event.get('StationName',''):22}\n")
                modules = event.get('Items',[{}]).copy()
                with open('modules.json', "wt") as jsonfile:
                    json.dump(modules, jsonfile, indent=3)

                continue

            elif ('Loadout' == eventname ):
                ships[event.get('ShipID')] = event.copy()
                current_ship = ships[event.get('ShipID')]
                jumpdistance = round(event.get('MaxJumpRange', 10),1)
                sys.stdout.write(f"{event.get('Ship',''):15} {event.get('ShipIdent',''):6} {event.get('MaxJumpRange',''):5,.1F} ly\n{header}")
                fuel_level.append(event.get('FuelLevel'))
                fuel_capacity = max(event.get('FuelCapacity',{}).get('Main',1),1)
                all_ships = {}
                try:
                    with open('ships.json', "rt") as jsonfile:
                        all_ships = json.load(jsonfile)
                    all_ships.update(ships)
                except Exception as x:
                    pass
                finally:
                    with open('ships.json', "wt") as jsonfile:
                        json.dump(all_ships, jsonfile, indent=3)

                continue

            elif (eventname == 'Rank' or eventname == 'Progress'):
                rankings[eventname].update(event)
                continue

            elif eventname == "Materials":
                my_materials = {T:{I.get("Name"):I.get("Count") for I in event.get(T,[])} for T in ['Raw','Encoded','Manufactured']}
                #my_materials = {I.get("Name_Localised", I.get("Name")):I.get("Count") for I in item.get('Raw',[]) + item.get("Encoded",[])}
                continue

            if ('scan' in eventname.lower() or 'signal' in eventname.lower())  and 'guardian' in str(event).lower() and not event.get('IsStation'):
                if eventname not in signaldump:
                    signaldump[eventname]={}
                if system_name not in signaldump:
                    signaldump[eventname][system_name] = {}
                if event.get('BodyName', '-') not in signaldump[eventname][system_name]:
                    signaldump[eventname][system_name][event.get('BodyName', '-')] =[]

                signaldump[eventname][system_name][event.get('BodyName', '-')].append(event.copy())

                sys.stdout.write(f"Signal at {event.get('BodyName', event.get('SignalName', '??'))} {', '.join([s.get('Type_Localised') for s in event.get('Signals',[])])} \n{header}") # FuelCapacity

        sys.stdout.write(f"\nBye bye\n")

    sound_queue.stop()

    if sector and current_sector:
        try:
            with open(current_sector, 'wt') as jsonfile:
                json.dump(sector, jsonfile, indent=3)
        except Exception as x:
            syslog.exception('Error %s', str(x))

    # if systems:
    #     try:
    #         with open('systems.json', "wt") as jsonfile:
    #             json.dump(systems, jsonfile, indent=3)
    #     except Exception as x:
    #         syslog.exception('Error %s', str(x))
    #         sys.stdout.write(f"Error {x}\n")

    if missions:
        try:
            with open('missions.json', "wt") as jsonfile:
                json.dump(missions, jsonfile, indent=3)
        except Exception as x:
            syslog.exception('Error %s', str(x))

    if completed:
        try:
            with open('completed.json', "wt") as jsonfile:
                json.dump(completed, jsonfile, indent=3)
        except Exception as x:
            syslog.exception('Error %s', str(x))

    if bodysignals:
        try:
            with open('bodysignals.json', "wt") as jsonfile:
                json.dump(bodysignals, jsonfile, indent=3)
        except Exception as x:
            sys.stdout.write(f"Error {x}\n")

    if signals:
        try:
            with open('signals.json', "wt") as jsonfile:
                json.dump(signals, jsonfile, indent=3)
        except Exception as x:
            sys.stdout.write(f"Error {x}\n")

    if saascan:
        try:
            with open('saascan.json', "wt") as jsonfile:
                json.dump(saascan, jsonfile, indent=3)
        except Exception as x:
            sys.stdout.write(f"Error {x}\n")

    if eventdump:
        try:
            with open('eventdump.json', "wt") as jsonfile:
                json.dump(eventdump, jsonfile, indent=3)
        except Exception as x:
            sys.stdout.write(f"Error {x}\n")

    if signaldump:
        try:
            with open('signaldump.json', "wt") as jsonfile:
                json.dump(signaldump, jsonfile, indent=3)
        except Exception as x:
            sys.stdout.write(f"Error {x}\n")

    if guardian_systems:
        try:
            with open('guardian_systems.json', "wt") as jsonfile:
                json.dump(guardian_systems, jsonfile, indent=3)
        except Exception as x:
            sys.stdout.write(f"Error {x}\n")


    return sector, system_name

if __name__ == "__main__":
    try:
        follow_journal(backlog=0,verbose=True)
    except KeyboardInterrupt as kbi:
        syslog.info(f"Keyboard Interrupt {kbi.info()}")
    print(f"\nDone")
