import sys
import os

from playsound import playsound

from  edcompanion import navroute
from  edcompanion import events
from edcompanion.events import edc_track_journal
from edcompanion.timetools import make_datetime, make_naive_utc
from edcompanion.edsm_api import get_edsm_info, distance_between_systems

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

def follow_journal():
    jumptimes = []
    system_name = ''
    system_factions = []
    entrytime = 0
    body_id = 0
    jumpdistance = 10
    mission_advice = ""

    for event in edc_track_journal(logpath, backlog=3):
        timestamp = make_datetime(event.pop("timestamp"))
        eventname = event.pop("event")

        if not jumptimes:
            entrytime=timestamp.timestamp()
            jumptimes.append(timestamp.timestamp())
        if not system_name and eventname == 'Location':
            system_name = event.get('StarSystem')

        sys.stdout.write(
            f"\r{str(timestamp)[:-6]:20} "+
            f"{timestamp.timestamp()-jumptimes[-1]:7,.0F} | {eventname:12} | {system_name:18} | ")
        if eventname == 'Journal':
            sys.stdout.write(f"{event.get('filename')}\n")
        elif eventname == 'Fileheader':
            sys.stdout.write(f"{'Odyssey' if event.get('Odyssey', False) else 'Horizons' }\n")

        elif eventname == 'FSDJump':
            entrytime=timestamp.timestamp()
            body_id = event.get('BodyID')
            system_name = event.get('StarSystem','')
            jumptimes.append(entrytime)
            if mission_advice and not [True for item in navroute.edc_navigationroute(logpath) if item.get('StarSystem') == mission_advice]:
                mnames = ', '.join([m.get('LocalisedName', '') for i,m in missions.items() if mission_advice==m.get('DestinationSystem') ])
                sys.stdout.write(f"Travel to {mission_advice} for \"{mnames}\"\n")
                continue
            system_factions = [f.get('Name','') for f in sorted(event.get('Factions',[{}]),key=lambda X: -X.get('Influence',0))]
            if len(system_factions) > 1:
                sys.stdout.write(f"Faction: {system_factions[0]}")


            sys.stdout.write(f"\n")
            #sys.stdout.write(f"{system_name:22}\n")
            continue

        elif eventname == 'LoadGame':
            entrytime=timestamp.timestamp()
            jumptimes.append(entrytime)
            #sys.stdout.write(f"\n")

        elif eventname == 'StartJump' and event.get("JumpType") == "Hyperspace":
            system_name = event.get('StarSystem','')
            sys.stdout.write(f"{system_name:22}\n")
            continue

        elif eventname == 'Scan' and event.get('BodyID') == body_id:
            sys.stdout.write(f"Class {event.get('StarType')}{event.get('Subclass')}")
            if not event.get('WasDiscovered'):
                sys.stdout.write(f"\t{'Undiscovered'}\n")
                playsound('./sound88.wav')
            else:
                sys.stdout.write(f"\t{'Previously Discovered'}\n")

            continue

        elif 'FSSSignalDiscovered' == eventname:
            signal = event.get("SignalName_Localised", None)
            if signal:
                if signal not in signals:
                    signals[signal] = set()
                signals.get(signal).add((system_name,system_factions[0] if system_factions else '-'))

        elif 'scan' in eventname.lower():
            if not event.get("WasDiscovered", True):
                if event.get('TerraformState') == 'Terraformable' or planet_values.get(
                    event.get("WasDiscovered"),{}).get(
                    event.get("WasMapped"),{}).get(
                    bool(event.get('TerraformState') == 'Terraformable'),{}).get(
                    event.get("PlanetClass"),0) > 0:

                    sys.stdout.write(f"{event.get('BodyName').replace(system_name,'')} {event.get('PlanetClass')} {event.get('TerraformState')}\n")

            if False and "Resource" in json.dumps(event):
                print(eventname, event)
                break



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
    follow_journal()
    print(f"\nDone")
