#pylint: disable=missing-module-docstring
#pylint: disable=missing-function-docstring
#pylint: disable=invalid-name
#pylint: disable=line-too-long


import collections
import sys
import os
import asyncpg
import math
import numpy as np
import pandas as pd
from numpy.lib.recfunctions import join_by
from numpy.core.records import fromarrays

from pgsqldata import PGSQLDataSourceEDDB

PathWayPoint = collections.namedtuple(
    "PathWayPoint",
    ["system", "distance", "remaining", "weight", "density"])

#--
async def generate_waypoints(
    start_system_name, target_system_name,
    cube_side=40, search_side=1200,
    pgsql_params = dict(
        url=os.getenv("PGSQL_URL"),
        server_settings={'search_path': "edsm"}
    ),
    logger=sys.stdout.write):

    datasource = PGSQLDataSourceEDDB(**pgsql_params)

    _target_system = await datasource.find_system(target_system_name)
    end = np.round(np.asarray([_target_system.get(k) for k in ["x","y","z"]]))

    _start_system = await datasource.find_system(start_system_name)
    start = np.round(np.asarray([_start_system.get(k) for k in ["x","y","z"]]))

    path = [start] # contains coordiantes of waypoints

    system_names = [_start_system.get('name')] # systemnames of waypoints
    path_info = {} # information about waypoints and algorithm execution


    # Add no mare then x waypoints
    pause_at = len(path)+7

    # distance from current path end to final target
    travel_distance = np.sqrt(np.sum(np.square(end-path[-1])))
    logger(f"{system_names[-1]} {path[-1]}\n")
    logger(f"\t{round(travel_distance)} ly -> {(await datasource.find_system(end.tolist())).get('name')} {np.round(end).tolist()}\n")

    # open DB connection
    async with asyncpg.connect(**pgsql_params) as pgconnection:

        while cube_side > 20:
            # Fix cube_side to be divisible by two (easier on our center points, though not neccesary)
            cube_side = round(2 * math.floor(cube_side/2))
            search_side = max(search_side, cube_side*9)
            cube_radius = np.sqrt(np.sum(np.square([cube_side/2, cube_side/2, cube_side/2])))
            def cube_center(point):
                return [round(cube_side*math.floor(v/cube_side) + cube_side/2) for v in point]
            def cube_location(point):
                return [round(cube_side*math.floor(v/cube_side) + cube_side/2) for v in point]

            # End of search condition
            while travel_distance > 6*cube_side:
                currentwp = cube_center(np.round(path[-1],2))

                travel_distance = np.sqrt(np.sum(np.square(end - currentwp)))
                direction = (end - currentwp)/travel_distance

                # first estimated waypoint
                wpdistance = math.floor(0.9*search_side/cube_side) * (
                    cube_side if len(path) > 2 and travel_distance > 2*search_side else cube_side/2)
                nextwp = cube_center(np.round(currentwp + min(wpdistance, travel_distance) * direction,2))
                sys.stdout.write(f"\nCurrent {str(currentwp)} to {str(nextwp)}")

                location_cube = [round(cube_side*math.floor(v/cube_side)) for v in currentwp]
                destination_cube = [round(cube_side*math.floor(v/cube_side)) for v in nextwp]

                if np.square(direction[0]) > np.square(direction[2]): # travel more along x then z axis
                    enclosure = [
                        min(location_cube[0], destination_cube[0]) - 1*cube_side, max(location_cube[0], destination_cube[0]) + 1*cube_side + 1,
                        min(location_cube[1], destination_cube[1]) - 16*cube_side, max(location_cube[1], destination_cube[1]) + 16*cube_side + 1,
                        min(location_cube[2], destination_cube[2]) - 8*cube_side, max(location_cube[2], destination_cube[2]) + 8*cube_side + 1,
                    ]
                else:
                    enclosure = [
                        min(location_cube[0], destination_cube[0]) - 8*cube_side, max(location_cube[0], destination_cube[0]) + 8*cube_side + 1,
                        min(location_cube[1], destination_cube[1]) - 16*cube_side, max(location_cube[1], destination_cube[1]) + 16*cube_side + 1,
                        min(location_cube[2], destination_cube[2]) - 1*cube_side, max(location_cube[2], destination_cube[2]) + 1*cube_side + 1,
                    ]


                t = [int(x) for x in enclosure]
                extend = [x for x in zip(t[::2], t[1::2])]

                # Note: from now work with the center-points of the cubes
                full = np.unique(np.asarray([(round(cx + cube_side/2),round(cy + cube_side/2),round(cz + cube_side/2))
                                                 for cx in range(*extend[0], cube_side)
                                                 for cy in range(*extend[1], cube_side)
                                                 for cz in range(*extend[2], cube_side)],
                                            dtype=[("cx","int64"),("cy","int64"),("cz","int64")]))

                sys.stdout.write(f"\rCurrent {str(currentwp)} to {str(nextwp)}, {str(full.shape[0])} cubes ...")

                regions = np.unique(datasource.get_data_array('''
                    SELECT ROUND($7*FLOOR(x/$7) + $7/2) AS cx, ROUND($7*FLOOR(y/$7) + $7/2) AS cy, ROUND($7*FLOOR(z/$7) + $7/2) AS cz,
                            count(1) starcount,
                            ROUND(|/((AVG(x)-$8)^2 + (AVG(y)-$9)^2 + (AVG(z)-$10)^2)) distance,
                            0 weight
                    FROM systems
                    WHERE x >= $1 AND x <= $2 AND  y >= $3 AND y <= $4  AND z  >= $5 AND z <= $6
                    GROUP BY cx, cy, cz

                ''', *enclosure, cube_side, *end.tolist(),
                    dtype=[("cx","int64"), ("cy","int64"),("cz","int64"),("starcount","float64"), ("distance","float64"), ("weight","float64")]))

                sys.stdout.write(f"\rCurrent {str(currentwp)} to {str(nextwp)}, {str(regions.shape[0])} regions found ...")
                assert not regions.shape[0] > full.shape[0]
                joined = join_by(
                    ('cx', 'cy','cz'),
                    full,regions,
                    jointype="outer", usemask=False,
                    defaults = {"starcount":0, "total":np.nan, "distance":np.nan, 'weight':np.nan}
                )

                joined["distance"] = np.round(np.sqrt(
                    np.square(joined["cx"]-end[0]) +
                    np.square(joined["cy"]-end[1]) +
                    np.square(joined["cz"]-end[2])))

                joined['weight'] = np.round(np.log(3+joined['starcount']) * joined["distance"])
                max_count = np.amax(joined['starcount'])

                # Travel direction, z-x order and normal
                if np.square(direction[0]) > np.square(direction[2]): # travel more along x then z axis
                    main_direction = direction[0]
                    main_order = ['cx', 'cz']
                    plane_normal = np.array([1,0,0])

                else: # travel more along z then x axis
                    main_direction = direction[2]
                    main_order = ['cz','cx']
                    plane_normal = np.array([0,0,1])

                if main_direction < 0:
                    joined.sort(order=main_order)
                    plane_normal *= -1
                else:
                    joined[::-1].sort(order=main_order) # reverse sort

                #('cx', 'cy', 'cz', 'starcount', 'distance', 'weight')
                cubespace = np.zeros(shape=(joined.shape[0],len(joined.dtype)+1+2))
                for ci, cn in zip(range(len(joined.dtype)), joined.dtype.names) :
                    cubespace[:,ci] = joined[cn]

                candidate_waypoints = joined[np.less(joined['distance'], np.percentile(joined['distance'],5))]
                waypoints = np.zeros(shape=(candidate_waypoints.shape[0],6))
                waypoints[:,3] = candidate_waypoints['starcount']
                for ci, cn in zip([0,1,2], ['cx', 'cy','cz']) :
                    waypoints[:,ci] = candidate_waypoints[cn]

                # calculate a weight for each line from currentwp to waypoints
                # based on the distance of each cube in our extend and the starcount
                #sys.stdout.write(f", {waypoints.shape[0]} waypoints")
                for waypoint in waypoints:
                    # First calculate distances between cubes and travel 'lines'
                    # d = norm(np.cross(lp2-lp1, lp1-p3))/norm(lp2-lp1)
                    lp1 = waypoint[0:3]
                    lp2 = currentwp
                    p3 = cubespace[:,0:3]

                    d = np.linalg.norm(np.cross(lp2-lp1, lp1-p3,axisb=1),axis=1)/np.linalg.norm(lp2-lp1)

                    w = np.less(d,4*cube_radius) # wider selection to get a relative density
                    n = np.less(d,2*cube_radius) # select cubes near the travel line
                    # sum and divide by traveldistance to get a measure of density
                    waypoint[4] = np.sum(cubespace[n][:,3]/(1+d[n])) / (1+np.sqrt(np.sum(np.square(lp1-lp2))))
                    waypoint[5] = np.mean(cubespace[n][:,3])
                    waypoint[4] *= (1+np.mean(cubespace[n][:,3])) / (1+np.mean(cubespace[w][:,3]))

                sys.stdout.write(f"\rCurrent {str(currentwp)} to {str(nextwp)}, weighed {str(waypoints.shape[0])} waypoint cubes\r")
                path_info[system_names[-1]] = {'candidates':[], 'stations':[]}
                waypoints = waypoints[np.less(waypoints[:,4], np.percentile(waypoints[:,4],15))]

                if len(regions) > 0:
                    candidates = []

                    for weighed_target in waypoints[waypoints[:,4].argsort()]:
                        sys.stdout.write(f"\rTarget, w={weighed_target[4]}: {np.round([weighed_target[k] for k in [0,1,2]])}\r")
                        s = cube_side
                        while not candidates and s < 200:
                            s += cube_side
                            candidates = await datasource.find_nearby_systems([weighed_target[k] for k in [0,1,2]], s)
                            sys.stdout.write(f"\rFound {len(candidates)} candidates for {np.round([weighed_target[k] for k in [0,1,2]])} {s} ly search\r")

                        if candidates:
                            candidate = candidates[0]
                            #print(candidate)
                            path_info[system_names[-1]]['candidates'].append(record_to_dict(candidate))
                            sys.stdout.write(f"\rFound candidate {record_to_dict(candidate).get('name')} for {np.round([weighed_target[k] for k in [0,1,2]])} {s} ly search\r")
                            #path_info[system_names[-1]]['stations'] += [(R.get('system'),R.get('station'), round(R.get('distance'))) for R in await find_nearby_stations([joined[0][k] for k in ["cx","cy","cz"]],300)]
                            if candidate.get("name") in system_names:
                                sys.stdout.write(f" ... is already a waypoint")
                                break

                            if np.round(np.sqrt(np.sum(np.square(path[-1]-end)))) < 2*cube_side+np.round(np.sqrt(np.sum(np.square(np.asarray([candidate.get(k) for k in ["x","y","z"]])-end)))):
                                sys.stdout.write(f" {path[-1]} is already closer to {end} than {[round(candidate.get(k)) for k in ['x','y','z']]}")
                                break

                            cube_density = weighed_target[3]
                            path.append(np.asarray([candidate.get(k) for k in ["x","y","z"]]))
                            sys.stdout.write(f"""\r{system_names[-1]:26} {np.sqrt(np.sum(np.square(path[-1]-currentwp))):7.1f} ly ->\t{candidate.get('name'):26} {weighed_target[4]:6.2f}\t{np.sqrt(np.sum(np.square(path[-1]-end))):7.1f} ly remaining {' ':12}""")

                            path_info[system_names[-1]]['nextwp']=dict(
                                system=candidate.get('name'),
                                distance=np.round(np.sqrt(np.sum(np.square(path[-1]-currentwp))),1),
                                remaining=np.round(np.sqrt(np.sum(np.square(path[-1]-end))),1),
                                weight=weighed_target[4],
                                density=1000*weighed_target[5]/math.pow(cube_side,3),#1e6*weighed_target[3]/(np.power(cube_side/3.26,3)*weighed_target[6]),
                                cube=[weighed_target[k] for k in [0,1,2]],
                                error=np.round(np.sqrt(np.sum(np.square(path[-1]-np.asarray(nextwp)))))
                            )
                            system_names.append(candidate.get("name"))
                            break

                    if np.sqrt(np.sum(np.square(np.asarray(cube_center(np.round(path[-1],2))) - currentwp))) < cube_side:
                        break

                else:
                    sys.stdout.write(f"\rCurrent {str(currentwp)} to {str(nextwp)}, no candidate-cubes\r")
                    break

            cube_side = int(0.8*cube_side)

        print()

