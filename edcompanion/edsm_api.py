#pylint: disable=missing-module-docstring
#pylint: disable=missing-function-docstring
#pylint: disable=invalid-name

from functools import lru_cache
import requests
import os
import numpy as np

def get_commander_position(commandername, token):
    req = requests.get(
        'https://www.edsm.net/api-logs-v1/get-position',
        params=dict(
            commanderName=commandername,
            apiKey=token,
            showCoordinates=1)
        )
    if req.status_code == 200:
        record = req.json()
        return record if record else {}
    else:
        return {}

@lru_cache(512)
def get_edsm_info(systemname, verbose=True):
    if not systemname:
        return {}
    req = requests.get(
        'https://www.edsm.net/api-system-v1/bodies' if verbose else 'https://www.edsm.net/api-v1/system',
        params=dict(
            systemName=systemname,
            showCoordinates=1)
        )
    if req.status_code == 200:
        record = req.json()
        return record if record else {}
    else:
        return {}

@lru_cache(64)
def get_edsm_system_risk(systemname):
    if not systemname:
        return {}
    traffic = requests.get(
        'https://www.edsm.net/api-system-v1/traffic',
        params=dict(systemName=systemname)
    )

    if traffic.status_code == 200:
        trafficrecord = traffic.json().get('traffic',{})
        weektraffic = trafficrecord.get('week', 0)
        if weektraffic > 0:
            deaths = requests.get(
                'https://www.edsm.net/api-system-v1/deaths',
                params=dict(systemName=systemname)
            )

            if deaths.status_code == 200:
                deathsrecord = deaths.json().get('deaths',{})

                totaldeaths = deathsrecord.get('total',0)
                if totaldeaths > 0:
                    weekdeaths = deathsrecord.get('week',0)
                    totaltraffic = trafficrecord.get('total', 0)
                    return (weekdeaths*totaltraffic) / (weektraffic * totaldeaths)

                else:
                    return 0

    return np.nan


@lru_cache(512)
def distance_between_systems(s1name,s2name):
    s1 = get_edsm_info(s1name, verbose=False)
    s2 = get_edsm_info(s2name, verbose=False)
    c1 = np.asarray([s1.get('coords',dict(x=0,y=0,z=0)).get(k) for k in ['x', 'y', 'z']])
    c2 = np.asarray([s2.get('coords',dict(x=0,y=0,z=0)).get(k) for k in ['x', 'y', 'z']])
    return np.sqrt(np.sum(np.square(c1-c2)))

def get_systems_in_cube(system, size=100):
    base_url='https://www.edsm.net/api-v1/cube-systems'
    if not system:
        return {}
    if isinstance(system, str):
        return get_systems_in_cube_by_name(system, size)

    req = requests.get(
        base_url,
        params=dict(
            x=system[0],
            y=system[1],
            z=system[2],
            showCoordinates=1,
            showPrimaryStar=1,
            size=size
        )
    )
    if req.status_code == 200:
        return req.json()
    else:
        return {}

@lru_cache(32)
def get_systems_in_cube_by_name(system, size=100):
    base_url='https://www.edsm.net/api-v1/cube-systems'
    if not system:
        return {}
    req = requests.get(
        base_url,
        params=dict(
            systemName=system,
            showCoordinates=1,
            showPrimaryStar=1,
            size=size
        )
    )
    if req.status_code == 200:
        return req.json()
    else:
        return {}

@lru_cache(32)
def get_systems_in_sphere(system, radius=100):
    base_url='https://www.edsm.net/api-v1/sphere-systems'
    if not system:
        return {}
    req = requests.get(
        base_url,
        params=dict(
            systemName=system,
            showCoordinates=1,
            showPrimaryStar=1,
            radius=radius
        )
    )
    if req.status_code == 200:
        return req.json()
    else:
        return {}
