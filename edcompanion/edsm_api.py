#pylint: disable=missing-module-docstring
#pylint: disable=missing-function-docstring
#pylint: disable=invalid-name

from functools import lru_cache
import requests
import numpy as np

@lru_cache(128)
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

@lru_cache(128)
def distance_between_systems(s1name,s2name):
    s1 = get_edsm_info(s1name)
    s2 = get_edsm_info(s2name)
    c1 = np.asarray([s1.get('coords',dict(x=0,y=0,z=0)).get(k) for k in ['x', 'y', 'z']])
    c2 = np.asarray([s2.get('coords',dict(x=0,y=0,z=0)).get(k) for k in ['x', 'y', 'z']])
    return np.sqrt(np.sum(np.square(c1-c2)))
