import numpy as np
from collections import namedtuple

_nt_line = namedtuple('line_nd',['direction', 'support'])

def project_point_on_line(point, direction, support):
    """Projects a point on a line"""
    dp = np.dot(point-support, direction) 
    return direction * dp

def distance_point_tol_line(point, direction, support):
    """Calculate distance between point and a line"""
    return np.linalg.norm(
        np.cross(
            direction,
            point - support,
            axis=-1),
        axis=-1) / np.linalg.norm(direction,axis=-1)

def line_from_points(points):
    """Fit a line through a set of points
        from: https://ltetrel.github.io/data-science/2018/06/08/line_svd.html
        
        returns named tuple with properties support and direction."""
    # https://ltetrel.github.io/data-science/2018/06/08/line_svd.html

    # Calculate the mean of the points, i.e. the 'center' of the cloud
    support = points.mean(axis=0)

    # Do an SVD on the mean-centered data.
    uu, dd, vv = np.linalg.svd(points - support)

    return _nt_line(
        direction = vv[0],
        support = support,
    )

