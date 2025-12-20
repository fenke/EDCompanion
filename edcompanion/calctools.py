import numpy as np
from collections import namedtuple

NT_Line = namedtuple('line_nd',['direction', 'support'])

def project_point_on_line(point, direction, support):

    dp = np.dot(np.asarray(point)-np.asarray(support), np.asarray(direction) )
    return np.round(dp*np.asarray(direction) + np.asarray(support),1)

    """Projects a point on a line"""
    if isinstance(point, np.ndarray) and isinstance(direction, np.ndarray):
        dp = np.dot(point-support, direction)
        return direction * dp + support
    else:
        return(np.asarray(point), np.asarray(direction), np.asarray(support))

def distance_point_to_line(point, direction, support):
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

    return NT_Line(
        direction = vv[0],
        support = support,
    )

def create_get_bits_func(bits, offset):
    mask=(2**bits)-1
    shift_right = (64 - offset) - bits
    def get_bits(val):
        return (val  >> shift_right) & mask
    return get_bits

id64_layout = {
    'sector_id': (41, 0),
    'sector_x': (14, 0),
    'sector_y': (13, 14),
    'sector_z': (14, 27),
    'cube_layer': (3, 41),
    'system_number': (11, 44),
    'body_number': (9, 55),
}

convert_id64_mappiing = {
    k: create_get_bits_func(*f)
    for k, f in id64_layout.items()
}


def split_id64(id64):
    return {
        k: f(id64)
        for k, f in convert_id64_mappiing.items()
    }