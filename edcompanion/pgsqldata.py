#pylint: disable=missing-module-docstring
#pylint: disable=missing-function-docstring

import math
import typing
import logging
import urllib.parse
import asyncpg
import numpy as np
import pandas as pd
#from edsm_api import get_edsm_info
from edcompanion.edsm_api import get_edsm_info

syslog = logging.getLogger("root." + __name__)

class PostgreSQLDataSource(object):
    """
        Wraps asyncpg::ConnectionPool to provide one pool object per url.

        PGSQLDataSource(url): Constructs pooling datasource for the URL given

    """

    poolcache = dict()

    def __init__(self, url, server_settings=None):

        class PoolProxy(typing.NamedTuple):
            pgsql_pool: typing.Awaitable

        # parse the url because there are sometimes illegal options in there
        dsn, *options = url.split("?")
        pgsql_params = dict(
            dsn=dsn,
            min_size=2, max_size=6,
            statement_cache_size=0
        )
        if options:
            for option, value in urllib.parse.parse_qsl(options):
                if option not in ["ssl"]: # not a valid option for Postgresql
                    pgsql_params[option] = value
        if server_settings:
            pgsql_params['server_settings'] = server_settings

        # Create one pool per DSN
        if dsn not in PostgreSQLDataSource.poolcache:
            PostgreSQLDataSource.poolcache[dsn] = None

        # closure - dsn, pgsql_params
        async def get_pool():
            if not PostgreSQLDataSource.poolcache[dsn]:
                syslog.info(f"Creating Pool for {dsn}")
                newpool = await asyncpg.create_pool(
                    **pgsql_params)
                if not PostgreSQLDataSource.poolcache[dsn]:
                    PostgreSQLDataSource.poolcache[dsn] = newpool

            return PostgreSQLDataSource.poolcache[dsn]

        # and finally, a get_pool function for self
        self.pool = PoolProxy(get_pool)

class PGSQLDataSourceEDDB(PostgreSQLDataSource):
    """
        Provides methods to read data directly from postgreSQL database
    """

    async def get_dataframe(self, query, *queryparams):

        pool = await self.pool.pgsql_pool()
        async with pool.acquire() as pgsql_connection:
            records = await pgsql_connection.fetch(
                query,
                *queryparams
            )
            return pd.DataFrame.from_records(
                records,
                columns=[k for k in records[0].keys()]
            )

    async def get_data_array(self, query, *queryparams, dtype=np.float64):

        pool = await self.pool.pgsql_pool()
        async with pool.acquire() as pgsql_connection:
            records = await pgsql_connection.fetch(
                query,
                *queryparams
            )

            return np.asarray([tuple(R) for R in records], dtype=dtype)


    async def find_system(self, system, distance=40):
        pool = await self.pool.pgsql_pool()
        if isinstance(system, str):
            q1 = await pool.fetchrow(
                """
                    SELECT s.*, 0 as distance, p.security
                    FROM systems s
                    LEFT JOIN populated p
                    ON s.name = p.systemname
                    where s.name = $1
                """, system)
            if not q1:
                return get_edsm_info(system)
            return q1

        assert len(system) == 3
        coordinates = system
        c20_location = [int(20*math.floor(v/20)) for v in coordinates]
        side = int(20*math.floor(distance/20))
        q1 = await pool.fetch(
            "SELECT systems.*, |/((x-$7)^2 + (y-$8)^2 + (z-$9)^2) as distance, populated.security "+
            "FROM systems "+
            "LEFT JOIN populated " +
            "ON systems.name = populated.systemname "
            "WHERE x>=$1 AND x<$2 AND  y>=$3 AND y<$4  AND  z>=$5 AND z<$6  AND |/((x-$7)^2 + (y-$8)^2 + (z-$9)^2) < $10"+
            "ORDER BY distance",
            *[d for c in coordinates for d in [c-40, c+40]], *coordinates, distance)
        if not q1:
            return q1
        return await self.find_system(q1[0].get("name"))

    async def find_nearby_systems(self, system, distance, limit=5):

        if isinstance(system, str):
            ql = await self.find_system(system)
            coordinates = [ql.get(k) for k in ["x", "y","z"]]
        else:
            coordinates = system

        #c20_location = [int(20*math.floor(v/20)) for v in coordinates]
        side = int(20*math.ceil(distance/20))

        pool = await self.pool.pgsql_pool()
        return await pool.fetch(
            "SELECT name, x,y,z, |/((x-$7)^2 + (y-$8)^2 + (z-$9)^2) as distance "+
            "FROM systems "+"""
                WHERE  x>=$1 AND x<$2
                AND  y>=$3 AND y<$4
                AND  z>=$5 AND z<$6
            """ +
            "  AND |/((x-$7)^2 + (y-$8)^2 + (z-$9)^2) < $10"+
            "ORDER BY distance LIMIT " + str(limit),
            *[d for c in coordinates for d in [c-side, c+side]],
            *coordinates, distance)

