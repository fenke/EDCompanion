from email.policy import default
import json
import sys
import os
import io
import time
import logging
import math
from turtle import distance
import numpy as np
import pandas as pd
import socket
import falcon
import falcon.asgi
import aiohttp
import asyncpg
import uvicorn

import pgsqldata


logging.basicConfig(
    format="%(asctime)s.%(msecs)03d \t%(threadName)s\t%(name)s\t%(lineno)d\t%(levelname)s\t%(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.INFO
)

logging.captureWarnings(True)
syslog = logging.getLogger("root." + __name__)
syslog.info("Python executable %s running from %s", sys.executable, os.getcwd())
syslog.info("Falcon version %s on ip-address %s\n", falcon.__version__, socket.gethostbyname(socket.gethostname()))

# Create the APP - adds routes
def create_companion_api():
    """
    Returns:
        falcon.asgi.API
    """
    # create with our middleware
    app = falcon.asgi.App(middleware=[
        RequiresAuthentication(token=os.getenv("USR_TOKEN")),
        RouteTimer(),
        RequiredMediaTypes()])

    eddatasource = pgsqldata.PGSQLDataSourceEDDB(os.getenv("PGSQL_URL"), server_settings={'search_path': "edsm"})
    # add routes -----------------------------
    app.add_route('/systems', SystemsEndpoint(eddatasource))
    return app

class RouteTimer(object):
    """ Middleware component to track route excecution times """

    def __init__(self) -> None:
        pass

    async def process_resource(self, req, resp, resource, params):
        """Process the request after routing.

        Note:
            This method is only called when the request matches
            a route to a resource.

        Args:
            req: Request object that will be passed to the
                routed responder.
            resp: Response object that will be passed to the
                responder.
            resource: Resource object to which the request was
                routed.
            params: A dict-like object representing any additional
                params derived from the route's URI template fields,
                that will be passed to the resource's responder
                method as keyword arguments.
        """
        req.context.start_process = time.process_time_ns()
        req.context.start_counter = time.perf_counter_ns()


    async def process_response(self, req, resp, resource, req_succeeded):
        """Post-processing of the response (after routing).

        Args:
            req: Request object.
            resp: Response object.
            resource: Resource object to which the request was
                routed. May be None if no route was found
                for the request.
            req_succeeded: True if no exceptions were raised while
                the framework processed and routed the request;
                otherwise False.
        """
        time_process = time.process_time_ns() - req.context.get("start_process",0)
        time_counter = time.perf_counter_ns() - req.context.get("start_counter", 0)
        syslog.info(f"Resource {str(type(resource))} - process: {(time_process)/1e6} ms, total: {(time_counter)/1e6} ms, waited {round(100*(time_counter-time_process)/time_counter)}%")


class RequiresAuthentication(object):
    """Provides authentication middleware
    """
    def __init__(self, token):
        self.token = token

    async def process_request(self, req, resp):
        """Checks request for authorization tokens and validates them

        Arguments:
            req {Request} -- Falcon request object
            resp {Response} -- Not used

        Raises:
            falcon.HTTPUnauthorized: Thrown when no valid authorization provided
        """
        token = req.get_param("token")
        if token is None:
            token = req.get_header("Authorization")

        if token is not None:
            if str(token) == self.token:
                return

        raise falcon.HTTPUnauthorized(
            "authentication required",
            description="Valid authentication is required")

class RequiredMediaTypes(object):
    """Checks if media is JSON"""
    def __init__(self):
        pass

    async def process_request(self, req, resp):
        """Checks request document type

        Arguments:
            req {Request} -- Falcon request object
            resp {Response} -- Not used

        Raises:
            falcon.HTTPUnsupportedMediaType: Thrown when non-JSON document given
        """
        if req.method in ["POST", "PUT"]:
            if req.content_type not in ["application/json"]:
                raise falcon.HTTPUnsupportedMediaType(f"Given Content-Type {req.content_type} but only JSON media are supported.")

class Endpoint(object):

    def __init__(self, datasource):
        self.datasource = datasource


    def file_response(self, req, resp, dataframe):
        writeformat = req.get_param("format", default="json/split").lower()
        with io.BytesIO() as outfile:

            if writeformat == "csv":
                resp.content_type = "application/octet-stream"
                self.write_csv(outfile, dataframe)

            elif "json" in writeformat:
                resp.content_type = "application/json"
                self.write_json(
                    outfile, dataframe,
                    writeformat,
                    req.get_param("dateFormat", default="iso").lower())

            outfile.seek(0)
            resp.data = outfile.read()

    def write_csv(self, outfile, adata):

        if isinstance(adata, pd.DataFrame):
            adata.to_csv(outfile, sep=',')
        else:
            np.savetxt(
                outfile, adata,
                delimiter=",", comments='',
                header=",".join(['time'] + [f"value_{str(i+1)}" for i in range(adata.shape[1]-1)])
            )

    def write_json(self, outfile, adata, writeformat, dateformat):
        h,*t = writeformat.lower().split('/')
        assert h == 'json', f"Invalid format specification: {h}"
        if t:
            s, *t = t

            df = None
            if isinstance(adata, pd.DataFrame):
                df = adata
            else:
                if len(adata.shape) == 2:
                    df = pd.DataFrame(
                        data=adata[:, 1:]
                    )
                elif len(adata.shape) == 1:
                    # Assume a record array
                    df = pd.DataFrame(adata)

            syslog.info(f"Writing Data from {str(df.info())}")

            if s == 'values':
                df.reset_index().to_json(outfile, orient='values', date_format=dateformat)
            elif s == 'split':
                outfile.write(f'''{{"columns":{str(['time']+[c for c in df.columns]).replace("'",'"')}, "data":'''.encode(encoding='UTF-8'))
                df.reset_index().to_json(outfile, orient='values', date_format=dateformat)
                outfile.write(f'}}'.encode(encoding='UTF-8'))

            elif s == 'split-index':
                df.to_json(outfile, orient='split', date_format=dateformat)

            elif s == 'records':
                df.reset_index().to_json(outfile, orient='records', date_format=dateformat, index=True)
            elif s == 'table':
                df.to_json(outfile, orient='table', date_format=dateformat, index=True)
            else:
                df.reset_index().to_json(outfile, orient=s, date_format=dateformat)


class SystemsEndpoint(Endpoint):
    def __init__(self, datasource):
        super().__init__(datasource)

    join_types = dict(
        inner=lambda T: f" INNER JOIN {T} ON {T}.systemname = systems.name ",
        left=f" LEFT JOIN {T} ON {T}.systemname = systems.name "
    )

    def convert_include(includes):
        from_item = f"FROM systems "
        for include_key, include_definition in includes.items():
            if include_definition in SystemsEndpoint.join_types:
                from_item += SystemsEndpoint.join_types[include_definition](include_key)

    async def on_get(self, req, resp):

        pgpool = await self.datasource.pool.pgsql_pool()
        qryfilter = json.loads(req.get_param('filter', default='{}'))
        if not qryfilter:
            system_name = req.get_param('name', default='')
            if system_name:
                self.file_response(req, resp, await self.datasource.get_dataframe(
                    """
                        SELECT s.*, 0 as distance, p.security
                        FROM systems s
                        LEFT JOIN populated p
                        ON s.name = p.systemname
                        where s.name like $1
                    """, system_name))
                return

            coordinates = [float(c) for c in req.get_param_as_list('coordinates', required=True)]

            distance = req.get_param_as_float('distance', default=40.0)
            assert len(coordinates) == 3

            self.file_response(req, resp, await self.datasource.get_dataframe(
                "SELECT systems.*, |/((x-$7)^2 + (y-$8)^2 + (z-$9)^2) as distance, populated.security "+
                "FROM systems "+
                "LEFT JOIN populated " +
                "ON systems.name = populated.systemname "
                "WHERE x>=$1 AND x<$2 AND  y>=$3 AND y<$4  AND  z>=$5 AND z<$6  AND |/((x-$7)^2 + (y-$8)^2 + (z-$9)^2) < $10"+
                "ORDER BY distance LIMIT 1",
                *[d for c in coordinates for d in [c-distance, c+distance]], *coordinates, distance
            ))
            return

        from_item = "FROM systems "
        for include_key, include_definition in qryfilter.get('include', {}).items():
            if include_definition in ["iner"]


class StationsEndpoint(Endpoint):
    def __init__(self, datasource):
        super().__init__(datasource)

    async def on_get(self, req, resp):
        syslog.error(f"Call of unimplemeted member")
        raise falcon.HTTPNotImplemented(
            title="Not Implemented",
            description="The resource has not implemented this functionality")




companion_api = create_companion_api()

if __name__ == "__main__":
    uvicorn.run("restapi:companion_api", host="0.0.0.0", port=8000, log_level="info")
