import sys
import os
import io
import time
import logging
import psutil
import json
import re
import uuid
import asyncpg
import hashlib
import uvicorn
import falcon
import falcon.asgi
from falcon.errors import HTTPInternalServerError

from edcompanion.timetools import make_datetime, make_naive_utc

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d \t%(threadName)s\t%(name)s\t%(lineno)d\t%(levelname)s\t%(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.INFO
)

logging.captureWarnings(True)

syslog = logging.getLogger(__name__)

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
        syslog.info(f"Processing resource for path {req.path}, params {list(params.keys())}")
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
        syslog.info(f"Resource for path {req.path} - process: {(time_process)/1e6} ms, total: {(time_counter)/1e6} ms, waited {round(100*(time_counter-time_process)/time_counter)}%")

class RequiresAuthentication(object):
    """Provides authentication middleware
    """
    def __init__(self, token):
        self.token = token
        syslog.info(f"Using {self.token}")

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
            token = req.get_header("Authorization", req.get_header("authorization"))

        if token is not None:
            if str(token) == self.token:
                return
        
        syslog.info(f"Auth failed with {token}")
        raise falcon.HTTPUnauthorized(
            title="authentication required",
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
                raise falcon.HTTPUnsupportedMediaType(title=f"Given Content-Type {req.content_type} but only JSON is supported.")


receiver_app = falcon.asgi.App(
        cors_enable=True,
        middleware=[
        RequiresAuthentication(token=os.getenv("USR_TOKEN")), 
        RouteTimer(),
        RequiredMediaTypes()])


async def pgsql_event_writer(journal_id, event):
    pass

class EventReceiverEndpoint(object):
    """ Handles incoming event messages """
    
    def __init__(self, **kwargs):
        self.pgsql_params = dict(
            dsn=kwargs.get('dsn', os.getenv("PGSQL_URL")),
            server_settings={'search_path': "eddb"}
        )
        self.regex_alphanum = re.compile('[^-.0-9a-zA-Z_]+')

        self.pgsql_params = dict(
            dsn=os.getenv("PGSQL_URL"),
            server_settings={'search_path': "eddb"}
        )
        self.pool = None

    async def get_pool(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(**self.pgsql_params)

            await self.pool.execute(f"""
                CREATE TABLE IF NOT EXISTS journals (
                    id UUID NOT NULL,
                    name TEXT NOT NULL,
                    commander_id TEXT,
                    commander_name TEXT,
                    timestamp TIMESTAMPTZ 
                );
            """) # commander_id = $2, commander_name = $3, timestamp = $4
            await self.pool.execute(f"""
                CREATE TABLE IF NOT EXISTS journal_events (
                    journal_id UUID NOT NULL,
                    event_hash TEXT NOT NULL,
                    event_time TIMESTAMPTZ NOT NULL,
                    event_name TEXT NOT NULL,
                    event_data JSON
                );
            """) # commander_id = $2, commander_name = $3, timestamp = $4

            await self.pool.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS journal_id_idx ON eddb.journals (id);
                CREATE INDEX IF NOT EXISTS journals_name_idx ON eddb.journals (name);
                CREATE INDEX IF NOT EXISTS journal_events_journal_id_idx ON eddb.journal_events (journal_id); 
                CREATE INDEX IF NOT EXISTS journal_events_event_name_idx ON eddb.journal_events (event_name);
                CREATE INDEX IF NOT EXISTS jounal_events_event_time_idx ON eddb.journal_events (event_time);
                CREATE UNIQUE INDEX IF NOT EXISTS journal_events_event_hash_idx ON eddb.journal_events (event_hash);
            """)
        return self.pool
    

    async def on_post(self, req, resp):
        """Writes ED journal events

        Arguments:
            req {Request} -- Falcon request object
            resp {Response} -- Falcon response object

            request params:
                journal_id: temporary, identifies open journal (required except when filename is passed)
                filename: Journal file name to use (required only when journal_id is not passed)
        """
        try:

            if req.content_type != "application/json":
                return

            data = await req.stream.read()
            events = json.loads(data.decode('utf-8'))
            if not isinstance(events, list):
                events = [events]

            pool = await self.get_pool()
            journal_id = req.get_param("journal_id")
            journal_name = req.get_param("journal_name")
            for event in events:

                event_hash=hashlib.md5(json.dumps(event, sort_keys=True).encode('utf-8')).hexdigest()
                timestamp = make_datetime(event.pop("timestamp"))
                event_name = event.pop('event')
                
                if event_name == 'Commander':
                    # now we can make the journal entry

                    cmdr_id = event.get('FID')
                    record = await pool.fetch("SELECT id FROM journals WHERE name = $1 and commander_id = $2", journal_name, cmdr_id)
                    if not record:
                        journal_id = uuid.uuid4()
                        await pool.execute(
                                """INSERT INTO journals (id, name, commander_id)
                                    VALUES ($1, $2, $3) 
                                    ON CONFLICT DO NOTHING
                                """, journal_id, journal_name, cmdr_id
                        )
                    else:
                        journal_id = uuid.UUID( str(record[0].get('id')) )
                        syslog.info(f"Found journal {journal_name} for {cmdr_id} with id {journal_id}")

                    await pool.execute(
                            """update journals
                                SET commander_name = $2, timestamp = $3
                                WHERE id = $1
                            """, journal_id, event.get('Name'), timestamp
                    )
                    
                else:
                    jid_param = req.get_param("journal_id")
                    if  jid_param:
                        journal_id = uuid.UUID(str(jid_param))


                if journal_id:
                    await pool.execute(
                            """INSERT INTO journal_events (journal_id, event_time, event_name, event_hash, event_data)
                                VALUES ($1, $2, $3, $4, $5)
                                ON CONFLICT DO NOTHING
                            """, journal_id, timestamp, event_name, event_hash, json.dumps(event)
                    )

            resp.content_type = "application/json"
            resp.text = json.dumps({
                "journal_id": str(journal_id)
            }).encode('utf-8') if journal_id else json.dumps({}).encode('utf-8')
            
        except Exception as e:
            syslog.exception(f"Failed to process request: {e}", exc_info=True)
            raise

receiver_app.add_route('/event', EventReceiverEndpoint())

svm = psutil.virtual_memory()
syslog.info(f"Total Mem: {round(svm.total / 1048576)} Mb, available: {round(svm.available / 1048576 )} Mb, {svm.percent}% used")


if __name__ == "__main__":
    uvicorn.run("eventreceiver:receiver_app", host="0.0.0.0", port=8080, log_level="info")
