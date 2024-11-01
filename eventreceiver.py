import sys
import os
import io
import time
import logging
import psutil
import json
import re
import uuid
import hashlib
import uvicorn
import falcon
import falcon.asgi
from falcon.errors import HTTPInternalServerError

from edcompanion.timetools import make_datetime, make_naive_utc
from edcompanion.threadworker import create_threaded_worker


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

def file_event_writer(journal, event):
    with open(journal['journal_path'], "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")

async def pgsql_event_writer(journal, event):
    pass

class EventReceiverEndpoint(object):
    """ Handles incoming event messages """
    
    def __init__(self, **kwargs):
        self.pgsql_params = dict(
            dsn=kwargs.get('dsn', os.getenv("PGSQL_URL")),
            server_settings={'search_path': "eddb"}
        )
        self.regex_alphanum = re.compile('[^-.0-9a-zA-Z_]+')
        self.journals = {}
        self.logpath = os.path.abspath(os.path.join('journals'))
        print(f"Logpath: {self.logpath}")
        self.write_queue = create_threaded_worker(file_event_writer)

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

            journal_id = self.regex_alphanum.sub('_', str(req.get_param("journal_id", default='')))
            journal_id = str(uuid.UUID(journal_id))

            if not journal_id:
                data = await req.stream.read()
                journal_name = self.regex_alphanum.sub('_', str(req.get_param("filename")))
                journal_id = hashlib.md5(data).update(io.BytesIO(journal_name.encode('utf-8'))).hexdigest()
                events = json.loads(data.decode('utf-8'))
                self.journals[journal_id] = dict(
                    journal_name = journal_name,
                    last_event_time = make_datetime(events[1].get("timestamp")),
                    player_id = self.regex_alphanum.sub('_', str(events[1].get('FID'))),
                    header = events
                )
                journal['journal_path'] = os.path.join(self.logpath, str(journal['player_id']), journal['journal_name'])

            else:
                syslog.info(f"Using journal_id {journal_id}")

            journal = self.journals[journal_id]
            last_event_time = journal.get('last_event_time')


            event = json.loads((await req.stream.read()).decode('utf-8'))
            timestamp = make_datetime(event.get("timestamp"))
            if timestamp < last_event_time:
                return
            
            event_name = event.get('event')

            if event_name == 'Fileheader':
                journal['header'].append(event)

            elif event_name == 'Commander':

                journal['header'].append(event)
                journal['player_id'] = self.regex_alphanum.sub('_', str(event.get('FID')))
                journal['journal_path'] = os.path.join(self.logpath, str(journal['player_id']), journal['journal_name'])

                print(f"Journal path: {journal['journal_path']}")
                os.makedirs(os.path.join(self.logpath, str(journal['player_id'])), exist_ok=True)
                with open(os.path.join(journal['journal_path']), "w", encoding="utf-8") as f:
                    f.writelines(journal['header'])
                    journal['headers'] = []

            else:
                self.write_queue.put(journal, event)

            resp.content_type = "application/json"
            with io.BytesIO() as outfile:
                outfile.write(json.dumps(dict(journal_id=journal_id)).encode('utf-8'))

                outfile.seek(0)
                resp.data = outfile.read()
            
        except Exception as e:
            syslog.exception(f"Failed to process request: {e}", exc_info=True)
            raise

receiver_app.add_route('/event', EventReceiverEndpoint())

svm = psutil.virtual_memory()
syslog.info(f"Total Mem: {round(svm.total / 1048576)} Mb, available: {round(svm.available / 1048576 )} Mb, {svm.percent}% used")


if __name__ == "__main__":
    uvicorn.run("eventreceiver:receiver_app", host="0.0.0.0", port=8080, log_level="info")
