
#
# Module dependencies.
#

from datetime import datetime, timedelta
from singer import utils
from urllib.parse import urlencode
import backoff
import json
import requests
import logging
import time

"""Simple wrapper for Iterable."""
import time
import socket
from functools import wraps
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
from http.client import IncompleteRead
from urllib3.exceptions import ProtocolError

logger = logging.getLogger()

class Iterable(object):
    def __init__(self, api_key, start_date=None, api_window_in_days=30):
        self.api_key = api_key
        self.uri = "https://api.iterable.com/api/"
        self.api_window_in_days = int(api_window_in_days)
        self.MAX_BYTES = 10240
        self.CHUNK_SIZE = 512
        self.headers = {"Api-Key": self.api_key}
    
    def _now(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def _daterange(self, start_date, end_date):
        total_days = (utils.strptime_with_tz(end_date) - utils.strptime_with_tz(start_date)).days
        if total_days >= self.api_window_in_days:
            for n in range(int(total_days / self.api_window_in_days)):
                yield (utils.strptime_with_tz(start_date) + n * timedelta(int(self.api_window_in_days))).strftime("%Y-%m-%d %H:%M:%S")
        else:
            yield start_date
        
    def _get_end_datetime(self, startDateTime):
        endDateTime = utils.strptime_with_tz(startDateTime) + timedelta(self.api_window_in_days)
        now = utils.strptime_with_tz(self._now())
        
        # Never return a date in the future
        if endDateTime > now:
            endDateTime = now
        
        return endDateTime.strftime("%Y-%m-%d %H:%M:%S")
    
    def retry_handler(details):
        logger.info("Received 429 -- sleeping for %s seconds", details['wait'])

    @staticmethod
    def connection_retry_handler(func):
        """Decorator to retry on connection failures with exponential backoff."""
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            max_retries = 5
            retry_count = 0
            
            # Exceptions that indicate connection was dropped
            connection_exceptions = (
                ChunkedEncodingError,
                ConnectionError,
                IncompleteRead,
                ProtocolError,
                Timeout,
                socket.timeout,
            )
            
            while retry_count < max_retries:
                try:
                    return func(self, *args, **kwargs)
                    
                except connection_exceptions as e:
                    retry_count += 1
                    
                    if retry_count >= max_retries:
                        logger.error(
                            f"Connection failed after {max_retries} retries: {e}"
                        )
                        raise
                    
                    # Exponential backoff: 2, 4, 8, 16, 32 seconds
                    wait_time = 2 ** retry_count
                    logger.warning(
                        f"Connection error ({type(e).__name__}: {e}). "
                        f"Retrying in {wait_time}s... (attempt {retry_count}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    
            return func(self, *args, **kwargs)
        return wrapper
    
    # The actual `get` request with both rate limit and connection retry handling
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.HTTPError,
        on_backoff=retry_handler,
        giveup=lambda e: e.response.status_code != 429,
        max_tries=10
    )
    @connection_retry_handler
    def _get(self, path, stream=True, **kwargs):
        conn_timeout = 30
        read_timeout = 60 * 5
        timeouts = (conn_timeout, read_timeout)
        uri = "{uri}{path}".format(uri=self.uri, path=path)
        
        # Add query params
        params = {}
        for key, value in kwargs.items():
            params[key] = value
        uri += "?{params}".format(params=urlencode(params))
        
        logger.info("GET request to {uri}".format(uri=uri))
        
        # Create session with TCP keep-alive
        session = self._get_session_with_keepalive()
        response = session.get(uri, stream=stream, headers=self.headers, timeout=timeouts)
        response.raise_for_status()
        
        return response
    
    def _get_session_with_keepalive(self):
        """Create a requests session with TCP keep-alive enabled."""
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.connection import create_connection
        
        # Custom connection with keep-alive
        class KeepAliveHTTPAdapter(HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                if 'socket_options' not in kwargs:
                    kwargs['socket_options'] = []
                
                # Enable TCP keep-alive
                kwargs['socket_options'] += [
                    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                    (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120),   # Start after 2min idle
                    (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30),   # Probe every 30s
                    (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3),      # 3 failed probes
                ]
                return super().init_poolmanager(*args, **kwargs)
        
        session = requests.Session()
        session.mount('http://', KeepAliveHTTPAdapter())
        session.mount('https://', KeepAliveHTTPAdapter())
        
        return session
    

  
    def get(self, path, **kwargs):
        response = self._get(path, **kwargs)
        return response.json()

  #
  # Get custom user fields, used for generating `users` schema in `discover`.
  #

    def get_user_fields(self):
        return self.get("users/getFields")

  # 
  # Methods to retrieve data per stream/resource.
  # 

    def lists(self, column_name=None, bookmark=None):
        res = self.get("lists")
        for l in res["lists"]:
          yield l


    def list_users(self, column_name=None, bookmark=None):
        res = self.get("lists")
        for l in res["lists"]:
          kwargs = {
            "listId": l["id"]
          }
          users = self._get("lists/getUsers", **kwargs)
          for user in users:
            yield {
              "email": user,
              "listId": l["id"],
              "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
            }


    def campaigns(self, column_name=None, bookmark=None):
        res = self.get("campaigns")
        for c in res["campaigns"]:
          yield c


    def channels(self, column_name, bookmark):
        res = self.get("channels")
        for c in res["channels"]:
          yield c


    def message_types(self, column_name=None, bookmark=None):
        res = self.get("messageTypes")
        for m in res["messageTypes"]:
          yield m


    def templates(self, column_name, bookmark):
        template_types = [
          "Base",
          "Blast",
          "Triggered",
          "Workflow"
        ]
        message_mediums = [
          "Email",
          "Push",
          "InApp",
          "SMS"
        ]
        for template_type in template_types:
          for medium in message_mediums:
            res = self.get("templates", templateTypes=template_type, messageMedium=medium)
            for t in res["templates"]:
              yield t


    def metadata(self, column_name=None, bookmark=None):
        tables = self.get("metadata")
        for t in tables["results"]:
          keys = self.get("metadata/{table_name}".format(table_name=t["name"]))
          for k in keys["results"]:
            value = self.get("metadata/{table_name}/{key}".format(table_name=k["table"], key=k["key"]))
            yield value


    def get_data_export_generator(self, data_type_name, bookmark=None):
        now = self._now()
        kwargs = {}
        for start_date_time in self._daterange(bookmark, now):
          kwargs["startDateTime"] = start_date_time
          kwargs["endDateTime"] = self._get_end_datetime(startDateTime=start_date_time)
          def get_data():
            return self._get("export/data.json", dataTypeName=data_type_name, **kwargs)
          yield get_data






