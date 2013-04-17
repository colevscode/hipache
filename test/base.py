
import os
import time
import signal
import socket
import logging
import unittest
import BaseHTTPServer

import redis
import requests


logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
        level=logging.DEBUG)


class HTTPHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def __init__(self, *args, **kwargs):
        BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def do_GET(self):
        self.send_response(self.server._code)
        body = self.server._body if self.server._body else 'This is a body'
        self.send_header('Content-Length', len(body))
        if self.server._headers:
            for header in self.server._headers:
                self.send_header(*header)
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def do_HEAD(self):
        return self.do_GET()


class TestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.redis = redis.StrictRedis()
        self.check_ready()
        self._httpd_pids = []
        self._frontends = []
        self.addCleanup(self.stop_all_httpd)
        self.addCleanup(self.unregister_all_frontends)

    def check_ready(self):
        """ Makes sure the activechecker is running """
        r = None
        try:
            r = requests.get('http://localhost:1080', headers={'Host': '__ping__'})
        except Exception:
            pass
        if r is None or r.status_code != 200:
            self.fail('Hipache should run on port 1080: \n'
                    '$ hipache -c config/config_test.json')

    def stop_all_httpd(self):
        if not self._httpd_pids:
            return
        for pid in self._httpd_pids:
            os.kill(pid, signal.SIGKILL)
            logger.info('httpd killed. PID: {0}'.format(pid))
        os.wait()

    def spawn_httpd(self, port, code=200, headers=None, body=None):
        pid = os.fork()
        if pid > 0:
            # In the father, wait for the child to be available
            while True:
                r = self.http_request('localhost', port=port)
                if r > 0:
                    self._httpd_pids.append(pid)
                    logger.info('httpd spawned on port {0}. PID: {1}'.format(port, pid))
                    return pid
                time.sleep(0.5)
        # In the child, spawn the httpd
        httpd = BaseHTTPServer.HTTPServer(('localhost', port), HTTPHandler)
        httpd._code = code
        httpd._headers = headers
        httpd._body = body
        httpd.serve_forever()

    def stop_httpd(self, pid):
        os.kill(pid, signal.SIGKILL)
        logger.info('httpd stopped. PID: {0}'.format(pid))
        os.wait()
        if pid in self._httpd_pids:
            self._httpd_pids.remove(pid)

    def register_frontend(self, frontend, backends_url, proto='', prefix=''):
        frontendkey = ':'.join(filter(None, (proto, frontend)))
        self.redis.rpush('{0}frontend:{1}'.format(prefix, frontendkey), frontend, *backends_url)
        self._frontends.append(frontendkey)

    def unregister_frontend(self, frontend, proto='', prefix=''):
        frontendkey = ':'.join(filter(None, (proto, frontend)))
        self.redis.delete('{0}frontend:{1}'.format(prefix, frontendkey))
        self.redis.delete('{0}dead:{1}'.format(prefix, frontendkey))
        if frontendkey in self._frontends:
            self._frontends.remove(frontendkey)

    def unregister_all_frontends(self):
        # Copy the list of frontend since we modify the list inside the loop
        for frontend in list(self._frontends):
            self.unregister_frontend(frontend)

    def http_request(self, host, port=1080, proto='http', check_text=False):
        try:
            headers = {'Host': host, 'X-Debug': 'true'}
            r = requests.get(proto+'://localhost:{0}/'.format(port),
                    headers=headers, timeout=1.0, verify=False)
            logger.debug('Frontend: {0}; Headers: {1}; Payload: "{2}"'.format(
                host, r.headers, r.text))
            return r.text if check_text else r.status_code
        except (requests.ConnectionError, requests.Timeout, socket.timeout) as e:
            return -1
