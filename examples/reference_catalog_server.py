"""Serve the owned catalog HTTP fixture on loopback. No deployed API is contacted."""
import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
import threading

import httpx

from catalog_fixture import CatalogFixture
from zarrsum.submissions import atomic_document


def serve(state_path, endpoint_path):
    state_path, endpoint_path = Path(state_path), Path(endpoint_path)
    fixture = CatalogFixture()
    if state_path.exists():
        saved = json.loads(state_path.read_bytes())
        fixture.records = {record['id']: record for record in saved['records']}
        fixture.events = saved['events']
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            return

        def do_GET(self):
            self.answer()

        def do_POST(self):
            self.answer()

        def answer(self):
            if self.headers.get('X-catalog-api-token') != 'local-fixture-only':
                self.send_error(403)
                return
            size = int(self.headers.get('Content-Length', 0))
            if not 0 <= size <= 4 << 20:
                self.send_error(413)
                return
            request = httpx.Request(self.command, 'https://catalog.invalid' + self.path,
                                    content=self.rfile.read(size))
            with lock:
                response = fixture.handle(request)
                atomic_document(state_path, {'records': list(fixture.records.values()),
                                              'events': fixture.events,
                                              'transport': 'owned loopback HTTP fixture'})
            self.send_response(response.status_code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

    server = HTTPServer(('127.0.0.1', 0), Handler)
    atomic_document(endpoint_path, {'base_url': 'http://127.0.0.1:' + str(server.server_port),
                                    'transport': 'owned loopback HTTP fixture'})
    try:
        server.serve_forever()
    finally:
        server.server_close()
        fixture.client.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', required=True)
    parser.add_argument('--endpoint', required=True)
    args = parser.parse_args()
    serve(args.state, args.endpoint)
