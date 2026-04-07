from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class TicketHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/api/tickets"):
            payload = {
                "tickets": [
                    {
                        "ticket_id": "ci-100",
                        "customer_id": "ci-customer-1",
                        "created_at": "2026-04-01T10:00:00Z",
                        "sentiment_score": -0.25,
                    }
                ]
            }
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a mock tickets API for contract checks.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()

    server = HTTPServer((args.host, args.port), TicketHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
