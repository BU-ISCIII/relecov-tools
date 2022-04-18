#!/usr/bin/env python
import logging
import json
import requests
import sys
import rich.console
import relecov_tools.utils

log = logging.getLogger(__name__)

stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class RestApi:
    def __init__(self, server, url):
        self.request_url = server + url
        self.headers = {"content-type": "application/json"}

    def get_request(self, request_info, parameter, value, safe=True):
        url_http = str(self.request_url + request_info + "?" + parameter + "=" + value)
        try:
            req = requests.get(url_http, headers=self.headers)
            if req.status_code != 200:
                if safe:
                    log.error(
                        "Unable to get parameters. Received error code %s",
                        req.status_code,
                    )
                    stderr.print(
                        "[red] Unable to fetch data. Received error ", req.status_code
                    )
                    sys.exit(1)
                return {"ERROR": req.status_code}
            return {"DATA": json.loads(req.text)}
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.server)
            stderr.print("[red] Unable to open connection towards ", self.server)
            return {"ERROR": "Server not available"}

    def put_request(self, request_info, parameter, value):
        url_http = str(self.request_url + request_info + "?" + parameter + "=" + value)
        try:
            requests.get(url_http, headers=self.headers)
            return True
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.server)
            return False

    def post_request(self, data, credentials):
        if isinstance(credentials, dict):
            auth = (credentials["user"], credentials["pass"])
        try:
            req = requests.post(
                self.request_url, data=data, headers=self.headers, auth=auth
            )
            if req.status_code != 201:
                log.error(
                    "Unable to store parameters. Received error code %s",
                    req.status_code,
                )
                stderr.print(
                    "[red] Unable to store data. Received error ", req.status_code
                )
                sys.exit(1)
            return True
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.server)
            stderr.print("[red] Unable to open connection towards ", self.server)
            return {"ERROR": "Server not available"}
        return {"Success": "Data are store in requested server"}
