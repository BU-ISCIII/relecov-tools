#!/usr/bin/env python
import logging
import json
import requests

log = logging.getLogger(__name__)


class RestApi:
    def __init__(self, server, url):
        self.request_url = server + url
        self.headers = {"content-type": "application/json"}

    def get_request(self, request_info, parameter, value):
        url_http = str(self.request_url + request_info + "?" + parameter + "=" + value)
        try:
            req = requests.get(url_http, headers=self.headers)
            if req.status_code > 201:
                return False
            return json.loads(req.text)
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.server)
            return False

    def put_request(self, request_info, parameter, value):
        url_http = str(self.request_url + request_info + "?" + parameter + "=" + value)
        try:
            requests.get(url_http, headers=self.headers)
            return True
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.server)
            return False

    def post_request(self, data):
        try:
            req = requests.post(self.request_url, data=data, headers=self.headers)
            if req.status_code > 201:
                log.error(str(req.status_code))
                return False
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.server)
        return True
