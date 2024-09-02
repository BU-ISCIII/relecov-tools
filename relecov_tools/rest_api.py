#!/usr/bin/env python
import logging
import json
import requests
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

    def get_request(self, request_info, parameter, value=None, safe=True):
        if parameter == "" or parameter is None:
            url_http = str(self.request_url + request_info)
        elif isinstance(parameter, dict):
            param_value = []
            for key, value in parameter.items():
                param_value.append(key + "=" + value)
            url_http = str(
                self.request_url + request_info + "?" + "&".join(param_value)
            )
        else:
            url_http = str(
                self.request_url + request_info + "?" + parameter + "=" + value
            )
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
                return {"ERROR": req.status_code}
            return {"DATA": json.loads(req.text)}
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.request_url)
            stderr.print("[red] Unable to open connection towards ", self.request_url)
            return {"ERROR": "Server not available"}

    def put_request(self, data, credentials, url):
        if isinstance(credentials, dict):
            auth = (credentials["user"], credentials["pass"])
        url_http = str(self.request_url + url)
        try:
            req = requests.put(url_http, data=data, auth=auth)
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.request_url)
            stderr.print("[red] Unable to open connection towards ", self.request_url)
            return {"ERROR": "Server not available"}
        if req.status_code != 201:
            log.error(
                "Unable to post parameters. Received error code %s",
                req.status_code,
            )
            if req.status_code != 500:
                stderr.print(f"[red] Unable to put data because  {req.text}")
            stderr.print(f"[red] Received error {req.status_code}")
            return {"ERROR": req.status_code}
        return {"Success": req.text}

    def post_request(self, data, credentials, url, file=None):
        if isinstance(credentials, dict):
            auth = (credentials["user"], credentials["pass"])
        url_http = str(self.request_url + url)
        try:
            if file:
                files = {"upload_file": open(file, "rb")}
                req = requests.post(
                    url_http, files=files, data=data, headers=self.headers, auth=auth
                )
            else:
                req = requests.post(
                    url_http, data=data, headers=self.headers, auth=auth
                )
            if req.status_code != 201:
                log.error(
                    "Unable to post parameters. Received error code %s",
                    req.status_code,
                )
                stderr.print(f"[red] Received error {req.status_code}")
                if req.status_code != 500:
                    stderr.print(f"[red] Unable to post data because  {req.text}")
                    return {"ERROR": req.status_code, "ERROR_TEST": req.text}
                else:
                    return {"ERROR": req.status_code, "ERROR_TEST": ""}
            return {"Success": req.text}
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.request_url)
            stderr.print("[red] Unable to open connection towards ", self.request_url)
            return {"ERROR": "Server not available"}
