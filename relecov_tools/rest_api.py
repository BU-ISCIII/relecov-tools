#!/usr/bin/env python
import logging
import requests
import rich.console
import relecov_tools.utils
from requests.auth import HTTPBasicAuth

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

    def get_request(
        self,
        request_info,
        parameter=None,
        value=None,
        safe=True,
        credentials=None,
        params={},
    ):
        """Send a GET request with optional parameters and authentication.

        Args:
            request_info (str): Endpoint path appended to the base URL.
            parameter ([str, dict, None]): Query parameter(s). If a string, used with `value`.
            If given as a dict, key-value pairs are joined as query string. If empty or None, no parameters.
            value (Optional[str]): Value for the `parameter` if it is a string.
            safe (bool): If True, errors are logged and not raised. Defaults to True.
            credentials ([dict, tuple, list, None]): Basic auth credentials. Can be a dict with
            'user' and 'pass' as keys, or a tuple/list (user, pass).
            params (dict): Dictionary of params for the get request

        Returns:
            dict: On success, returns {'DATA': <parsed JSON response>}.
                  On failure, returns {'ERROR': <status code or error message>}.
        """
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
        if credentials is not None:
            if isinstance(credentials, dict):
                credentials = HTTPBasicAuth(credentials["user"], credentials["pass"])
            elif isinstance(credentials, (list, tuple)):
                credentials = HTTPBasicAuth(credentials[0], credentials[1])
        try:
            req = requests.get(
                url_http, headers=self.headers, auth=credentials, params=params
            )
            if req.status_code != 200:
                response = req.json()
                message = (
                    response.get("detail")
                    or response.get("error")
                    or response.get("ERROR")
                    or response.get("message")
                    or f"Unexpected error with status {req.status_code}"
                )
                if safe:
                    log.error(
                        f"Unable to get parameters. Received '{message}' error code {req.status_code}"
                    )
                    stderr.print(
                        f"[red]Unable to fetch data. Received '{message}' with error {req.status_code}"
                    )
                return {"ERROR": message, "status_code": req.status_code}
            return {"DATA": req.json(), "status_code": req.status_code}
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.request_url)
            stderr.print("[red] Unable to open connection towards ", self.request_url)
            return {
                "ERROR": "Server not available",
                "status_code": "503 Service Unavailable",
            }

    def put_request(self, data, credentials, url):
        """Send a PUT request to update data on the server.

        Args:
            data (dict): Data payload for the PUT request.
            credentials (dict): Basic auth credentials, with 'user' and 'pass' keys.
            url (str): Endpoint path appended to the base URL.

        Returns:
            dict: {'Success': <response text>} on success,
                  {'ERROR': <status code or message>} on failure.
        """
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
        """Send a POST request with optional file upload.

        Args:
            data (dict): Data payload for the POST request.
            credentials (dict): Basic auth credentials, with 'user' and 'pass' keys.
            url (str): Endpoint path appended to the base URL.
            file (Optional[str]): Path to a file to be uploaded (if any).

        Returns:
            dict: {'Success': <response text>} on success,
                  {'ERROR': <status code>, 'ERROR_TEST': <error text>} on failure.
        """
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
