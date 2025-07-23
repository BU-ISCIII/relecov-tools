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
        self.UNABLE_TO_CONNECT = {
            "ERROR": "Server not available",
            "status_code": "503 Service Unavailable",
            "data": {},
        }

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
            dict: Same as RestApi.standardize_response() dict
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
            response = requests.get(
                url_http, headers=self.headers, auth=credentials, params=params
            )
            return RestApi.standardize_response(
                response, success_code=200, method="GET"
            )
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.request_url)
            stderr.print("[red] Unable to open connection towards ", self.request_url)
            return self.UNABLE_TO_CONNECT

    def put_request(self, data, credentials, url):
        """Send a PUT request to update data on the server.

        Args:
            data (dict): Data payload for the PUT request.
            credentials (dict): Basic auth credentials, with 'user' and 'pass' keys.
            url (str): Endpoint path appended to the base URL.

        Returns:
            dict: Same as RestApi.standardize_response() dict
        """
        if isinstance(credentials, dict):
            auth = (credentials["user"], credentials["pass"])
        url_http = str(self.request_url + url)
        try:
            response = requests.put(url_http, data=data, auth=auth)
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.request_url)
            stderr.print("[red] Unable to open connection towards ", self.request_url)
            return self.UNABLE_TO_CONNECT
        return RestApi.standardize_response(response, success_code=201, method="PUT")

    def post_request(self, data, credentials, url, file=None):
        """Send a POST request with optional file upload.

        Args:
            data (dict): Data payload for the POST request.
            credentials (dict): Basic auth credentials, with 'user' and 'pass' keys.
            url (str): Endpoint path appended to the base URL.
            file (Optional[str]): Path to a file to be uploaded (if any).

        Returns:
            dict: Same as RestApi.standardize_response() dict
        """
        if isinstance(credentials, dict):
            auth = (credentials["user"], credentials["pass"])
        url_http = str(self.request_url + url)
        try:
            if file:
                files = {"upload_file": open(file, "rb")}
                response = requests.post(
                    url_http, files=files, data=data, headers=self.headers, auth=auth
                )
            else:
                response = requests.post(
                    url_http, data=data, headers=self.headers, auth=auth
                )
            return RestApi.standardize_response(
                response, success_code=201, method="POST"
            )
        except requests.ConnectionError:
            log.error("Unable to open connection towards %s", self.request_url)
            stderr.print("[red] Unable to open connection towards ", self.request_url)
            return self.UNABLE_TO_CONNECT

    def sample_already_in_db(self, api_func, credentials, sample_data):
        """Check if sample with data already exists in the target platform

        Args:
            api_func (str): Api functionality to check if sample is present
            credentials (dict["user": user, "pass": pass]): Credentials dictionary
            sample_data (dict): Dictionary with sample metadata to use as input

        Raises:
            ValueError: if the conection was interrupted unexpectedly.

        Returns:
            bool: Wether the sample is found in the database or not.
        """
        response = self.get_request(
            api_func, credentials=credentials, params=sample_data, safe=False
        )
        log.info(str(response))
        if response["status_code"] == 404:
            return False
        elif response["status_code"] == 200:
            return True
        else:
            raise ValueError(f"Error trying to check for sample: {response}")

    @staticmethod
    def standardize_response(response, success_code=200, method=""):
        """
        Parses a `requests.Response` object into a standardized dictionary format.

        Args:
            response (requests.Response): The HTTP response object to parse.
            success_code (int): Expected HTTP status code indicating success. Default is 200.

        Returns:
            dict: Standardized dictionary with:
                - "Success" or "ERROR": response.text
                - "status_code": HTTP status code
                - "data": Parsed JSON body if content-type is application/json, else empty dict.
        """
        response_header = response.headers.get("Content-Type", "")
        try:
            data = response.json() if "application/json" in response_header else {}
        except ValueError:
            data = {}
        if data:
            message = (
                data.get("detail")
                or data.get("error")
                or data.get("ERROR")
                or data.get("message")
                or f"Unexpected error"
            )
        else:
            if response.status_code != success_code:
                message = "Unexpected error"
            else:
                message = "No message"
        if response.status_code != success_code:
            logtxt = f"Unable to {method} parameters. Received '{message}' with status code: {response.status_code}"
            log.error(logtxt)
            stderr.print(f"[red]{logtxt}")
            return {"ERROR": message, "status_code": response.status_code, "data": data}
        else:
            logtxt = f"Successful {method} response. Received '{message}' with status code: {response.status_code}"
            log.info(logtxt)
            return {
                "Success": message,
                "status_code": response.status_code,
                "data": data,
            }
