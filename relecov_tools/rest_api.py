#!/usr/bin/env python
import logging
import requests
import rich.console
from typing import Any
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
            params (dict): dictionary of params for the get request

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
                response, success_codes=[200, 201], method="GET"
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
        return RestApi.standardize_response(response, success_codes=[201], method="PUT")

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
                response, success_codes=[201], method="POST"
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
            sample_data (dict): dictionary with sample metadata to use as input

        Raises:
            ValueError: if the conection was interrupted unexpectedly.

        Returns:
            bool: Wether the sample is found in the database or not.
        """
        response = self.get_request(
            api_func, credentials=credentials, params=sample_data, safe=False
        )
        log.info(str(response))
        if missing_keys := [
            key for key in ["message", "data", "status_code"] if key not in response
        ]:
            log.error(f"Missing keys in API response: {missing_keys}")
            stderr.print(f"Missing keys in API response: {missing_keys}")
            raise KeyError(f"Missing keys in API response: {missing_keys}")

        if response["status_code"] == 404:
            return False
        elif response["status_code"] == 200:
            return not bool(
                response["message"] != "Sample not found" or response["data"]
            )
        else:
            raise ValueError(f"Error trying to check for sample: {response}")

    @staticmethod
    def standardize_response(
        response,
        success_codes: list[int] = [200],
        method: str = "",
        *,
        message_keys: list[str] = ["message", "detail", "msg"],
        error_keys: list[str] = ["ERROR", "error"],
        data_keys: list[str] = ["data", "payload", "result"],
        include_raw_on_non_json: bool = False,
    ) -> dict[str, Any]:
        """
        Normalize a `requests.Response` into your canonical shape while preserving API-provided fields.

        Canonical output:
            {
              "message": <str>,                # Prefer API 'message'/'detail'/...
              "status_code": <int>,            # Always present
              "data": <obj>,                   # Prefer API 'data'/'payload'/'result', else {}
              "ERROR": <str> (optional)        # Included if API provided it, or if status is not success
              ["raw"]: <str> (optional)        # Raw text for non-JSON responses if enabled
            }

        Flexibility:
          - Accepts alternative key names via message_keys, error_keys, data_keys.
          - If the API already provides 'message', 'data', 'ERROR', those are preserved.
          - If response is not JSON, still returns the canonical keys.
        """
        status_code = getattr(response, "status_code", None)
        content_type = (
            response.headers.get("Content-Type", "")
            if hasattr(response, "headers")
            else ""
        )
        is_success = status_code in success_codes

        # Try to parse JSON safely
        payload: Any = None
        if "application/json" in content_type.lower():
            try:
                payload = response.json()
            except Exception:
                payload = None
        else:
            # Some APIs forget the content-type; try JSON parse as best effort
            try:
                payload = response.json()
            except Exception:
                payload = None

        # Initialize canonical result
        result: dict[str, Any] = {
            "message": "",
            "status_code": status_code,
            "data": {},
        }

        def _first_key(d: dict[str, Any], keys: list[str]):
            """Get the first key from `keys` that is present in `d`

            Args:
            d : dict
                d is a dict to search
            keys
                list of keys to search

            Returns:
            key, value : tuple  returns the first key and value
            """
            for k in keys:
                if isinstance(d, dict) and k in d and d[k] not in (None, ""):
                    return k, d[k]
            return None, None

        if isinstance(payload, dict):
            # Prefer exact API fields if present
            # ERROR
            err_key, err_val = _first_key(payload, list(error_keys))
            if err_key is not None:
                result["ERROR"] = err_val

            # message - prefer API 'message'/'detail'/...
            if "message" in payload and payload["message"] is not None:
                result["message"] = payload["message"]
            else:
                _, alt_msg = _first_key(payload, list(message_keys))
                if alt_msg is not None:
                    result["message"] = alt_msg
                elif "ERROR" in result:
                    result["message"] = str(result["ERROR"])  # fallback to error text
                else:
                    result["message"] = "OK" if is_success else "Unexpected error"

            # data - prefer API 'data'/'payload'/'result'
            if "data" in payload:
                result["data"] = payload["data"]
            else:
                _, alt_data = _first_key(payload, list(data_keys))
                if alt_data is not None:
                    result["data"] = alt_data
                else:
                    # fallback: use all non-canonical keys
                    canon_keys = set(message_keys) | set(error_keys) | set(data_keys)
                    canon_keys |= {"message", "ERROR", "data"}
                    extra = {k: v for k, v in payload.items() if k not in canon_keys}
                    if extra:
                        result["data"] = extra
                    else:
                        result["data"] = {}

            # If HTTP status is not success and API did not include ERROR, synthesize it
            if not is_success and "ERROR" not in result:
                result["ERROR"] = result["message"] or f"HTTP {status_code}"

        else:
            # Non-JSON response
            text = ""
            try:
                text = response.text or ""
            except Exception:
                text = ""

            if not is_success:
                # On error, surface the body as ERROR (truncated to avoid huge logs)
                truncated = text.strip()[:1000]
                result["ERROR"] = truncated or "Unexpected error"
                result["message"] = truncated or "Unexpected error"
            else:
                # On success without JSON, be conservative
                result["message"] = "OK"
                result["data"] = {}

            if include_raw_on_non_json:
                result["raw"] = text

        # Logging
        if not is_success:
            logtxt = (
                f"Unable to {method} parameters. Received '{result.get('ERROR','Unexpected error')}' "
                f"with status code: {status_code}"
            )
            log.error(logtxt)
        else:
            logtxt = (
                f"Successful {method} response. Received '{result.get('message','OK')}' "
                f"with status code: {status_code}"
            )
            log.info(logtxt)

        return result
