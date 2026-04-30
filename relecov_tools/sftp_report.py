#!/usr/bin/env python
import json
import os
import re
import stat
import urllib.error
import urllib.request
from datetime import datetime, timedelta

import rich.console

from relecov_tools.base_module import BaseModule
from relecov_tools.config_json import ConfigJson
from relecov_tools.sftp_client import SftpClient
import relecov_tools.utils

stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class SftpReport(BaseModule):
    """Inspect SFTP folders and report laboratories with pending uploads."""

    def __init__(
        self,
        user=None,
        password=None,
        conf_file=None,
        target_folders=None,
        subfolder=None,
        metadata_pattern=None,
        since_days=7,
        all_files=False,
        include_empty=False,
        output_dir=None,
    ):
        super().__init__(output_dir=output_dir, called_module="sftp_report")
        config = ConfigJson(extra_config=True)
        download_cfg = config.get_configuration("download") or {}

        self.user = user or download_cfg.get("user")
        self.password = password or download_cfg.get("password")
        self.conf_file = conf_file or download_cfg.get("conf_file")
        self.target_folders = self._parse_target_folders(
            target_folders or download_cfg.get("target_folders")
        )
        self.subfolder = subfolder or download_cfg.get("subfolder") or "RELECOV"
        self.metadata_pattern = re.compile(
            metadata_pattern or r"metadata.*\.(xlsx|xls)$", re.IGNORECASE
        )
        self.since_days = since_days
        self.all_files = all_files
        self.include_empty = include_empty
        self.allowed_file_ext = tuple(
            config.get_topic_data("sftp_handle", "allowed_file_extensions") or []
        )
        self.skip_path_patterns = {
            "tmp_processing",
            "invalid_samples",
            config.get_topic_data("sftp_handle", "analysis_results_folder"),
        }
        self.relecov_sftp = SftpClient(self.conf_file, self.user, self.password)

    @staticmethod
    def _parse_target_folders(target_folders):
        if isinstance(target_folders, str):
            folders = target_folders.strip()
            if not folders:
                return None
            return [
                folder.strip()
                for folder in folders.strip("[").strip("]").split(",")
                if folder.strip()
            ]
        if isinstance(target_folders, list):
            return [folder.strip() for folder in target_folders if folder.strip()]
        return None

    def _is_sequence_file(self, filepath):
        filename = os.path.basename(filepath).lower()
        return filename.endswith(self.allowed_file_ext)

    def _is_metadata_file(self, filepath):
        filename = os.path.basename(filepath)
        return bool(self.metadata_pattern.search(filename))

    def _is_skipped_path(self, filepath):
        filepath = filepath.lower()
        return any(
            str(pattern).lower() in filepath
            for pattern in self.skip_path_patterns
            if pattern
        )

    def _cutoff_timestamp(self):
        if self.all_files or self.since_days in (None, ""):
            return None
        return (datetime.now() - timedelta(days=int(self.since_days))).timestamp()

    def _list_recent_files(self, folder_name, cutoff_timestamp):
        files = []

        def recursive_list(current_folder):
            for item in self.relecov_sftp.sftp.listdir_attr(current_folder):
                full_path = os.path.join(current_folder, item.filename)
                if self._is_skipped_path(full_path):
                    continue
                if stat.S_ISDIR(item.st_mode):
                    recursive_list(full_path)
                    continue
                if not stat.S_ISREG(item.st_mode):
                    continue
                if cutoff_timestamp is not None and item.st_mtime < cutoff_timestamp:
                    continue
                files.append(
                    {
                        "path": full_path,
                        "mtime": datetime.fromtimestamp(item.st_mtime).isoformat(
                            timespec="seconds"
                        ),
                    }
                )

        recursive_list(folder_name)
        return files

    def _get_laboratories(self):
        if self.target_folders:
            return sorted(
                {folder.strip("./").split("/")[0] for folder in self.target_folders}
            )

        remote_folders = self.relecov_sftp.list_remote_folders(".", recursive=True)
        labs = {
            folder.strip("./").split("/")[0]
            for folder in remote_folders
            if folder.strip("./")
        }
        return sorted(labs)

    def inspect_laboratory(self, lab_code):
        remote_folder = os.path.join(lab_code, self.subfolder)
        try:
            files = self._list_recent_files(
                remote_folder, cutoff_timestamp=self._cutoff_timestamp()
            )
        except (FileNotFoundError, OSError):
            files = []

        sequence_files = [
            file for file in files if self._is_sequence_file(file["path"])
        ]
        metadata_files = [
            file for file in files if self._is_metadata_file(file["path"])
        ]

        if sequence_files and metadata_files:
            status = "ready"
        elif sequence_files or metadata_files:
            status = "incomplete"
        else:
            status = "empty"

        return {
            "laboratory": lab_code,
            "remote_folder": remote_folder,
            "status": status,
            "sequence_files": len(sequence_files),
            "metadata_files": len(metadata_files),
            "total_files": len(files),
            "metadata_filenames": [
                os.path.basename(file["path"]) for file in metadata_files
            ],
        }

    def build_report(self):
        if not self.user:
            self.user = relecov_tools.utils.prompt_text(msg="Enter the user id")
            self.relecov_sftp.user_name = self.user
        if not self.password:
            self.password = relecov_tools.utils.prompt_password(
                msg="Enter your password"
            )
            self.relecov_sftp.password = self.password

        if not self.relecov_sftp.open_connection():
            raise ConnectionError("Unable to establish sftp connection")

        try:
            labs = self._get_laboratories()
            entries = [self.inspect_laboratory(lab) for lab in labs]
        finally:
            self.relecov_sftp.close_connection()

        if not self.include_empty:
            entries = [entry for entry in entries if entry["status"] != "empty"]

        summary = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "subfolder": self.subfolder,
            "since_days": None if self.all_files else int(self.since_days),
            "total_laboratories": len(labs),
            "reported_laboratories": len(entries),
            "ready": sum(entry["status"] == "ready" for entry in entries),
            "incomplete": sum(entry["status"] == "incomplete" for entry in entries),
            "empty": sum(entry["status"] == "empty" for entry in entries),
        }
        return {"summary": summary, "laboratories": entries}

    @staticmethod
    def format_text(report):
        summary = report["summary"]
        time_window = (
            "all files"
            if summary.get("since_days") is None
            else f"last {summary['since_days']} days"
        )
        lines = [
            f"SFTP upload report - {summary['generated_at']}",
            f"Subfolder: {summary['subfolder']}",
            f"Time window: {time_window}",
            "",
            (
                "Summary: "
                f"{summary['ready']} ready, "
                f"{summary['incomplete']} incomplete, "
                f"{summary['reported_laboratories']} reported laboratories"
            ),
        ]

        if not report["laboratories"]:
            lines.extend(["", "No pending uploads found."])
            return "\n".join(lines)

        lines.append("")
        for entry in report["laboratories"]:
            lines.append(
                "- {laboratory}: {status} ({sequence_files} sequence files, "
                "{metadata_files} metadata files)".format(**entry)
            )
        return "\n".join(lines)

    @staticmethod
    def format_slack(report):
        summary = report["summary"]
        time_window = (
            "todos los ficheros"
            if summary.get("since_days") is None
            else f"ultimos {summary['since_days']} dias"
        )
        lines = [
            ":calendar: *Revision semanal SFTP RELECOV*",
            f"_Ventana revisada: {time_window}_",
            "",
            (
                f"*Resumen:* {summary['ready']} listos, "
                f"{summary['incomplete']} incompletos, "
                f"{summary['reported_laboratories']} laboratorios con actividad."
            ),
        ]

        ready = [
            entry for entry in report["laboratories"] if entry["status"] == "ready"
        ]
        incomplete = [
            entry for entry in report["laboratories"] if entry["status"] == "incomplete"
        ]

        if ready:
            lines.extend(["", "*Laboratorios listos*"])
            for entry in ready:
                lines.append(
                    "- {laboratory}: {sequence_files} ficheros de secuencia, "
                    "{metadata_files} metadata".format(**entry)
                )
        if incomplete:
            lines.extend(["", "*Subidas incompletas*"])
            for entry in incomplete:
                lines.append(
                    "- {laboratory}: {sequence_files} ficheros de secuencia, "
                    "{metadata_files} metadata".format(**entry)
                )
        if not ready and not incomplete:
            lines.extend(["", "No se han encontrado subidas pendientes."])

        return "\n".join(lines)

    @staticmethod
    def format_json(report):
        return json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False)

    def render_report(self, output_format="text"):
        report = self.build_report()
        if output_format == "json":
            return self.format_json(report)
        if output_format == "slack":
            return self.format_slack(report)
        return self.format_text(report)

    @staticmethod
    def _build_slack_payload(message, channel=None):
        payload = {"text": message}
        if channel:
            payload["channel"] = channel
        return payload

    @staticmethod
    def send_slack_message(message, webhook_url, channel=None):
        if not webhook_url:
            raise ValueError(
                "Slack webhook URL not provided. Use --slack-webhook, "
                "RELECOV_SLACK_WEBHOOK_URL, SLACK_WEBHOOK_URL, or slack.webhook_url "
                "in extra_config.json."
            )

        payload = SftpReport._build_slack_payload(message, channel=channel)
        request = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                response_body = response.read().decode("utf-8")
        except urllib.error.URLError as e:
            raise ConnectionError(f"Could not send Slack report: {e}") from e

        if response_body.strip().lower() != "ok":
            raise RuntimeError(f"Unexpected Slack webhook response: {response_body}")
        return True

    @staticmethod
    def get_slack_config():
        config = ConfigJson(extra_config=True)
        slack_cfg = config.get_configuration("slack") or {}
        webhook_url = (
            os.environ.get("RELECOV_SLACK_WEBHOOK_URL")
            or os.environ.get("SLACK_WEBHOOK_URL")
            or slack_cfg.get("webhook_url")
            or slack_cfg.get("slack_webhook_url")
        )
        channel = os.environ.get("RELECOV_SLACK_CHANNEL") or slack_cfg.get("channel")
        return webhook_url, channel
