from relecov_tools.sftp_report import SftpReport


def test_format_text_without_uploads():
    report = {
        "summary": {
            "generated_at": "2026-04-30T12:00:00",
            "subfolder": "RELECOV",
            "since_days": 7,
            "ready": 0,
            "incomplete": 0,
            "reported_laboratories": 0,
        },
        "laboratories": [],
    }

    text = SftpReport.format_text(report)

    assert "No pending uploads found." in text


def test_format_slack_groups_ready_and_incomplete_labs():
    report = {
        "summary": {
            "generated_at": "2026-04-30T12:00:00",
            "subfolder": "RELECOV",
            "since_days": 7,
            "ready": 1,
            "incomplete": 1,
            "reported_laboratories": 2,
        },
        "laboratories": [
            {
                "laboratory": "COD-001",
                "status": "ready",
                "sequence_files": 4,
                "metadata_files": 1,
            },
            {
                "laboratory": "COD-002",
                "status": "incomplete",
                "sequence_files": 2,
                "metadata_files": 0,
            },
        ],
    }

    text = SftpReport.format_slack(report)

    assert "*Laboratorios listos*" in text
    assert "COD-001" in text
    assert "*Subidas incompletas*" in text
    assert "COD-002" in text


def test_skipped_path_matches_invalid_samples_suffix():
    report = SftpReport.__new__(SftpReport)
    report.skip_path_patterns = {"invalid_samples"}

    assert report._is_skipped_path("COD-001/RELECOV/run_invalid_samples/file.xlsx")


def test_build_slack_payload_with_channel():
    payload = SftpReport._build_slack_payload("message", channel="#relecov")

    assert payload == {"text": "message", "channel": "#relecov"}
