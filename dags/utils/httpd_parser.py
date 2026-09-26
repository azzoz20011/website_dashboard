import re
from datetime import datetime

import pyarrow as pa


ACCESS_SCHEMA = pa.schema([
    ("event_timestamp", pa.timestamp("us", tz="UTC")),
    ("client_ip", pa.string()),
    ("method", pa.string()),
    ("path", pa.string()),
    ("protocol", pa.string()),
    ("status_code", pa.int32()),
    ("response_bytes", pa.int64()),
    ("referrer", pa.string()),
    ("user_agent", pa.string()),
    ("source_file", pa.string()),
    ("ingestion_date", pa.date32()),
])


ERROR_SCHEMA = pa.schema([
    ("event_timestamp", pa.timestamp("us")),
    ("module", pa.string()),
    ("level", pa.string()),
    ("pid", pa.int32()),
    ("tid", pa.int32()),
    ("error_code", pa.string()),
    ("message", pa.string()),
    ("source_file", pa.string()),
    ("ingestion_date", pa.date32()),
])


ACCESS_PATTERN = re.compile(
    r'(?P<client_ip>\S+) '
    r'\S+ '
    r'\S+ '
    r'\[(?P<timestamp>[^\]]+)\] '
    r'"(?P<request>[^"]*)" '
    r'(?P<status_code>\d{3}) '
    r'(?P<response_bytes>\S+) '
    r'"(?P<referrer>[^"]*)" '
    r'"(?P<user_agent>[^"]*)"'
)


ERROR_PATTERN = re.compile(
    r'^\[(?P<timestamp>[^\]]+)\] '
    r'\[(?P<module>[^:\]]+):(?P<level>[^\]]+)\] '
    r'\[pid (?P<pid>\d+):tid (?P<tid>\d+)\] '
    r'(?P<error_code>AH\d+): '
    r'(?P<message>.*)$'
)


def parse_access_line(line, source_file, ingestion_date):
    match = ACCESS_PATTERN.match(line.strip())

    if not match:
        return None

    row = match.groupdict()

    try:
        timestamp = datetime.strptime(
            row["timestamp"],
            "%d/%b/%Y:%H:%M:%S %z",
        )

        status_code = int(row["status_code"])

        response_bytes = (
            None
            if row["response_bytes"] == "-"
            else int(row["response_bytes"])
        )

    except ValueError:
        return None

    request_parts = row["request"].split()

    method = request_parts[0] if len(request_parts) >= 1 else None
    path = request_parts[1] if len(request_parts) >= 2 else None
    protocol = request_parts[2] if len(request_parts) >= 3 else None

    return {
        "event_timestamp": timestamp,
        "client_ip": row["client_ip"],
        "method": method,
        "path": path,
        "protocol": protocol,
        "status_code": status_code,
        "response_bytes": response_bytes,
        "referrer": (
            None
            if row["referrer"] == "-"
            else row["referrer"]
        ),
        "user_agent": (
            None
            if row["user_agent"] == "-"
            else row["user_agent"]
        ),
        "source_file": source_file,
        "ingestion_date": ingestion_date,
    }


def parse_error_line(line, source_file, ingestion_date):
    match = ERROR_PATTERN.match(line.strip())

    if not match:
        return None

    row = match.groupdict()

    try:
        timestamp = datetime.strptime(
            row["timestamp"],
            "%a %b %d %H:%M:%S.%f %Y",
        )

        pid = int(row["pid"])
        tid = int(row["tid"])

    except ValueError:
        return None

    return {
        "event_timestamp": timestamp,
        "module": row["module"],
        "level": row["level"],
        "pid": pid,
        "tid": tid,
        "error_code": row["error_code"],
        "message": row["message"],
        "source_file": source_file,
        "ingestion_date": ingestion_date,
    }