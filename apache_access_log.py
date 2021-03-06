
import re

from pyspark.sql import Row


APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)'

# Returns a dictionary containing the parts of the Apache Access Log.
def parse_apache_log_line(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    # Handle the case when not able parse the log line

    # Need a better regular expression here.
    if match is None:
        return Row(
        ip_address    = '',
        client_identd = '',
        user_id       = '',
        date_time     = '',
        method        = '',
        endpoint      = '',
        protocol      = '',
        response_code = int(0),
        content_size  = long(0)
        )
    else:
        return Row(
        ip_address    = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = match.group(4),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = long(match.group(9))
        )
