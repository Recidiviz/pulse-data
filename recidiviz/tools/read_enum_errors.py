"""Parses enum errors from logs.

Run the following to generate an input from production logs:
$ gcloud logging read \
'resource.type="gae_app"
logName="projects/recidiviz-123/logs/app"
"could not parse"' --project recidiviz-123 --format json > FILE

Note to look at staging errors replace all 'recidiviz-123's with
'recidiviz-staging'

Then run this against the file:
$ python -m recidiviz.tools.read_enum_errors --file FILE [--include-request]
"""
import argparse
import json
import pprint
from typing import List, Tuple


def enum_errors_from_logs(filename: str) -> List[Tuple[str, str, str]]:
    """Extract unique enum errors from the log file"""
    errors: List[Tuple[str, str, str]] = []
    with open(filename) as logs_file:
        logs = json.loads(logs_file.read())
        for log in logs:
            enum_type, enum_string = extract_enum_string_type(
                log['jsonPayload']['message']['text'])
            region = log['jsonPayload']['message']['region']

            assert region and enum_type and enum_string
            error = (region, enum_type, enum_string)
            if error not in errors:
                errors.append(error)
    return errors


def extract_enum_string_type(text: str) -> Tuple[str, str]:
    assert text.startswith('Could not parse')
    text = text[len('Could not parse'):].strip()
    enum_string = text[:text.find(' when building ')].strip()
    enum_type = text.split("'")[-2].strip()
    return enum_type, enum_string


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file')
    args = parser.parse_args()

    enum_errors = enum_errors_from_logs(args.file)
    pprint.pprint(enum_errors)
