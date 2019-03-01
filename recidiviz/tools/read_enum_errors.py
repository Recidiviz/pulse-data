"""Parses enum errors from logs.

Run the following to generate an input from production logs:
$ gcloud logging read \
'resource.type="gae_app"
logName="projects/recidiviz-123/logs/app"
"EnumParsingError"' --project recidiviz-123 --format json > FILE

Note to look at staging errors replace all 'recidiviz-123's with
'recidiviz-staging'

Then run this against the file:
$ python -m recidiviz.tools.read_enum_errors --file FILE [--include-request]
"""
import argparse
import ast
import json
import pprint
from typing import Any, Dict, Tuple


def enum_errors_from_logs(filename: str) -> Dict[Tuple[str, str, str], Any]:
    """Extract unique enum errors from the log file"""
    errors: Dict[Tuple[str, str, str], Any] = {}
    with open(filename) as logs_file:
        logs = json.loads(logs_file.read())
        for log in logs:
            lines = log['jsonPayload']['message'].splitlines()

            enum_type = enum_string = region = None

            # Iterate over the lines to find the relavant information.
            line_iter = iter(lines)
            while True:
                line = next(line_iter)
                if line.startswith('recidiviz.common.constants.entity_enum.'
                                   'EnumParsingError'):
                    enum_type, enum_string = extract_enum_string_type(line)
                if line.startswith('recidiviz.ingest.scrape.worker.'
                                   'RequestProcessingError'):
                    region = extract_region(line)
                    # Keep the rest of the lines containing the request in the
                    # iterator.
                    break
            assert region and enum_type and enum_string
            error = (region, enum_type, enum_string)
            if error not in errors:
                # Build the request dict from the rest of the lines in the
                # iterator and put it in errors.
                request = ast.literal_eval('\n'.join(list(line_iter)))
                errors[error] = request
    return errors


def extract_region(line: str) -> str:
    _, text = map(str.strip, line.split(':', 1))
    text = text[text.find('for') + len('for'):].strip()
    return text[:text.find('with')].strip().strip('\'')


def extract_enum_string_type(line: str) -> Tuple[str, str]:
    _, text = map(str.strip, line.split(':', 1))
    assert text.startswith('Could not parse')
    text = text[len('Could not parse'):].strip()
    enum_string = text[:text.find(' when building ')].strip()
    enum_type = text.split("'")[-2].strip()
    return enum_type, enum_string


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file')
    parser.add_argument('--include-request',
                        required=False, action='store_true')
    args = parser.parse_args()

    enum_errors = enum_errors_from_logs(args.file)
    pprint.pprint(enum_errors
                  if args.include_request
                  else set(enum_errors.keys()))
