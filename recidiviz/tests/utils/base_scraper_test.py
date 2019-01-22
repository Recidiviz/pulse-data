# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================

"""Base test class for scrapers."""
from datetime import datetime

import yaml

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest import ingest_utils
from recidiviz.ingest import constants
from recidiviz.ingest.models import ingest_info
from recidiviz.ingest.models.ingest_info_diff import diff_ingest_infos
from recidiviz.ingest.task_params import Task
from recidiviz.persistence.converter import converter
from recidiviz.persistence.validator import validate

_FAKE_SCRAPER_START_TIME = datetime(year=2019, month=1, day=2)


class BaseScraperTest:
    """A base class for scraper tests which does extra validations."""

    def setUp(self):
        self.scraper = None
        self.yaml = None
        self._init_scraper_and_yaml()

    def test_scraper_not_none(self):
        if not self.scraper:
            raise AttributeError("The scraper instance must be set")

    def test_yaml_is_correct(self):
        if self.yaml:
            with open(self.yaml, 'r') as ymlfile:
                manifest = yaml.load(ymlfile)

            person = ingest_info.IngestInfo().create_person()
            booking = person.create_booking()
            charge = booking.create_charge()
            arrest = booking.create_arrest()
            sentence = charge.create_sentence()
            bond = charge.create_bond()
            object_verification_map = {
                'person': person,
                'booking': booking,
                'charge': charge,
                'arrest': arrest,
                'bond': bond,
                'sentence': sentence
            }

            # Validate that key_mappings exists
            if 'key_mappings' not in manifest:
                raise AttributeError("key_mappings must exist in the manifest")

            # Make sure there are no unknown keys
            for key in manifest:
                if (key not in
                        ['key_mappings', 'multi_key_mapping',
                         'keys_to_ignore', 'css_key_mappings']):
                    raise AttributeError("Unknown yaml key %s" % key)

            # Make sure every mapped value in the yaml file exists as a variable
            # in the relevant class.
            for value in manifest['key_mappings'].values():
                class_to_set, attr = value.split('.')
                if attr not in vars(object_verification_map[class_to_set]):
                    raise AttributeError(
                        "Attribute %s is unknown on %s, found in key_mappings"
                        % (attr, class_to_set))

            if 'multi_key_mappings' in manifest:
                for value in manifest['multi_key_mappings'].values():
                    class_to_set, attr = value.split('.')
                    if attr not in vars(object_verification_map[class_to_set]):
                        raise AttributeError(
                            "Attribute %s is unknown on %s, found in "
                            "multi_key_mappings" % (attr, class_to_set))

    def validate_and_return_get_more_tasks(
            self, content, task, expected_result):
        """This function runs get more tasks and runs some extra validation
        on the output.

        Args:
            content: the content of the page to pass into get_more_tasks
            params: the params to pass into get_more_tasks
            expected_result: the result we expect from the call

        Returns:
            The result from get_more_tasks in case the user needs to do any
            extra validations on the output.
        """
        result = self.scraper.get_more_tasks(content, task)
        assert result == expected_result

    def validate_and_return_populate_data(
            self, content, expected_result, task=None, info=None):
        """This function runs populate_data and runs some extra validation
        on the output.

        Args:
            content: the content of the page to pass into get_more_tasks
            params: the params to pass into get_more_tasks
            expected_result: the result we expect from the call
            info: an ingest_info to use if provided.

        Returns:
            The result from populate_data in case the user needs to do any
            extra validations on the output.
        """
        info = info or ingest_info.IngestInfo()
        task = task or Task(task_type=constants.TaskType.SCRAPE_DATA,
                            endpoint='')

        result = self.scraper.populate_data(content, task, info)

        # Attempt to convert the result to the ingest info proto,
        # validate the proto, and finally attempt to convert the proto into
        # our entitiy/ objects (which includes parsing strings into types)
        result_proto = ingest_utils.convert_ingest_info_to_proto(result)
        validate(result_proto)
        metadata = IngestMetadata(
            self.scraper.region, _FAKE_SCRAPER_START_TIME,
            self.scraper.get_enum_overrides())
        converter.convert(result_proto, metadata)

        differences = diff_ingest_infos(expected_result, result)

        if differences:
            self.fail('IngestInfo objects do not match.\n'
                      'Expected:\n{}\n'
                      'Actual:\n{}\n'
                      'Differences:\n{}\n\n'
                      '(paste the following) scraped object:'
                      '\n{}'.format(expected_result,
                                    result,
                                    '\n'.join(differences),
                                    repr(result)))

        return result
