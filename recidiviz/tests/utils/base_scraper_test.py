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
import yaml
from recidiviz.ingest import ingest_info_utils
from recidiviz.ingest.models import ingest_info
from recidiviz.persistence.validator import validate


class BaseScraperTest(object):
    """A base class for scraper tests which does extra validations."""

    def setup_method(self, _test_method):
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
                         'keys_to_ignore']):
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
            self, content, params, expected_result):
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
        result = self.scraper.get_more_tasks(content, params)

        # Make sure at minimum endpoint and tasktype are returned.
        for key in ['endpoint', 'task_type']:
            for param in result:
                if key not in param:
                    raise AttributeError(
                        "No value for '%s' exists in the"
                        " result, at minimum a call to get_more_tasks must have"
                        " endpoint and task_type" % key)

        assert result == expected_result

    def validate_and_return_populate_data(
            self, content, params, expected_result, info=None):
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
        result = self.scraper.populate_data(content, params, info)

        # Attempt to convert the result to the final proto and validate its
        # output.
        result_proto = ingest_info_utils.convert_ingest_info_to_proto(result)
        validate(result_proto)

        assert result == expected_result
        return result
