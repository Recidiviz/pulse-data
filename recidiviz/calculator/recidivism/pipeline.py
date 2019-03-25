# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""MapReduce pipeline for recidivism calculation.

This includes functions for the `map` and `reduce` phases of the pipeline,
a pipeline class that composes all MapReduce components, and a request handler
class that initiates pipelines on web requests.
"""


from .calculator import map_recidivism_combinations
from .identifier import find_recidivism
from .metrics import RecidivismMetric

# placeholder - these should be distributed counters
COUNTERS = {
    'total_people_mapped': 0,
    'total_metric_combinations_mapped': 0,
    'unique_metric_keys_reduced': 0,
    'total_records_reduced': 0,
    'total_recidivisms_reduced': 0,
}


# placeholder - should be mapreduce execution id
EXECUTION_ID = 1234

def map_person(person, records, snapshots):
    """Performs the `map` phase of the pipeline.

    Maps the Person read from the database into a set of metric combinations for
    each Record from which that Person has been released from prison. That is,
    this identifies each instance of recidivism or not-recidivism following a
    release from prison, and maps each to all unique combinations of
    characteristics whose calculations will be impacted by that instance.

    Args:
        person: a Person to be mapped into recidivism metrics.
        records: placeholder - records for person ordered by custody date
        snapshots: placeholder - snapshots for person ordered descending by
            creation date

    Yields:
        Metrics for each unique recidivism metric derived from the person. Also
        MapReduce Increment counters for a variety of metrics related to the
        `map` phase.
    """
    params = {}
    include_conditional_violations = \
        params.get('include_conditional_violations')

    recidivism_events = find_recidivism(records, snapshots,
                                        include_conditional_violations)
    metric_combinations = map_recidivism_combinations(person, recidivism_events)

    COUNTERS['total_people_mapped'] += 1

    for combination in metric_combinations:
        COUNTERS['total_metric_combinations_mapped'] += 1
        yield combination


def reduce_recidivism_events(metric_key, values):
    """Performs the `reduce` phase of the pipeline.

    Takes in a unique key, i.e. a mapping of characteristics to identify a
    recidivism metric, and the set of all recidivism or not-recidivism instances
    for that combination of characteristics, and calculates the overall
    recidivism rate for that combination.

    Args:
        metric_key: the mapping of characteristics for a particular recidivism
            metric. This is a json-serialized string representation of a
            dictionary because all keys in the GAE MapReduce framework must be
            strings.
        values: a list containing recidivism values, i.e. 0s, for instances when
            recidivism did not occur, 1s when it did occur, or maybe floating
            point values between (0,1] where the methodology is 'OFFENDER'.

    Yields:
        A RecidivismMetric instance to be persisted by the framework. Also
        MapReduce Increment counters for a variety of metrics related to the
        `reduce` phase.
    """
    if not values:
        # This method should never be called by MapReduce with an
        # empty values parameter, but we'll be defensive.
        return

    total_records = len(values)
    value_numerals = map(float, values)
    total_recidivism = sum(event for event in value_numerals if event > 0)

    metric = to_metric(metric_key, total_records, total_recidivism)

    COUNTERS['unique_metric_keys_reduced'] += 1
    COUNTERS['total_records_reduced'] += total_records
    COUNTERS['total_recidivisms_reduced'] += total_recidivism

    yield metric


def to_metric(metric_key, total_records, total_recidivism):
    """Transforms a metric key and values into a RecidivismMetric.

    Transforms the metric key and the number of total records and recidivating
    records into a new RecidivismMetric instance for persistence.

    Args:
        metric_key: the mapping of characteristics for a particular recidivism
            metric. This is a json-serialized string representation of a
            dictionary because all keys in the GAE MapReduce framework must be
            strings.
        total_records: the integer number of records to whom this metric key
            applies.
        total_recidivism: the total number of records that led to recidivism
            that the characteristics in this metric describe. This is a float
            because the 'OFFENDER' methodology calculates a weighted recidivism
            number based on total recidivating instances for the offender.

    Returns:
        A RecidivismMetric instance ready to persist.
    """
    recidivism_rate = ((total_recidivism + 0.0) / total_records)

    metric = RecidivismMetric()
    metric.execution_id = EXECUTION_ID
    metric.total_records = total_records
    metric.total_recidivism = total_recidivism
    metric.recidivism_rate = recidivism_rate

    # TODO: eval is evil. Find a better way to pass the key through
    # map and shuffle to reduce as a dictionary.
    key_mapping = eval(metric_key)  # pylint: disable=eval-used

    metric.release_cohort = key_mapping['release_cohort']
    metric.follow_up_period = key_mapping['follow_up_period']
    metric.methodology = key_mapping['methodology']

    if 'age' in key_mapping:
        metric.age_bucket = key_mapping['age']
    if 'race' in key_mapping:
        metric.race = key_mapping['race']
    if 'sex' in key_mapping:
        metric.sex = key_mapping['sex']
    if 'release_facility' in key_mapping:
        metric.release_facility = key_mapping['release_facility']
    if 'stay_length' in key_mapping:
        metric.stay_length_bucket = key_mapping['stay_length']

    return metric
