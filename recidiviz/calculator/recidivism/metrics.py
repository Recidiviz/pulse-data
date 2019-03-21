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

"""Recidivism metrics we calculate."""


class RecidivismMetric:
    """Models a single recidivism metric.

    A recidivism metric contains a recidivism rate, including the numerator of
    total instances of recidivism and a denominator of total instances of
    release from prison. It also contains all of the identifying characteristics
    of the metric, including required characteristics for normalization as well
    as optional characteristics for slicing the data.

    Attributes:
        execution_id: the string id of the calculation pipeline that produced
            this metric. Required.
        release_cohort: the integer year during which the person was released.
            Required.
        follow_up_period: the integer number of years after date of release
            duringwhich recidivism was measured, e.g. the recidivism rate within
            5 years of release. Required.
        methodology: the calculation methodology for the metric, i.e. 'EVENT' or
            'OFFENDER'. Required.
        age_bucket: the age bucket string of the person the metric describes,
            e.g. '<25' or '35-39'.
        stay_length_bucket: the bucket string of the person's prison stay length
            (in months), e.g., '<12' or '36-48'.
        race: the race of the person the metric describes.
        sex: the sex of the person the metric describes.
        release_facility: the facility the person was released from prior to
            recidivating.
        total_records: the integer number of records that the characteristics in
            this metric describe.
        total_recidivism: the total number of records that led to recidivism
            that the characteristics in this metric describe. This is a float
            because the 'OFFENDER' methodology calculates a weighted recidivism
            number based on total recidivating instances for the offender.
        recidivism_rate: the float rate of recidivism for the records that the
            characteristics in this metric describe.
        created_on: a DateTime for when this metric was created.
        updated_on: a DateTime for when this metric was last updated.

    """

    def __init__(self, execution_id=None, release_cohort=None,
                 follow_up_period=None, methodology=None, age_bucket=None,
                 stay_length_bucket=None, race=None, sex=None,
                 release_facility=None, total_records=None,
                 total_recidivism=None, recidivism_rate=None, created_on=None,
                 updated_on=None):
        # Id of the calculation pipeline that created this metric
        self.execution_id = execution_id

        # Required characteristics
        self.release_cohort = release_cohort
        self.follow_up_period = follow_up_period
        self.methodology = methodology

        # Optional characteristics
        self.age_bucket = age_bucket
        self.stay_length_bucket = stay_length_bucket
        self.race = race
        self.sex = sex
        self.release_facility = release_facility

        # Metric values
        self.total_records = total_records
        self.total_recidivism = total_recidivism
        self.recidivism_rate = recidivism_rate

        # Record keeping fields
        self.created_on = created_on
        self.updated_on = updated_on
