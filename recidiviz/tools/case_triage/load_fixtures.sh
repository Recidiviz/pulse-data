#!/usr/bin/env bash

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

# This script should be run only after `docker-compose -f docker-compose.case-triage.yaml up`
# has been run. This will delete everything from the etl_* tables and then re-add them from the
# fixture files.


# These are the default values from docker-compose.case-triage.yaml
PGDBHOST='localhost'
PGDBUSER='postgres'
PGDB='postgres'

# Delete old data first
PGPASSWORD='example' psql -h $PGDBHOST \
    -U $PGDBUSER \
    -d $PGDB \
    -c "DELETE FROM etl_clients;"
PGPASSWORD='example' psql -h $PGDBHOST \
    -U $PGDBUSER \
    -d $PGDB \
    -c "DELETE FROM etl_officers;"

# Run CSV import
PGPASSWORD='example' psql -h $PGDBHOST \
    -U $PGDBUSER \
    -d $PGDB \
    -c "\copy etl_officers FROM 'recidiviz/tools/case_triage/fixtures/etl_officers.csv' delimiter ',' csv"
PGPASSWORD='example' psql -h $PGDBHOST \
    -U $PGDBUSER \
    -d $PGDB \
    -c "\copy etl_clients FROM 'recidiviz/tools/case_triage/fixtures/etl_clients.csv' delimiter ',' csv"
