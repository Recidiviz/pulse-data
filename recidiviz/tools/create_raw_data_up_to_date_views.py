# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""
Script for creating/updating all views in the us_xx_raw_data_up_to_date_views dataset.

When in dry-run mode (default), this will only log the output rather than uploading to BQ.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.create_raw_data_up_to_date_views \
    --project-id recidiviz-staging --state-code US_ID --dry-run True

"""

import argparse
import logging
from typing import List

from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.bq_utils import create_or_update_view
from recidiviz.calculator.query.bqview import BigQueryView
from recidiviz.utils.params import str_to_bool

US_ID_RAW_TABLES_TO_PKS = [
    ('applc_usr', ['usr_id']),
    ('assess_qstn', ['assess_qstn_num', 'tst_sctn_num', 'assess_tst_id']),
    ('assess_qstn_choice', ['qstn_choice_num', 'assess_qstn_num', 'tst_sctn_num', 'assess_tst_id']),
    ('assess_tst', ['assess_tst_id']),
    ('behavior_evaluation_source_cd', ['behavior_evaluation_source_cd']),
    ('body_loc_cd', ['body_loc_cd']),
    ('casemgr', ['move_srl', 'case_dtd']),
    ('cis_codepersonnamesuffix', ['id']),
    ('cis_codepersonnametype', ['id']),
    ('cis_offender', ['id']),
    ('cis_personname', ['id']),
    ('clssfctn_cust_lvl', ['clssfctn_cust_lvl_cd']),
    ('cntc_rslt_cd', ['cntc_rslt_cd']),
    ('cntc_typ_cd', ['cntc_typ_cd']),
    ('cntc_typ_subtyp_cd', ['cntc_typ_cd', 'cnt_subtyp_cd']),
    ('county', ['cnty_cd']),
    ('early_discharge', ['early_discharge_id']),
    ('early_discharge_form_typ', ['early_discharge_form_typ_id']),
    ('early_discharge_sent', ['early_discharge_sent_id']),
    ('ethnic', ['ethnic_cd']),
    ('facility', ['fac_cd']),
    ('hrchy', ['staff_usr_id']),
    ('judge', ['judge_cd']),
    ('jurisdiction_decision_code', ['jurisdiction_decision_code_id']),
    ('lgl_stat_cd', ['lgl_stat_cd']),
    ('loc_typ_cd', ['loc_typ_cd']),
    ('location', ['loc_cd']),
    ('lvgunit', ['fac_cd', 'lu_cd']),
    ('mittimus', ['mitt_srl']),
    ('movement', ['move_srl']),
    ('offender', ['docno']),
    ('offense', ['off_cat', 'off_cd', 'off_deg']),
    ('offstat', ['docno', 'incrno', 'statno']),
    ('ofndr_agnt', ['ofndr_num']),
    ('ofndr_behavior_evaluation', ['ofndr_behavior_evaluation_id']),
    ('ofndr_classifications', ['classification_id']),
    ('ofndr_dob', ['ofndr_num']),
    ('ofndr_sctn_eval', ['ofndr_tst_id', 'tst_sctn_num', 'assess_tst_id']),
    ('ofndr_tst', ['ofndr_tst_id']),
    ('ofndr_tst_cert', ['ofndr_tst_id']),
    ('ofndr_wrkld', ['ofndr_wrkld_id']),
    ('sentdisp', ['sent_disp']),
    ('sentence', ['mitt_srl', 'sent_no']),
    ('sentprob', ['mitt_srl', 'sent_no']),
    ('sentretn', ['mitt_srl', 'sent_no', 'retn_no']),
    ('sexcod', ['sex_cd']),
    ('sprvsn_cntc', ['sprvsn_cntc_id']),
    ('statrls', ['stat_rls_typ', 'stat_cd']),
    ('statstrt', ['stat_strt_typ']),
    ('tst_qstn_rspns', ['tst_qstn_rspns_id']),
    ('wrkld_cat', ['wrkld_cat_id']),
]

RAW_TABLES_TO_PKS_BY_STATE = {
    'us_id': US_ID_RAW_TABLES_TO_PKS,
}

LATEST_UPLOAD_DATE_QUERY = "(SELECT MAX(update_datetime) FROM `{project_id}.{dataset}.{table_name}`)"

PARAMETERIZED_UPDATE_TIMESTAMP = "@update_timestamp"

RAW_DATA_VIEW_QUERY_TEMPLATE = """
WITH rows_with_recency_rank AS (
   SELECT 
      *, 
      ROW_NUMBER() OVER (PARTITION BY {primary_keys} ORDER BY update_datetime DESC) AS recency_rank
   FROM 
      `{project_id}.{dataset}.{table_name}`
   WHERE 
       update_datetime <= {update_datetime}
)

SELECT * 
EXCEPT (file_id, recency_rank, update_datetime)
FROM rows_with_recency_rank
WHERE recency_rank = 1
"""


def create_or_update_view_for_table(
        raw_table_name: str,
        primary_keys: List[str],
        project_id: str,
        raw_dataset: str,
        views_dataset: str,
        dry_run: bool):
    """Creates/Updates views corresponding to the provided |raw_table_name|."""
    logging.info('===================== CREATING QUERIES FOR %s  =======================', raw_table_name)
    primary_key_str = ", ".join(primary_keys)
    by_update_date_view_id = f'{raw_table_name}_by_update_date'
    by_update_date_query = RAW_DATA_VIEW_QUERY_TEMPLATE.format(
        primary_keys=primary_key_str,
        project_id=project_id,
        dataset=raw_dataset,
        table_name=raw_table_name,
        update_datetime=PARAMETERIZED_UPDATE_TIMESTAMP
    )
    by_update_date_view = BigQueryView(view_id=by_update_date_view_id, view_query=by_update_date_query)

    latest_view_id = f'{raw_table_name}_latest'
    latest_query = RAW_DATA_VIEW_QUERY_TEMPLATE.format(
        primary_keys=primary_key_str,
        project_id=project_id,
        dataset=raw_dataset,
        table_name=raw_table_name,
        update_datetime=LATEST_UPLOAD_DATE_QUERY.format(
            project_id=project_id,
            dataset=raw_dataset,
            table_name=raw_table_name)
    )
    latest_view = BigQueryView(view_id=latest_view_id, view_query=latest_query)

    if dry_run:
        logging.info('[DRY RUN] would have created/updated view %s with query:\n %s',
                     latest_view.view_id, latest_view.view_query)
        logging.info('[DRY RUN] would have created/updated view %s with query:\n %s',
                     by_update_date_view.view_id, by_update_date_view.view_query)
        return

    views_dataset_ref = bq_utils.client().dataset(views_dataset, project=project_id)
    create_or_update_view(dataset_ref=views_dataset_ref, view=by_update_date_view)
    logging.info('Created/Updated view %s', by_update_date_view.view_id)
    create_or_update_view(dataset_ref=views_dataset_ref, view=latest_view)
    logging.info('Created/Updated view %s', latest_view.view_id)


def main(state_code: str, project_id: str, dry_run: bool):
    raw_tables_to_pks = RAW_TABLES_TO_PKS_BY_STATE.get(state_code)
    if raw_tables_to_pks is None:
        raise ValueError(f'No found tables to primary key associations for state {state_code}')

    raw_dataset = f'{state_code}_raw_data'
    views_dataset = f'{state_code}_raw_data_up_to_date_views'
    succeeded_tables = []
    failed_tables = []
    for raw_table_name, primary_keys in raw_tables_to_pks:
        try:
            create_or_update_view_for_table(
                raw_table_name=raw_table_name, primary_keys=primary_keys, project_id=project_id,
                raw_dataset=raw_dataset, views_dataset=views_dataset, dry_run=dry_run)
            succeeded_tables.append(raw_table_name)
        except Exception:
            failed_tables.append(raw_table_name)
            logging.exception("Couldn't create/update views for file %s", raw_table_name)

    logging.info('Succeeded tables %s', succeeded_tables)
    if failed_tables:
        logging.error('Failed tables %s', failed_tables)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--dry-run', default=True, type=str_to_bool,
                        help='Runs copy in dry-run mode, only prints the file copies it would do.')
    parser.add_argument('--project-id', required=True, help='The project_id to upload views to')
    parser.add_argument('--state-code', required=True, help='The state to upload views for.')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    main(project_id=args.project_id, state_code=args.state_code.lower(), dry_run=args.dry_run)
