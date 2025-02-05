# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Query containing supervision sentence length information."""

from recidiviz.ingest.direct.regions.us_ar.ingest_views.us_ar_view_query_fragments import (
    SUPERVISION_SENTENCE_LENGTHS_FRAGMENT,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH
/* 
Sentence length entities for supervision sentences are ingested using the SUPVTIMELINE table,
which functions similarly to the RELEASEDATECHANGE table, just for supervision sentences 
rather than prison sentences. For each sentence component consisting of or including a supervision
term, as soon as the supervision required by that component begins, a record is created in
SUPVTIMELINE with the supervision sentence's details. Sentences are identified using the
same columns as SENTENCECOMPONENT (OFFENDERID, COMMITMENTPREFIX, and SENTENCECOMPONENT),
and the start date of the sentence is given by SUPVPERIODBEGINDATE. This is usually but not
always the same as SUPVSTARTDATE, which is the day someone actually begins being actively 
supervised for that sentence. Over the course of someone's supervision, new rows can be
added, reflecting updated information about the supervision sentence. SUPVPERIODBEGINDATE
will always indicate the date that the row's information became true, whereas SUPVSTARTDATE
will indicate the date that someone started being actively supervised under that supervision
period. So, if someone is sentenced to supervision and starts being supervised on date 1,
then is jailed for a supervision sanction on date 2, and resumes supervision with a revised
discharge date on date 3, they would have a SUPVTIMELINE entry with SUPVPERIODBEGINDATE and
SUPVSTARTDATE = 1, along with an entry with SUPVPERIODBEGINDATE and SUPVSTARTDATE = 3. For
a person with the same progression, but who does NOT have their discharge date revised following
the sanction, they would have a SUPVTIMELINE entry with SUPVPERIODBEGINDATE and 
SUPVSTARTDATE = 1 and an entry with SUPVPERIODBEGINDATE = 1 and SUPVSTARTDATE = 3.

When using this table to construct sentence lengths, we have to deal with components containing
both probation and parole time. This view query takes SUPVTIMELINE and breaks hybrid sentences
into separate rows, then constructs a historical ledger of sentence lenghts using SUPVPERIODBEGINDATE
as the update date for each entry.
*/
{SUPERVISION_SENTENCE_LENGTHS_FRAGMENT}

SELECT * 
FROM sentence_lengths_pp
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="supervision_sentence_length",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
