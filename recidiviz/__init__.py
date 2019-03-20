"""Top-level recidiviz package."""
import datetime
from typing import Optional

import cattr
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape import ingest_utils
from recidiviz.utils import environment

Session = sessionmaker()
db_engine: Optional[Engine]

# We want to add these globally because the serialization hooks are used in
# ingest and persistence.

cattr.register_unstructure_hook(datetime.datetime,
                                datetime.datetime.isoformat)
cattr.register_structure_hook(
    datetime.datetime,
    lambda serialized, desired_type: desired_type.fromisoformat(serialized))

cattr.register_unstructure_hook(
    IngestInfo, ingest_utils.ingest_info_to_serializable)
cattr.register_structure_hook(
    IngestInfo,
    lambda serializable, desired_type:
        ingest_utils.ingest_info_from_serializable(serializable))
