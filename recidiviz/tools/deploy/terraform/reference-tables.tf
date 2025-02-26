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

module "external_reference_tables_bucket" {
  source = "./modules/cloud-storage-bucket"

  project_id  = var.project_id
  name_suffix = "external-reference-data"
}

module "external_reference_dataset" {
  source      = "./modules/big_query_dataset"
  dataset_id  = "external_reference"
  description = "Contains reference tables from external sources that are synced from our repository."
}

module "county_resident_adult_populations_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "county_resident_adult_populations"
  schema     = <<EOF
[
  {
    "name": "fips",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "year",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "population",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }
]
EOF
}

module "county_resident_populations_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "county_resident_populations"
  schema     = <<EOF
[
  {
    "name": "fips",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "year",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "population",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }
]
EOF
}

module "county_fips_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "county_fips"
  schema     = <<EOF
[
  {
    "name": "fips",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "state_code",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "county_code",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "county_name",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
EOF
}

module "us_nd_incarceration_facility_names_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_nd_incarceration_facility_names"
  schema     = <<EOF
[
  {
    "name": "facility_code",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "facility_name",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}

module "us_me_incarceration_facility_names_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_me_incarceration_facility_names"
  schema     = <<EOF
[
  {
    "name": "facility_code",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "facility_name",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}

module "us_tn_supervision_facility_names_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_tn_supervision_facility_names"
  schema     = <<EOF
[
  {
    "name": "facility_code",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "facility_name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "type",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "district",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "division",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}


module "us_tn_supervision_locations_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_tn_supervision_locations"
  schema     = <<EOF
[
  {
    "name": "site_code",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "site_name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "type",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "district",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "division",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}

module "us_id_supervision_unit_to_district_map_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_id_supervision_unit_to_district_map"
  schema     = <<EOF
[
  {
    "name": "level_1_supervision_location_external_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "level_2_supervision_location_external_id",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}

module "us_id_supervision_district_names_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_id_supervision_district_names"
  schema     = <<EOF
[
  {
    "name": "level_2_supervision_location_external_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "level_2_supervision_location_name",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}

module "us_id_incarceration_facility_names_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_id_incarceration_facility_names"
  schema     = <<EOF
[
  {
    "name": "facility_code",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "facility_name",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}

module "us_id_incarceration_facility_map_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_id_incarceration_facility_map"
  schema     = <<EOF
[
  {
    "name": "level_1_incarceration_location_external_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "level_2_incarceration_location_external_id",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}

module "us_tn_incarceration_facility_names_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_tn_incarceration_facility_names"
  schema     = <<EOF
[
  {
    "name": "facility_code",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "facility_name",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}

module "us_tn_incarceration_facility_map_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_tn_incarceration_facility_map"
  schema     = <<EOF
[
  {
    "name": "level_1_incarceration_location_external_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "level_2_incarceration_location_external_id",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}

module "us_mi_incarceration_facility_names_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_mi_incarceration_facility_names"
  schema     = <<EOF
[
  {
    "name": "facility_code",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "facility_name",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}

module "us_co_incarceration_facility_names_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_co_incarceration_facility_names"
  schema     = <<EOF
[
  {
    "name": "facility_code",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "facility_name",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}

module "us_co_incarceration_facility_map_table" {
  source = "./modules/reference-table"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  dataset_id     = module.external_reference_dataset.dataset_id
  recidiviz_root = local.recidiviz_root

  table_name = "us_co_incarceration_facility_map"
  schema     = <<EOF
[
  {
    "name": "level_1_incarceration_location_external_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "level_2_incarceration_location_external_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "level_3_incarceration_location_external_id",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF
}
