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

# TODO(#46196): Delete this table and it's source table YAML definition and replace usages with the new view in
#  static_reference_data_views
module "county_fips_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "county_fips.csv"
}

# TODO(#46196): Delete this table and it's source table YAML definition and replace usages with the new view in
#  static_reference_data_views
module "us_nd_incarceration_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_nd_incarceration_facility_names.csv"
}

# TODO(#46196): Delete this table and it's source table YAML definition and replace usages with the new view in
#  static_reference_data_views
module "us_me_incarceration_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_me_incarceration_facility_names.csv"
}

# TODO(#46196): Delete this table and it's source table YAML definition and replace usages with the new view in
#  static_reference_data_views
module "us_tn_supervision_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_tn_supervision_facility_names.csv"
}

# TODO(#46196): Delete this table and it's source table YAML definition and replace usages with the new view in
#  static_reference_data_views
module "us_tn_incarceration_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_tn_incarceration_facility_names.csv"
}

# TODO(#46196): Delete this table and it's source table YAML definition and replace usages with the new view in
#  static_reference_data_views
module "us_tn_incarceration_facility_map_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_tn_incarceration_facility_map.csv"
}

# TODO(#46196): Delete this table and it's source table YAML definition and replace usages with the new view in
#  static_reference_data_views
module "us_mi_incarceration_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_mi_incarceration_facility_names.csv"
}

# TODO(#46196): Delete this table and it's source table YAML definition and replace usages with the new view in
#  static_reference_data_views
module "us_co_incarceration_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_co_incarceration_facility_names.csv"
}

# TODO(#46196): Delete this table and it's source table YAML definition and replace usages with the new view in
#  static_reference_data_views
module "us_co_incarceration_facility_map_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_co_incarceration_facility_map.csv"
}

# TODO(#46196): Delete this table and it's source table YAML definition and replace usages with the new view in
#  static_reference_data_views
module "us_mo_incarceration_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_mo_incarceration_facility_names.csv"

}

module "state_resident_populations_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "state_resident_populations.csv"
}
