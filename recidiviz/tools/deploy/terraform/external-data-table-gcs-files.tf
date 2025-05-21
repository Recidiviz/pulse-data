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

module "county_resident_adult_populations_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "county_resident_adult_populations.csv"
}

module "county_resident_populations_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "county_resident_populations.csv"
}

module "county_fips_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "county_fips.csv"
}

module "us_nd_incarceration_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_nd_incarceration_facility_names.csv"
}

module "us_me_incarceration_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_me_incarceration_facility_names.csv"
}

module "us_tn_supervision_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_tn_supervision_facility_names.csv"
}

module "us_id_supervision_unit_to_district_map_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_id_supervision_unit_to_district_map.csv"
}

module "us_id_supervision_district_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_id_supervision_district_names.csv"
}

module "us_id_incarceration_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_id_incarceration_facility_names.csv"
}

module "us_id_incarceration_facility_map_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_id_incarceration_facility_map.csv"
}

module "us_tn_incarceration_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_tn_incarceration_facility_names.csv"
}

module "us_tn_incarceration_facility_map_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_tn_incarceration_facility_map.csv"
}

module "us_mi_incarceration_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_mi_incarceration_facility_names.csv"
}

module "us_co_incarceration_facility_names_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_co_incarceration_facility_names.csv"
}

module "us_co_incarceration_facility_map_table" {
  source = "./modules/local-csv-backed-gcs-file"

  project_id     = var.project_id
  bucket_name    = module.external_reference_tables_bucket.name
  recidiviz_root = local.recidiviz_root

  file_name = "us_co_incarceration_facility_map.csv"
}

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
