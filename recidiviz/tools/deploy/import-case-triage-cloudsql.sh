#!/usr/bin/env bash

# TODO(#5163) Remove once #5160 is merged and this test has been run against staging

PROJECT_ID=$1
GIT_HASH=$2
ACTION=$3

if [ -z ${PROJECT_ID:+x} ]; then
  echo "PROJECT_ID must be passed as first argument"
  exit 1
fi

if [ -z ${GIT_HASH:+x} ]; then
  echo "GIT_HASH must be passed as second argument"
  exit 1
fi

if [ -z ${ACTION:+x} ]; then
  echo "ACTION must be passed as third argument"
  exit 1
fi

case "$PROJECT_ID" in
	"recidiviz-staging") PREFIX="dev" ;;
	*) PREFIX="prod" ;;
esac

function tf-import {
	RESOURCE=$1
	ADDRESS=$2
	terraform import -config=recidiviz/tools/deploy/terraform -var="project_id=$PROJECT_ID" -var="git_hash=$GIT_HASH" $RESOURCE $ADDRESS
}

CASE_TRIAGE_SUFFIX='0af0a'

# Case Triage staging re-import validation:
if [[ $ACTION == "import" ]]; then
  tf-import module.case_triage_database.google_sql_database_instance.data $PREFIX-case-triage-$CASE_TRIAGE_SUFFIX
  tf-import module.case_triage_database.google_sql_user.postgres $PROJECT_ID/$PREFIX-case-triage-$CASE_TRIAGE_SUFFIX/postgres
  tf-import module.case_triage_database.google_sql_user.readonly $PROJECT_ID/$PREFIX-case-triage-$CASE_TRIAGE_SUFFIX/readonly
  # Certificates have arbitrary identifiers created by Cloud SQL. Fetch resource URI once it has been created
  # tf-import module.case_triage_database.google_sql_ssl_cert.client_cert INSERT_URL_HERE
  tf-import module.case_triage_database.google_secret_manager_secret.secret_client_key https://secretmanager.googleapis.com/v1/projects/984160736970/secrets/case_triage_db_client_key
  tf-import module.case_triage_database.google_secret_manager_secret_version.secret_version_client_key projects/984160736970/secrets/case_triage_db_client_key/versions/1
  tf-import module.case_triage_database.google_secret_manager_secret.secret_client_cert https://secretmanager.googleapis.com/v1/projects/984160736970/secrets/case_triage_db_client_cert
  tf-import module.case_triage_database.google_secret_manager_secret_version.secret_version_client_cert projects/984160736970/secrets/case_triage_db_client_cert/versions/1
  tf-import module.case_triage_database.google_secret_manager_secret.secret_server_cert https://secretmanager.googleapis.com/v1/projects/984160736970/secrets/case_triage_db_server_cert
  tf-import module.case_triage_database.google_secret_manager_secret_version.secret_version_server_cert projects/984160736970/secrets/case_triage_db_server_cert/versions/1
elif [[ $ACTION == "remove" ]]; then
  terraform state rm module.case_triage_database.google_sql_database_instance.data
  terraform state rm module.case_triage_database.google_sql_user.postgres
  terraform state rm module.case_triage_database.google_sql_user.readonly
  terraform state rm module.case_triage_database.google_sql_ssl_cert.client_cert
  terraform state rm module.case_triage_database.google_secret_manager_secret.secret_client_key
  terraform state rm module.case_triage_database.google_secret_manager_secret_version.secret_version_client_key
  terraform state rm module.case_triage_database.google_secret_manager_secret.secret_client_cert
  terraform state rm module.case_triage_database.google_secret_manager_secret_version.secret_version_client_cert
  terraform state rm module.case_triage_database.google_secret_manager_secret.secret_server_cert
  terraform state rm module.case_triage_database.google_secret_manager_secret_version.secret_version_server_cert
else
  echo "Invalid ACTION provided as third argument"
  exit 1
fi

