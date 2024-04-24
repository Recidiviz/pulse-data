#!/bin/bash

# TODO(#27373): This script can be deleted once the calc DAG includes ingest
# and supports end to end sandbox runs.

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"

usage() {
	cat <<EOF
Usage: bash recidiviz/tools/ingest/development/full_sandbox_pipeline.sh [-options] [--arguments]

This script will run the following steps for the specified state:
  1. create_or_update_dataflow_sandbox
  2. run_sandbox_ingest_pipeline
  3. run_sandbox_calculation_pipeline (normalization)
  4. run_sandbox_calculation_pipeline (selected pipelines and metrics)

Options:
  -h, --help                  Show this help message and exit

Arguments:
  --project_id                The project ID to use (required)
                                [recidiviz-staging, recidiviz-123]
  --sandbox_prefix            The sandbox prefix to use (required)
                                Lowercase, no spaces, no special characters.
  --state_code                The state code to use (required)
                                [US_CA, US_TX, US_NY, ...]

The below arguments can be used to specify which pipelines and metrics to run. You must
specify at least one pipeline to run. If you specify a pipeline, you must also specify
the specific metrics to run for that pipeline. Pipelines are specified as arguments,
followed by the metrics for that pipeline in a comma separated list (NO SPACES). See
examples below for more information.

Pipeline Arguments:
  --incarceration_metrics     Available metrics:
                              - INCARCERATION_ADMISSION
                              - INCARCERATION_COMMITMENT_FROM_SUPERVISION
                              - INCARCERATION_RELEASE
  --supervision_metrics       Available metrics:
                              - SUPERVISION_COMPLIANCE
                              - SUPERVISION_POPULATION
                              - SUPERVISION_OUT_OF_STATE_POPULATION
                              - SUPERVISION_START
                              - SUPERVISION_SUCCESS
                              - SUPERVISION_TERMINATION
  --population_span_metrics   Available metrics:
                              - INCARCERATION_POPULATION_SPAN
                              - SUPERVISION_POPULATION_SPAN
  --violation_metrics         Available metrics:
                              - VIOLATION
  --program_metrics           Available metrics:
                              - PROGRAM_PARTICIPATION
  --recidivism_metrics        Available metrics:
                              - REINCARCERATION_RATE

Examples:
  # Run population span metrics for California
  bash recidiviz/tools/ingest/development/full_sandbox_pipeline.sh \\
    --project_id recidiviz-staging --sandbox_prefix jkgerig --state_code US_CA \\
    --population_span_metrics "INCARCERATION_POPULATION_SPAN,SUPERVISION_POPULATION_SPAN"
  
  # Run all metrics for Idaho
  bash recidiviz/tools/ingest/development/full_sandbox_pipeline.sh \\
    --project_id recidiviz-staging --sandbox_prefix zbrenda --state_code US_ID \\
    --incarceration_metrics "INCARCERATION_ADMISSION,INCARCERATION_COMMITMENT_FROM_SUPERVISION,INCARCERATION_RELEASE" \\
    --supervision_metrics "SUPERVISION_COMPLIANCE,SUPERVISION_POPULATION,SUPERVISION_OUT_OF_STATE_POPULATION,SUPERVISION_START,SUPERVISION_SUCCESS,SUPERVISION_TERMINATION" \\
    --population_span_metrics "INCARCERATION_POPULATION_SPAN,SUPERVISION_POPULATION_SPAN" \\
    --violation_metrics "VIOLATION" \\
    --program_metrics "PROGRAM_PARTICIPATION" \\
    --recidivism_metrics "REINCARCERATION_RATE"
EOF
}

# Helper function to check pipeline args
check_pipeline_arg() {
	local pipeline_name="$1"
	local metric_types="$2"

	if [[ -z "${metric_types:-}" ]]; then
		echo_error "Error: No metrics specified for $pipeline_name pipeline"
		usage
		exit 1
	elif [[ ! "$metric_types" =~ ^[A-Z_,]+$ ]]; then
		echo_error "Error: Invalid metrics specified for $pipeline_name pipeline"
		usage
		exit 1
	fi
}

# Parse command line arguments
declare -A PIPELINES

while (("$#")); do
	case "$1" in
	-h | --help)
		usage
		exit 0
		;;
	--project_id)
		PROJECT_ID="$2"
		shift 2
		;;
	--sandbox_prefix)
		SANDBOX_PREFIX="$2"
		shift 2
		;;
	--state_code)
		STATE_CODE="$2"
		shift 2
		;;

	--incarceration_metrics)
		check_pipeline_arg "incarceration_metrics" "${2:-}"
		PIPELINES["INCARCERATION_METRICS"]="$2"
		shift 2
		;;

	--supervision_metrics)
		check_pipeline_arg "supervision_metrics" "${2:-}"
		PIPELINES["SUPERVISION_METRICS"]="$2"
		shift 2
		;;

	--population_span_metrics)
		check_pipeline_arg "population_span_metrics" "${2:-}"
		PIPELINES["POPULATION_SPAN_METRICS"]="$2"
		shift 2
		;;

	--violation_metrics)
		check_pipeline_arg "violation_metrics" "${2:-}"
		PIPELINES["VIOLATION_METRICS"]="$2"
		shift 2
		;;

	--program_metrics)
		check_pipeline_arg "program_metrics" "${2:-}"
		PIPELINES["PROGRAM_METRICS"]="$2"
		shift 2
		;;

	--recidivism_metrics)
		check_pipeline_arg "recidivism_metrics" "${2:-}"
		PIPELINES["RECIDIVISM_METRICS"]="$2"
		shift 2
		;;

	*)
		echo_error "Error: Invalid option"
		usage
		exit 1
		;;
	esac
done

# Check required arguments
if [ -z "${PROJECT_ID:-}" ] || [ -z "${SANDBOX_PREFIX:-}" ] || [ -z "${STATE_CODE:-}" ]; then
	echo_error "Missing required arguments"
	usage
	exit 1
fi

# Check for at least one pipeline to run
if [ -v PIPELINES ] && [ ${#PIPELINES[@]} -eq 0 ]; then
	echo_error "At least one pipeline must be specified"
	usage
	exit 1
fi

# Function to pause and wait for user confirmation before continuing
function confirm_cmd {
	cmd="$*"

	echo -e "About to run command: $cmd"

	while true; do

		read -p "[(P)roceed, (S)kip, or (E)xit]: " -r

		if [[ $REPLY =~ ^[Ee]$ ]]; then
			echo "Exiting."
			exit 1
		elif [[ $REPLY =~ ^[Pp]$ ]]; then
			echo "Proceeding with command."
			run_cmd "$@"
			break
		elif [[ $REPLY =~ ^[Ss]$ ]]; then
			echo "Skipping command."
			break
		else
			echo "Invalid input (${REPLY})."
		fi
	done
}

# Some more variables
STATE_CODE_LOWERCASE=${STATE_CODE,,}                 # convert to lowercase

STANDARD_STATE_SPECIFIC_STATE_DATASET="${STATE_CODE_LOWERCASE}_state_primary"
SANDBOX_STATE_DATASET="${SANDBOX_PREFIX}_${STANDARD_STATE_SPECIFIC_STATE_DATASET}"
SANDBOX_STATE_SPECIFIC_NORMALIZED_DATASET="${SANDBOX_PREFIX}_${STATE_CODE_LOWERCASE}_normalized_state"
STANDARD_NORMALIZED_STATE_DATASET="normalized_state"
SANDBOX_NORMALIZED_DATASET="${SANDBOX_PREFIX}_${STANDARD_NORMALIZED_STATE_DATASET}"

# Create datasets
echo "Ready to create sandbox normalization dataset for ${STATE_CODE}."
confirm_cmd python -m recidiviz.tools.calculator.create_or_update_dataflow_sandbox \
	--project_id "${PROJECT_ID}" \
	--sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
	--state_code "${STATE_CODE}" \
	--allow_overwrite \
	--datasets_to_create "normalization"

echo "If applicable, you should confirm that the dataset was created before proceeding."

echo "Ready to create sandbox metrics dataset for ${STATE_CODE}."
confirm_cmd python -m recidiviz.tools.calculator.create_or_update_dataflow_sandbox \
	--project_id "${PROJECT_ID}" \
	--sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
	--state_code "${STATE_CODE}" \
	--allow_overwrite \
	--datasets_to_create "metrics"

echo "If applicable, you should confirm that the dataset was created before proceeding."

# Ingest pipeline
echo "Ready to run sandbox ingest pipeline for ${STATE_CODE}."
confirm_cmd python -m recidiviz.tools.ingest.development.run_sandbox_ingest_pipeline \
	--project "${PROJECT_ID}" \
	--state_code "${STATE_CODE}" \
	--output_sandbox_prefix "${SANDBOX_PREFIX}"

echo "If applicable, you should confirm that the ingest pipeline is complete before proceeding."

# Normalization pipeline
echo "Ready to run sandbox normalization pipeline for ${STATE_CODE}."
confirm_cmd python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
	--pipeline comprehensive_normalization \
	--type normalization \
	--project "${PROJECT_ID}" \
	--output_sandbox_prefix "${SANDBOX_PREFIX}" \
	--state_code "${STATE_CODE}" \
	--input_dataset_overrides_json "'{\"${STANDARD_STATE_SPECIFIC_STATE_DATASET}\": \"${SANDBOX_STATE_DATASET}\"}'"

echo "If applicable, you should confirm that the normalization pipeline is complete before proceeding."

echo "Ready to load data from ${SANDBOX_STATE_DATASET} and ${SANDBOX_STATE_SPECIFIC_NORMALIZED_DATASET} into the unified ${SANDBOX_NORMALIZED_DATASET} dataset."
confirm_cmd python -m recidiviz.tools.calculator.update_sandbox_normalized_state_dataset \
  --project_id "${PROJECT_ID}" \
  --state_code "${STATE_CODE}" \
  --input_state_dataset "${SANDBOX_STATE_DATASET}" \
  --input_normalized_state_dataset "${SANDBOX_STATE_SPECIFIC_NORMALIZED_DATASET}" \
  --output_sandbox_prefix "${SANDBOX_PREFIX}"

# Metric pipelines
for pipeline in "${!PIPELINES[@]}"; do
	pipeline_lowercase=${pipeline,,}
	echo "Ready to run sandbox metrics pipeline for ${STATE_CODE} for pipeline ${pipeline_lowercase}."
	IFS=',' read -r -a metric_types <<<"${PIPELINES[$pipeline]}"
	confirm_cmd python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
		--pipeline "${pipeline_lowercase}" \
		--type metrics \
		--project "${PROJECT_ID}" \
		--output_sandbox_prefix "${SANDBOX_PREFIX}" \
		--state_code "${STATE_CODE}" \
		--input_dataset_overrides_json "'{\"${STANDARD_NORMALIZED_STATE_DATASET}\": \"${SANDBOX_NORMALIZED_DATASET}\"}'" \
		--metric_types "${metric_types[*]}"
done

echo "All pipelines started successfully for ${STATE_CODE}!"
