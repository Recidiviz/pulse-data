#!/usr/bin/env bash

project=$1
bucket=$2
pipeline=$3
job_name=$4
input=$5
reference_input=$6
output=$7

# Optional arguments
metric_type=$8
state_code=$9

if [ x"$project" == x -o x"$bucket" == x -o x"$pipeline" == x -o x"$job_name" == x -o x"$input" == x -o x"$output" == x -o x"$reference_input" == x ]; then
    echo "usage: $0 <project> <bucket> <pipeline> <job_name> <input> <output>"
    exit 1
fi

function run_cmd {
    cmd="$1"
    echo "Running \`$cmd\`"
    $cmd

    ret_code=$?
    if [ $ret_code -ne 0 ]; then
        echo "Failed with exit code $ret_code"
        exit $ret_code
    fi
}

# We hard-code to n1-standard-4 below because we don't want low-level config
# like worker provisioning to be a high-level parameter for configuration
echo "Deploying $pipeline pipeline to template"
command="python -m recidiviz.tools.run_calculation_pipelines
    --pipeline $pipeline
    --job_name $job_name
    --runner DataflowRunner
    --project $project
    --setup_file ./setup.py
    --staging_location gs://$bucket/staging
    --temp_location gs://$bucket/temp
    --template_location gs://$bucket/templates/$job_name
    --worker_machine_type n1-standard-4
    --experiments=shuffle_mode=service
    --region=us-west1
    --input $input
    --reference_input $reference_input
    --output $output
    --methodology=BOTH
    --include_race=True
    --include_age=True
    --include_stay_length=True
    --include_ethnicity=True
    --include_gender=True
    --include_release_facility=True"

# Append optional arguments if they are not empty
[[ -z "$metric_type" ]] || command="$command --metric_type $metric_type"
[[ -z "$state_code" ]]  || command="$command --state_code $state_code"

run_cmd "${command}"
