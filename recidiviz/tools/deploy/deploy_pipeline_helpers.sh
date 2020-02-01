#!/usr/bin/env bash

function run_cmd {
    command="$1"
    echo "Running \`command\`"
    $command

    ret_code=$?
    if [ $ret_code -ne 0 ]; then
        echo "Failed with exit code $ret_code"
        exit $ret_code
    fi
}

function check_not_empty {
    # Check if variable is empty and exit if it is
    [ -z "$1" ] && echo "Variable is unset" && exit 1
}

function deploy_pipelines {
  bash_source_dir=$(dirname "$BASH_SOURCE")

  project=$1
  bucket=$2
  file=$3

  pipeline_count="$(cat $file | pipenv run yq -r .pipeline_count)"
  check_not_empty pipeline_count

  END=$pipeline_count
  ((END = END - 1))
  i=0
  # Deploy each pipeline in the file to a template
  while [[ $i -le $END ]]
  do
      pipeline="$(cat $file | pipenv run yq -r .pipelines[$i].pipeline)"
      check_not_empty pipeline
      job_name="$(cat $file | pipenv run yq -r .pipelines[$i].job_name)"
      check_not_empty job_name
      input="$(cat $file | pipenv run yq -r .pipelines[$i].input)"
      check_not_empty input
      reference_input="$(cat $file | pipenv run yq -r .pipelines[$i].reference_input)"
      check_not_empty reference_input
      output="$(cat $file | pipenv run yq -r .pipelines[$i].output)"
      check_not_empty output

      metric_type="$(cat $file | pipenv run yq -r .pipelines[$i].metric_type)"
      state_code="$(cat $file | pipenv run yq -r .pipelines[$i].state_code)"

      base_command="pipenv run ${bash_source_dir}/deploy_pipeline_to_template.sh $project $bucket $pipeline $job_name $input $reference_input $output"

      [[ ${metric_type} == "null" ]] || base_command="$base_command $metric_type"
      [[ ${state_code} == "null" ]] || base_command="$base_command $state_code"

      run_cmd "${base_command}"
      ((i = i + 1))
  done
}


function deploy_pipeline_templates_to_staging {
    bash_source_dir=$(dirname "$BASH_SOURCE")
    pipeline_dir=${bash_source_dir}/./../../calculator/pipeline

    echo "Deploying stage-only calculation pipelines to templates in recidiviz-staging."
    deploy_pipelines recidiviz-staging recidiviz-staging-dataflow-templates ${pipeline_dir}/staging_only_calculation_pipeline_templates.yaml


    echo "Deploying prod-ready calculation pipelines to templates in recidiviz-staging."
    deploy_pipelines recidiviz-staging recidiviz-staging-dataflow-templates ${pipeline_dir}/prod_calculation_pipeline_templates.yaml
}

function deploy_pipeline_templates_to_prod {
    bash_source_dir=$(dirname "$BASH_SOURCE")
    pipeline_dir=${bash_source_dir}/./../../calculator/pipeline

    echo "Deploying prod-ready calculation pipelines to templates in recidiviz-123."
    deploy_pipelines recidiviz-123 recidiviz-123-dataflow-templates ${pipeline_dir}/prod_calculation_pipeline_templates.yaml
}
