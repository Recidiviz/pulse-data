# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/state/state_dataset_view_generator.py`.

view: child {
  sql_table_name: state.child ;;

  dimension: child_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.child_id ;;
  }

  dimension: favorite_toy_id {
    type: number
    sql: ${TABLE}.favorite_toy_id ;;
  }

  dimension: full_name {
    type: string
    sql: ${TABLE}.full_name ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
