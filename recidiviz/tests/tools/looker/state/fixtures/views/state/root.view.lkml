# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/state/state_dataset_view_generator.py`.

view: root {
  sql_table_name: state.root ;;

  dimension: root_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.root_id ;;
  }

  dimension: root_type {
    type: string
    sql: ${TABLE}.root_type ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
