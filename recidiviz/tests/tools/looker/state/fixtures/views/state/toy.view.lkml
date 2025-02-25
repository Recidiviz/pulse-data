# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/state/state_dataset_view_generator.py`.

view: toy {
  sql_table_name: state.toy ;;

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: toy_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.toy_id ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
