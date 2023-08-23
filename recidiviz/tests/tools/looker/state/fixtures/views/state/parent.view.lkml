# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/state/state_dataset_view_generator.py`.

view: parent {
  sql_table_name: state.parent ;;

  dimension: full_name {
    type: string
    sql: ${TABLE}.full_name ;;
  }

  dimension: parent_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.parent_id ;;
  }

  dimension: root_id {
    type: number
    sql: ${TABLE}.root_id ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
