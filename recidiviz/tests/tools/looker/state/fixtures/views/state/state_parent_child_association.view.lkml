# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/state/state_dataset_view_generator.py`.

view: state_parent_child_association {
  sql_table_name: state.state_parent_child_association ;;

  dimension: child_id {
    type: number
    sql: ${TABLE}.child_id ;;
  }

  dimension: parent_id {
    type: number
    sql: ${TABLE}.parent_id ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
