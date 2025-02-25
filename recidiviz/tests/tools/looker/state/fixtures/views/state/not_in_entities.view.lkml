# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/state/state_dataset_view_generator.py`.

view: not_in_entities {
  sql_table_name: state.not_in_entities ;;

  dimension: field {
    type: string
    sql: ${TABLE}.field ;;
  }

  dimension: not_in_entities_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.not_in_entities_id ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
