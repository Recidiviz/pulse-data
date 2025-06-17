# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_or_raw_data_template {
  extension: required

  view_name: us_or_RCDVZ_PRDDTA_OP970P
  view_label: "us_or_RCDVZ_PRDDTA_OP970P"

  description: "Data pertaining to an individual in Oregon"
  group_label: "Raw State Data"
  label: "US_OR Raw Data"
  join: us_or_RCDVZ_CISPRDDTA_CLOVER {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDTA_CLOVER.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDTA_CLOVER"
  }

  join: us_or_RCDVZ_CISPRDDTA_CMCROH {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDTA_CMCROH.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDTA_CMCROH"
  }

  join: us_or_RCDVZ_CISPRDDTA_CMOFFT {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDTA_CMOFFT.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDTA_CMOFFT"
  }

  join: us_or_RCDVZ_CISPRDDTA_CMOFRH {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDTA_CMOFRH.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDTA_CMOFRH"
  }

  join: us_or_RCDVZ_CISPRDDTA_CMSACN {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDTA_CMSACN.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDTA_CMSACN"
  }

  join: us_or_RCDVZ_CISPRDDTA_CMSACO {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDTA_CMSACO.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDTA_CMSACO"
  }

  join: us_or_RCDVZ_CISPRDDTA_CMSAIM {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDTA_CMSAIM.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDTA_CMSAIM"
  }

  join: us_or_RCDVZ_CISPRDDTA_MTOFDR {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDTA_MTOFDR.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDTA_MTOFDR"
  }

  join: us_or_RCDVZ_CISPRDDTA_MTRULE {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDTA_MTRULE.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDTA_MTRULE"
  }

  join: us_or_RCDVZ_CISPRDDTA_MTSANC {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDTA_MTSANC.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDTA_MTSANC"
  }

  join: us_or_RCDVZ_CISPRDDTA_OPCOND {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDTA_OPCOND.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDTA_OPCOND"
  }

  join: us_or_RCDVZ_CISPRDDTA_OPCONE {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDTA_OPCONE.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDTA_OPCONE"
  }

  join: us_or_RCDVZ_CISPRDDT_CLCLHD {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_CISPRDDT_CLCLHD.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_CISPRDDT_CLCLHD"
  }

  join: us_or_RCDVZ_PRDDTA_OP007P {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_PRDDTA_OP007P.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_PRDDTA_OP007P"
  }

  join: us_or_RCDVZ_PRDDTA_OP008P {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_PRDDTA_OP008P.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_PRDDTA_OP008P"
  }

  join: us_or_RCDVZ_PRDDTA_OP009P {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_PRDDTA_OP009P.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_PRDDTA_OP009P"
  }

  join: us_or_RCDVZ_PRDDTA_OP010P {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_PRDDTA_OP010P.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_PRDDTA_OP010P"
  }

  join: us_or_RCDVZ_PRDDTA_OP011P {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_PRDDTA_OP011P.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_PRDDTA_OP011P"
  }

  join: us_or_RCDVZ_PRDDTA_OP013P {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_PRDDTA_OP013P.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_PRDDTA_OP013P"
  }

  join: us_or_RCDVZ_PRDDTA_OP053P {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_PRDDTA_OP053P.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_PRDDTA_OP053P"
  }

  join: us_or_RCDVZ_PRDDTA_OP054P {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_PRDDTA_OP054P.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_PRDDTA_OP054P"
  }

  join: us_or_RCDVZ_PRDDTA_OPCOUR {
    sql_on: ${us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY} = ${us_or_RCDVZ_PRDDTA_OPCOUR.RECORD_KEY};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_or_RCDVZ_PRDDTA_OPCOUR"
  }

}
