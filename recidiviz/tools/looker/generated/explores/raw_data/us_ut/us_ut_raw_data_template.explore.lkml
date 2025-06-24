# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_ut_raw_data_template {
  extension: required

  view_name: us_ut_ofndr
  view_label: "us_ut_ofndr"

  description: "Data pertaining to an individual in Utah"
  group_label: "Raw State Data"
  label: "US_UT Raw Data"
  join: us_ut_cap {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_cap.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_cap"
  }

  join: us_ut_ofndr_addr_arch {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_addr_arch.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_addr_arch"
  }

  join: us_ut_ofndr_agnt {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_agnt.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_agnt"
  }

  join: us_ut_ofndr_cap_actvty {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_cap_actvty.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_cap_actvty"
  }

  join: us_ut_ofndr_dio_prog {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_dio_prog.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_dio_prog"
  }

  join: us_ut_ofndr_dob {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_dob.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_dob"
  }

  join: us_ut_ofndr_email {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_email.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_email"
  }

  join: us_ut_ofndr_lgl_stat {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_lgl_stat.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_lgl_stat"
  }

  join: us_ut_ofndr_loc_hist {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_loc_hist.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_loc_hist"
  }

  join: us_ut_ofndr_name {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_name.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_name"
  }

  join: us_ut_ofndr_norm {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_norm.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_norm"
  }

  join: us_ut_ofndr_oth_num {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_oth_num.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_oth_num"
  }

  join: us_ut_ofndr_phone {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_phone.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_phone"
  }

  join: us_ut_ofndr_prgrmng {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_prgrmng.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_prgrmng"
  }

  join: us_ut_ofndr_sec_assess {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_sec_assess.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_sec_assess"
  }

  join: us_ut_ofndr_spcl_sprvsn {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_spcl_sprvsn.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_spcl_sprvsn"
  }

  join: us_ut_ofndr_sprvsn {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_sprvsn.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_sprvsn"
  }

  join: us_ut_ofndr_tst {
    sql_on: ${us_ut_ofndr.ofndr_num} = ${us_ut_ofndr_tst.ofndr_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ut_ofndr_tst"
  }

}
