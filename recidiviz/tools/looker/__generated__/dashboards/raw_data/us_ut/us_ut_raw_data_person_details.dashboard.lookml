# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_ut_raw_data_person_details
  title: Utah Raw Data Person Details
  description: For examining individuals in US_UT's raw data tables
  layout: newspaper
  load_configuration: wait

  filters:
  - name: View Type
    title: View Type
    type: field_filter
    default_value: raw^_data^_up^_to^_date^_views
    allow_multiple_values: false
    required: true
    ui_config: 
      type: dropdown_menu
      display: inline
    model: "@{project_id}"
    explore: us_ut_raw_data
    field: us_ut_ofndr.view_type

  - name: US_UT_DOC
    title: US_UT_DOC
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{project_id}"
    explore: us_ut_raw_data
    field: us_ut_ofndr.ofndr_num

  elements:
  - name: ofndr
    title: ofndr
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr.primary_key,
      us_ut_ofndr.ofndr_num,
      us_ut_ofndr.eye_color_cd,
      us_ut_ofndr.hair_color_cd,
      us_ut_ofndr.mrtl_stat_cd,
      us_ut_ofndr.race_cd,
      us_ut_ofndr.rlgn_cd,
      us_ut_ofndr.sex,
      us_ut_ofndr.ht_ft,
      us_ut_ofndr.ht_in,
      us_ut_ofndr.wt_lb,
      us_ut_ofndr.ethnic_cd_ncic,
      us_ut_ofndr.immgrtn_stat,
      us_ut_ofndr.cmt_rec_flg,
      us_ut_ofndr.lgl_juris_typ,
      us_ut_ofndr.czn_ctry_cd,
      us_ut_ofndr.ofndr_photo,
      us_ut_ofndr.escape_flg,
      us_ut_ofndr.abscond_flg,
      us_ut_ofndr.juv_inst_flg,
      us_ut_ofndr.juris_st_cd,
      us_ut_ofndr.mntl_inst_flg,
      us_ut_ofndr.utah_sid,
      us_ut_ofndr.updt_usr_id,
      us_ut_ofndr.updt_dt__raw,
      us_ut_ofndr.file_id,
      us_ut_ofndr.is_deleted]
    sorts: [us_ut_ofndr.updt_dt__raw]
    note_display: hover
    note_text: "Offender"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 0
    col: 0
    width: 24
    height: 6

  - name: cap
    title: cap
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_cap.primary_key,
      us_ut_cap.cap_id,
      us_ut_cap.cap_not_reqd_cd,
      us_ut_cap.ofndr_num,
      us_ut_cap.init_cap_dt__raw,
      us_ut_cap.cap_mgr,
      us_ut_cap.end_dt__raw,
      us_ut_cap.mstr_updt_usr_id,
      us_ut_cap.mstr_updt_dt__raw,
      us_ut_cap.updt_usr_id,
      us_ut_cap.updt_dt__raw,
      us_ut_cap.file_id,
      us_ut_cap.is_deleted]
    sorts: [us_ut_cap.init_cap_dt__raw]
    note_display: hover
    note_text: "Base table for Case Action Plan (CAP)"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 6
    col: 0
    width: 24
    height: 6

  - name: ofndr_addr_arch
    title: ofndr_addr_arch
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_addr_arch.primary_key,
      us_ut_ofndr_addr_arch.ofndr_addr_id,
      us_ut_ofndr_addr_arch.ofndr_num,
      us_ut_ofndr_addr_arch.addr_typ_cd,
      us_ut_ofndr_addr_arch.orig_addr_typ_cd,
      us_ut_ofndr_addr_arch.addr_strt_dt__raw,
      us_ut_ofndr_addr_arch.addr_end_dt__raw,
      us_ut_ofndr_addr_arch.pssa_unit_cd,
      us_ut_ofndr_addr_arch.pssa_range,
      us_ut_ofndr_addr_arch.psda_num,
      us_ut_ofndr_addr_arch.psda_pre_dir,
      us_ut_ofndr_addr_arch.psda_street,
      us_ut_ofndr_addr_arch.psda_sfx_cd,
      us_ut_ofndr_addr_arch.psda_post_dir,
      us_ut_ofndr_addr_arch.city,
      us_ut_ofndr_addr_arch.st,
      us_ut_ofndr_addr_arch.zip,
      us_ut_ofndr_addr_arch.ctry_cd,
      us_ut_ofndr_addr_arch.area_cd,
      us_ut_ofndr_addr_arch.ph_num,
      us_ut_ofndr_addr_arch.vld_addr_flg,
      us_ut_ofndr_addr_arch.apt_complex_id,
      us_ut_ofndr_addr_arch.mail_addr_flg,
      us_ut_ofndr_addr_arch.assgn_zone,
      us_ut_ofndr_addr_arch.updt_usr_id,
      us_ut_ofndr_addr_arch.updt_dt__raw,
      us_ut_ofndr_addr_arch.vrfy_dt__raw,
      us_ut_ofndr_addr_arch.vrfy_usr_id,
      us_ut_ofndr_addr_arch.vrfy_rslt_flg,
      us_ut_ofndr_addr_arch.vrfy_mthd_cd,
      us_ut_ofndr_addr_arch.new_addr_id,
      us_ut_ofndr_addr_arch.new_ofndr_addr_hist_id,
      us_ut_ofndr_addr_arch.cnvrt_dt__raw,
      us_ut_ofndr_addr_arch.naive_convert_flg,
      us_ut_ofndr_addr_arch.file_id,
      us_ut_ofndr_addr_arch.is_deleted]
    sorts: [us_ut_ofndr_addr_arch.addr_strt_dt__raw]
    note_display: hover
    note_text: "This table contains address information but only until 2022. We should use ofndr_addr_hist instead."
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 12
    col: 0
    width: 24
    height: 6

  - name: ofndr_agnt
    title: ofndr_agnt
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_agnt.primary_key,
      us_ut_ofndr_agnt.ofndr_num,
      us_ut_ofndr_agnt.agnt_id,
      us_ut_ofndr_agnt.agcy_id,
      us_ut_ofndr_agnt.agnt_strt_dt__raw,
      us_ut_ofndr_agnt.end_dt__raw,
      us_ut_ofndr_agnt.usr_typ_cd,
      us_ut_ofndr_agnt.create_usr_id,
      us_ut_ofndr_agnt.create_dt__raw,
      us_ut_ofndr_agnt.updt_usr_id,
      us_ut_ofndr_agnt.updt_dt__raw,
      us_ut_ofndr_agnt.file_id,
      us_ut_ofndr_agnt.is_deleted]
    sorts: [us_ut_ofndr_agnt.agnt_strt_dt__raw]
    note_display: hover
    note_text: "Offender Agnt"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 18
    col: 0
    width: 24
    height: 6

  - name: ofndr_cap_actvty
    title: ofndr_cap_actvty
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_cap_actvty.primary_key,
      us_ut_ofndr_cap_actvty.ofndr_cap_actvty_id,
      us_ut_ofndr_cap_actvty.ofndr_num,
      us_ut_ofndr_cap_actvty.ofndr_cap_goal_actn_stp_id,
      us_ut_ofndr_cap_actvty.cap_actvty_id,
      us_ut_ofndr_cap_actvty.cstm_actvty_name,
      us_ut_ofndr_cap_actvty.cstm_actvty_desc,
      us_ut_ofndr_cap_actvty.trgt_strt_dt__raw,
      us_ut_ofndr_cap_actvty.trgt_end_dt__raw,
      us_ut_ofndr_cap_actvty.strt_dt__raw,
      us_ut_ofndr_cap_actvty.end_dt__raw,
      us_ut_ofndr_cap_actvty.omp_exit_typ_cd,
      us_ut_ofndr_cap_actvty.trgt_dosage,
      us_ut_ofndr_cap_actvty.actual_dosage,
      us_ut_ofndr_cap_actvty.create_usr_id,
      us_ut_ofndr_cap_actvty.create_dt__raw,
      us_ut_ofndr_cap_actvty.updt_usr_id,
      us_ut_ofndr_cap_actvty.updt_dt__raw,
      us_ut_ofndr_cap_actvty.file_id,
      us_ut_ofndr_cap_actvty.is_deleted]
    sorts: [us_ut_ofndr_cap_actvty.trgt_strt_dt__raw]
    note_display: hover
    note_text: "Contains activities relating to each CAP in the base table. There is a many to one relationship between activities relating to a CAP, contained in this table, and CAPs themselves, contained in the base `cap` table."
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 24
    col: 0
    width: 24
    height: 6

  - name: ofndr_dio_prog
    title: ofndr_dio_prog
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_dio_prog.primary_key,
      us_ut_ofndr_dio_prog.ofndr_num,
      us_ut_ofndr_dio_prog.prvdr_id,
      us_ut_ofndr_dio_prog.dio_prog_id,
      us_ut_ofndr_dio_prog.prog_rfrl_dt__raw,
      us_ut_ofndr_dio_prog.prog_strt_dt__raw,
      us_ut_ofndr_dio_prog.exit_typ_cd,
      us_ut_ofndr_dio_prog.exit_dt__raw,
      us_ut_ofndr_dio_prog.updt_usr_id,
      us_ut_ofndr_dio_prog.updt_dt__raw,
      us_ut_ofndr_dio_prog.file_id,
      us_ut_ofndr_dio_prog.is_deleted]
    sorts: [us_ut_ofndr_dio_prog.prog_rfrl_dt__raw]
    note_display: hover
    note_text: "Offender Division Of Institutional Operations Program"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 30
    col: 0
    width: 24
    height: 6

  - name: ofndr_dob
    title: ofndr_dob
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_dob.primary_key,
      us_ut_ofndr_dob.ofndr_dob_id,
      us_ut_ofndr_dob.ofndr_num,
      us_ut_ofndr_dob.dob,
      us_ut_ofndr_dob.brth_st,
      us_ut_ofndr_dob.brth_ctry_cd,
      us_ut_ofndr_dob.brth_city,
      us_ut_ofndr_dob.updt_usr_id,
      us_ut_ofndr_dob.updt_dt__raw,
      us_ut_ofndr_dob.temp_name_id,
      us_ut_ofndr_dob.file_id,
      us_ut_ofndr_dob.is_deleted]
    sorts: [us_ut_ofndr_dob.updt_dt__raw]
    note_display: hover
    note_text: "Offender Date Of Birth"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 36
    col: 0
    width: 24
    height: 6

  - name: ofndr_email
    title: ofndr_email
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_email.primary_key,
      us_ut_ofndr_email.id,
      us_ut_ofndr_email.ofndr_num,
      us_ut_ofndr_email.email_addr,
      us_ut_ofndr_email.email_typ_cd,
      us_ut_ofndr_email.strt_dt__raw,
      us_ut_ofndr_email.end_dt__raw,
      us_ut_ofndr_email.updt_usr_id,
      us_ut_ofndr_email.updt_dt__raw,
      us_ut_ofndr_email.file_id,
      us_ut_ofndr_email.is_deleted]
    sorts: [us_ut_ofndr_email.strt_dt__raw]
    note_display: hover
    note_text: "Offender Email"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 42
    col: 0
    width: 24
    height: 6

  - name: ofndr_lgl_stat
    title: ofndr_lgl_stat
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_lgl_stat.primary_key,
      us_ut_ofndr_lgl_stat.ofndr_num,
      us_ut_ofndr_lgl_stat.lgl_stat_cd,
      us_ut_ofndr_lgl_stat.stat_beg_dt__raw,
      us_ut_ofndr_lgl_stat.stat_beg_tm,
      us_ut_ofndr_lgl_stat.stat_end_dt__raw,
      us_ut_ofndr_lgl_stat.lgl_stat_chg_cd,
      us_ut_ofndr_lgl_stat.prev_lgl_stat_cd,
      us_ut_ofndr_lgl_stat.updt_usr_id,
      us_ut_ofndr_lgl_stat.updt_dt__raw,
      us_ut_ofndr_lgl_stat.file_id,
      us_ut_ofndr_lgl_stat.is_deleted]
    sorts: [us_ut_ofndr_lgl_stat.stat_beg_dt__raw]
    note_display: hover
    note_text: "Offender Legal Stat"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 48
    col: 0
    width: 24
    height: 6

  - name: ofndr_loc_hist
    title: ofndr_loc_hist
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_loc_hist.primary_key,
      us_ut_ofndr_loc_hist.ofndr_num,
      us_ut_ofndr_loc_hist.body_loc_cd,
      us_ut_ofndr_loc_hist.assgn_dt__raw,
      us_ut_ofndr_loc_hist.assgn_tm,
      us_ut_ofndr_loc_hist.assgn_rsn_cd,
      us_ut_ofndr_loc_hist.end_dt__raw,
      us_ut_ofndr_loc_hist.updt_usr_id,
      us_ut_ofndr_loc_hist.updt_dt__raw,
      us_ut_ofndr_loc_hist.file_id,
      us_ut_ofndr_loc_hist.is_deleted]
    sorts: [us_ut_ofndr_loc_hist.assgn_dt__raw]
    note_display: hover
    note_text: "Offender Location History"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 54
    col: 0
    width: 24
    height: 6

  - name: ofndr_name
    title: ofndr_name
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_name.primary_key,
      us_ut_ofndr_name.ofndr_name_id,
      us_ut_ofndr_name.ofndr_num,
      us_ut_ofndr_name.name_typ_cd,
      us_ut_ofndr_name.lname,
      us_ut_ofndr_name.fname,
      us_ut_ofndr_name.mname,
      us_ut_ofndr_name.sfx_name,
      us_ut_ofndr_name.soundex_cd,
      us_ut_ofndr_name.updt_usr_id,
      us_ut_ofndr_name.updt_dt__raw,
      us_ut_ofndr_name.temp_name_id,
      us_ut_ofndr_name.file_id,
      us_ut_ofndr_name.is_deleted]
    sorts: [us_ut_ofndr_name.updt_dt__raw]
    note_display: hover
    note_text: "Offender Name"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 60
    col: 0
    width: 24
    height: 6

  - name: ofndr_norm
    title: ofndr_norm
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_norm.primary_key,
      us_ut_ofndr_norm.ofndr_num,
      us_ut_ofndr_norm.full_name,
      us_ut_ofndr_norm.addr,
      us_ut_ofndr_norm.city,
      us_ut_ofndr_norm.phone,
      us_ut_ofndr_norm.region_id,
      us_ut_ofndr_norm.assnd_staff,
      us_ut_ofndr_norm.zip,
      us_ut_ofndr_norm.state,
      us_ut_ofndr_norm.ofndr_ssn,
      us_ut_ofndr_norm.dob,
      us_ut_ofndr_norm.ofndr_lgl_stat,
      us_ut_ofndr_norm.body_loc_cd,
      us_ut_ofndr_norm.super_agnt_id,
      us_ut_ofndr_norm.updt_usr_id,
      us_ut_ofndr_norm.updt_dt__raw,
      us_ut_ofndr_norm.fname,
      us_ut_ofndr_norm.lname,
      us_ut_ofndr_norm.file_id,
      us_ut_ofndr_norm.is_deleted]
    sorts: [us_ut_ofndr_norm.updt_dt__raw]
    note_display: hover
    note_text: "Offender Norm"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 66
    col: 0
    width: 24
    height: 6

  - name: ofndr_oth_num
    title: ofndr_oth_num
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_oth_num.primary_key,
      us_ut_ofndr_oth_num.ofndr_oth_num_id,
      us_ut_ofndr_oth_num.ofndr_num,
      us_ut_ofndr_oth_num.num_typ_cd,
      us_ut_ofndr_oth_num.gvnmt_src_cd,
      us_ut_ofndr_oth_num.num_val,
      us_ut_ofndr_oth_num.updt_usr_id,
      us_ut_ofndr_oth_num.updt_dt__raw,
      us_ut_ofndr_oth_num.file_id,
      us_ut_ofndr_oth_num.is_deleted]
    sorts: [us_ut_ofndr_oth_num.updt_dt__raw]
    note_display: hover
    note_text: "Offender Other Num"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 72
    col: 0
    width: 24
    height: 6

  - name: ofndr_phone
    title: ofndr_phone
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_phone.primary_key,
      us_ut_ofndr_phone.id,
      us_ut_ofndr_phone.ofndr_num,
      us_ut_ofndr_phone.area_cd,
      us_ut_ofndr_phone.phone_num,
      us_ut_ofndr_phone.phone_typ_cd,
      us_ut_ofndr_phone.cmt,
      us_ut_ofndr_phone.strt_dt__raw,
      us_ut_ofndr_phone.end_dt__raw,
      us_ut_ofndr_phone.updt_usr_id,
      us_ut_ofndr_phone.updt_dt__raw,
      us_ut_ofndr_phone.file_id,
      us_ut_ofndr_phone.is_deleted]
    sorts: [us_ut_ofndr_phone.strt_dt__raw]
    note_display: hover
    note_text: "Offender Phone"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 78
    col: 0
    width: 24
    height: 6

  - name: ofndr_prgrmng
    title: ofndr_prgrmng
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_prgrmng.primary_key,
      us_ut_ofndr_prgrmng.ofndr_prgrmng_id,
      us_ut_ofndr_prgrmng.ofndr_num,
      us_ut_ofndr_prgrmng.prgm_prvdr_id,
      us_ut_ofndr_prgrmng.prgrm_prvdr_loc_id,
      us_ut_ofndr_prgrmng.agcy_name,
      us_ut_ofndr_prgrmng.prog_typ_cd,
      us_ut_ofndr_prgrmng.strt_dt__raw,
      us_ut_ofndr_prgrmng.end_dt__raw,
      us_ut_ofndr_prgrmng.cmt,
      us_ut_ofndr_prgrmng.prog_comp_cd,
      us_ut_ofndr_prgrmng.omp_exit_typ_cd,
      us_ut_ofndr_prgrmng.updt_usr_id,
      us_ut_ofndr_prgrmng.updt_dt__raw,
      us_ut_ofndr_prgrmng.vrfy_dt__raw,
      us_ut_ofndr_prgrmng.vrfy_usr_id,
      us_ut_ofndr_prgrmng.vrfy_rslt_flg,
      us_ut_ofndr_prgrmng.vrfy_mthd_cd,
      us_ut_ofndr_prgrmng.file_id,
      us_ut_ofndr_prgrmng.is_deleted]
    sorts: [us_ut_ofndr_prgrmng.strt_dt__raw]
    note_display: hover
    note_text: "Offender Programng"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 84
    col: 0
    width: 24
    height: 6

  - name: ofndr_sec_assess
    title: ofndr_sec_assess
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_sec_assess.primary_key,
      us_ut_ofndr_sec_assess.assess_id,
      us_ut_ofndr_sec_assess.ofndr_num,
      us_ut_ofndr_sec_assess.ofndr_tst_id,
      us_ut_ofndr_sec_assess.assess_rsn_cd,
      us_ut_ofndr_sec_assess.assess_dt__raw,
      us_ut_ofndr_sec_assess.assess_typ,
      us_ut_ofndr_sec_assess.case_wrkr_usr_id,
      us_ut_ofndr_sec_assess.security_score,
      us_ut_ofndr_sec_assess.eff_strt_dt__raw,
      us_ut_ofndr_sec_assess.custody_level,
      us_ut_ofndr_sec_assess.override_rsn_cd,
      us_ut_ofndr_sec_assess.ovrd_rsn_cmt,
      us_ut_ofndr_sec_assess.review_by_usr_id,
      us_ut_ofndr_sec_assess.end_dt__raw,
      us_ut_ofndr_sec_assess.updt_usr_id,
      us_ut_ofndr_sec_assess.updt_dt__raw,
      us_ut_ofndr_sec_assess.file_id,
      us_ut_ofndr_sec_assess.is_deleted]
    sorts: [us_ut_ofndr_sec_assess.assess_dt__raw]
    note_display: hover
    note_text: "JII security assessments. I think these security assessments are the result of tests described in the ofndr_tst and ofndr_tst_eval tables. Not all tests result in a security assessment."
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 90
    col: 0
    width: 24
    height: 6

  - name: ofndr_spcl_sprvsn
    title: ofndr_spcl_sprvsn
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_spcl_sprvsn.primary_key,
      us_ut_ofndr_spcl_sprvsn.ofndr_spcl_sprvsn_id,
      us_ut_ofndr_spcl_sprvsn.ofndr_num,
      us_ut_ofndr_spcl_sprvsn.spcl_sprvsn_id,
      us_ut_ofndr_spcl_sprvsn.strt_dt__raw,
      us_ut_ofndr_spcl_sprvsn.end_dt__raw,
      us_ut_ofndr_spcl_sprvsn.end_rsn_cd,
      us_ut_ofndr_spcl_sprvsn.updt_usr_id,
      us_ut_ofndr_spcl_sprvsn.updt_dt__raw,
      us_ut_ofndr_spcl_sprvsn.file_id,
      us_ut_ofndr_spcl_sprvsn.is_deleted]
    sorts: [us_ut_ofndr_spcl_sprvsn.strt_dt__raw]
    note_display: hover
    note_text: "Offender Special Supervision"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 96
    col: 0
    width: 24
    height: 6

  - name: ofndr_sprvsn
    title: ofndr_sprvsn
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_sprvsn.primary_key,
      us_ut_ofndr_sprvsn.ofndr_sprvsn_id,
      us_ut_ofndr_sprvsn.ofndr_num,
      us_ut_ofndr_sprvsn.sprvsn_lvl_cd,
      us_ut_ofndr_sprvsn.ofndr_tst_id,
      us_ut_ofndr_sprvsn.strt_dt__raw,
      us_ut_ofndr_sprvsn.end_dt__raw,
      us_ut_ofndr_sprvsn.updt_usr_id,
      us_ut_ofndr_sprvsn.updt_dt__raw,
      us_ut_ofndr_sprvsn.file_id,
      us_ut_ofndr_sprvsn.is_deleted]
    sorts: [us_ut_ofndr_sprvsn.strt_dt__raw]
    note_display: hover
    note_text: "Offender Supervision"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 102
    col: 0
    width: 24
    height: 6

  - name: ofndr_tst
    title: ofndr_tst
    explore: us_ut_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ut_ofndr_tst.primary_key,
      us_ut_ofndr_tst.ofndr_tst_id,
      us_ut_ofndr_tst.body_loc_cd,
      us_ut_ofndr_tst.ofndr_num,
      us_ut_ofndr_tst.assess_tst_id,
      us_ut_ofndr_tst.tst_dt__raw,
      us_ut_ofndr_tst.score_by_name,
      us_ut_ofndr_tst.updt_usr_id,
      us_ut_ofndr_tst.updt_dt__raw,
      us_ut_ofndr_tst.ofn_assess_tst_id,
      us_ut_ofndr_tst.ofn_ofndr_tst_id,
      us_ut_ofndr_tst.tst_qstn_rspns_id,
      us_ut_ofndr_tst.file_id,
      us_ut_ofndr_tst.is_deleted]
    sorts: [us_ut_ofndr_tst.tst_dt__raw]
    note_display: hover
    note_text: "Contains records of all \"tests\" which can include assessments and other types of surveys"
    listen: 
      View Type: us_ut_ofndr.view_type
      US_UT_DOC: us_ut_ofndr.ofndr_num
    row: 108
    col: 0
    width: 24
    height: 6

