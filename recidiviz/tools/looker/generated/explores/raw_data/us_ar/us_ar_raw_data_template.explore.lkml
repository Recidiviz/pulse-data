# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_ar_raw_data_template {
  extension: required

  view_name: us_ar_OFFENDERPROFILE
  view_label: "us_ar_OFFENDERPROFILE"

  description: "Data pertaining to an individual in Arkansas"
  group_label: "Raw State Data"
  label: "US_AR Raw Data"
  join: us_ar_ADMISSIONSUMMARY {
    sql_on: ${us_ar_ADMISSIONSUMMARY.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_ADMISSIONSUMMARY"
  }

  join: us_ar_BEDASSIGNMENT {
    sql_on: ${us_ar_BEDASSIGNMENT.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_BEDASSIGNMENT"
  }

  join: us_ar_BOARDHEARING {
    sql_on: ${us_ar_BOARDHEARING.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_BOARDHEARING"
  }

  join: us_ar_CLIENTPROFILE {
    sql_on: ${us_ar_CLIENTPROFILE.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_CLIENTPROFILE"
  }

  join: us_ar_COMMITMENTSUMMARY {
    sql_on: ${us_ar_COMMITMENTSUMMARY.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_COMMITMENTSUMMARY"
  }

  join: us_ar_CUSTODYCLASS {
    sql_on: ${us_ar_CUSTODYCLASS.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_CUSTODYCLASS"
  }

  join: us_ar_DEADTIME {
    sql_on: ${us_ar_DEADTIME.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_DEADTIME"
  }

  join: us_ar_DEADTIMEPENALTY {
    sql_on: ${us_ar_DEADTIMEPENALTY.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_DEADTIMEPENALTY"
  }

  join: us_ar_DEMOGRAPHICPROFILE {
    sql_on: ${us_ar_DEMOGRAPHICPROFILE.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_DEMOGRAPHICPROFILE"
  }

  join: us_ar_DETAINERNOTIFY {
    sql_on: ${us_ar_DETAINERNOTIFY.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_DETAINERNOTIFY"
  }

  join: us_ar_DISCIPLINARYVIOLAT {
    sql_on: ${us_ar_DISCIPLINARYVIOLAT.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_DISCIPLINARYVIOLAT"
  }

  join: us_ar_EDUCATIONRECORD {
    sql_on: ${us_ar_EDUCATIONRECORD.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_EDUCATIONRECORD"
  }

  join: us_ar_EMPLOYMENTHISTORY {
    sql_on: ${us_ar_EMPLOYMENTHISTORY.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_EMPLOYMENTHISTORY"
  }

  join: us_ar_EXTERNALMOVEMENT {
    sql_on: ${us_ar_EXTERNALMOVEMENT.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_EXTERNALMOVEMENT"
  }

  join: us_ar_INMATEPROFILE {
    sql_on: ${us_ar_INMATEPROFILE.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_INMATEPROFILE"
  }

  join: us_ar_INTERVENTOFFENSE {
    sql_on: ${us_ar_INTERVENTOFFENSE.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_INTERVENTOFFENSE"
  }

  join: us_ar_INTERVENTSANCTION {
    sql_on: ${us_ar_INTERVENTSANCTION.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_INTERVENTSANCTION"
  }

  join: us_ar_INTERVENTTYPE {
    sql_on: ${us_ar_INTERVENTTYPE.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_INTERVENTTYPE"
  }

  join: us_ar_JOBPROGRAMASGMT {
    sql_on: ${us_ar_JOBPROGRAMASGMT.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_JOBPROGRAMASGMT"
  }

  join: us_ar_OFFENDERNAMEALIAS {
    sql_on: ${us_ar_OFFENDERNAMEALIAS.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_OFFENDERNAMEALIAS"
  }

  join: us_ar_OFFNSTANDARDFORM {
    sql_on: ${us_ar_OFFNSTANDARDFORM.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_OFFNSTANDARDFORM"
  }

  join: us_ar_OFNRELATEDADDRESS {
    sql_on: ${us_ar_OFNRELATEDADDRESS.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_OFNRELATEDADDRESS"
  }

  join: us_ar_PAROLERISKASMNT {
    sql_on: ${us_ar_PAROLERISKASMNT.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_PAROLERISKASMNT"
  }

  join: us_ar_PAROLERISKASMTTOOL {
    sql_on: ${us_ar_PAROLERISKASMTTOOL.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_PAROLERISKASMTTOOL"
  }

  join: us_ar_PROGRAMACHIEVEMENT {
    sql_on: ${us_ar_PROGRAMACHIEVEMENT.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_PROGRAMACHIEVEMENT"
  }

  join: us_ar_RELASSOCRELATION {
    sql_on: ${us_ar_RELASSOCRELATION.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_RELASSOCRELATION"
  }

  join: us_ar_RELEASEDATECHANGE {
    sql_on: ${us_ar_RELEASEDATECHANGE.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_RELEASEDATECHANGE"
  }

  join: us_ar_RISKNEEDCLASS {
    sql_on: ${us_ar_RISKNEEDCLASS.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_RISKNEEDCLASS"
  }

  join: us_ar_SENTENCECOMPONENT {
    sql_on: ${us_ar_SENTENCECOMPONENT.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_SENTENCECOMPONENT"
  }

  join: us_ar_SENTENCECOMPUTE {
    sql_on: ${us_ar_SENTENCECOMPUTE.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_SENTENCECOMPUTE"
  }

  join: us_ar_SENTENCECREDITDBT {
    sql_on: ${us_ar_SENTENCECREDITDBT.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_SENTENCECREDITDBT"
  }

  join: us_ar_SPECIALCONDITION {
    sql_on: ${us_ar_SPECIALCONDITION.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_SPECIALCONDITION"
  }

  join: us_ar_SUPERVISIONCONTACT {
    sql_on: ${us_ar_SUPERVISIONCONTACT.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_SUPERVISIONCONTACT"
  }

  join: us_ar_SUPERVISIONEVENT {
    sql_on: ${us_ar_SUPERVISIONEVENT.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_SUPERVISIONEVENT"
  }

  join: us_ar_SUPVINCENTIVE {
    sql_on: ${us_ar_SUPVINCENTIVE.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_SUPVINCENTIVE"
  }

  join: us_ar_SUPVINTERVENTION {
    sql_on: ${us_ar_SUPVINTERVENTION.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_SUPVINTERVENTION"
  }

  join: us_ar_SUPVTIMELINE {
    sql_on: ${us_ar_SUPVTIMELINE.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_SUPVTIMELINE"
  }

  join: us_ar_VISITATIONSTATUS {
    sql_on: ${us_ar_VISITATIONSTATUS.OFFENDERID} = ${us_ar_OFFENDERPROFILE.OFFENDERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ar_VISITATIONSTATUS"
  }

}
