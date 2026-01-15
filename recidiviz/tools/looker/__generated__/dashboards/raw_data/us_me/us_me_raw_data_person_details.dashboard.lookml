# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_me_raw_data_person_details
  title: Maine Raw Data Person Details
  description: For examining individuals in US_ME's raw data tables
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
    model: "@{model_name}"
    explore: us_me_raw_data
    field: us_me_CIS_100_CLIENT.view_type

  - name: US_ME_DOC
    title: US_ME_DOC
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{model_name}"
    explore: us_me_raw_data
    field: us_me_CIS_100_CLIENT.Client_Id

  elements:
  - name: CIS_100_CLIENT
    title: CIS_100_CLIENT
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_100_CLIENT.primary_key,
      us_me_CIS_100_CLIENT.Adult_Ind,
      us_me_CIS_100_CLIENT.Birth_Date__raw,
      us_me_CIS_100_CLIENT.Cis_1002_Citizenship_Cd,
      us_me_CIS_100_CLIENT.Cis_1004_Eye_Colour_Cd,
      us_me_CIS_100_CLIENT.Cis_1005_Hair_Colour_Cd,
      us_me_CIS_100_CLIENT.Cis_1006_Race_Cd,
      us_me_CIS_100_CLIENT.Cis_1010_Marital_Status_Cd,
      us_me_CIS_100_CLIENT.Cis_1013_Complexion_Type_Cd,
      us_me_CIS_100_CLIENT.Cis_1016_Disch_Type_Cd,
      us_me_CIS_100_CLIENT.Cis_1016_Gang_Cd,
      us_me_CIS_100_CLIENT.Cis_1016_Guardian_Cd,
      us_me_CIS_100_CLIENT.Cis_1016_Hispanic_Cd,
      us_me_CIS_100_CLIENT.Cis_1016_Milit_Br_Cd,
      us_me_CIS_100_CLIENT.Cis_1016_Na_Tribe_Cd,
      us_me_CIS_100_CLIENT.Cis_1016_Religion_Cd,
      us_me_CIS_100_CLIENT.Cis_1016_Report_Type_Cd,
      us_me_CIS_100_CLIENT.Cis_1016_Skill_Cd,
      us_me_CIS_100_CLIENT.Cis_1017_Milit_Stat_Cd,
      us_me_CIS_100_CLIENT.Cis_900_Employee_Id,
      us_me_CIS_100_CLIENT.Cis_9012_Gender_Cd,
      us_me_CIS_100_CLIENT.Cis_9013_Lang_Preferred_Cd,
      us_me_CIS_100_CLIENT.Cis_9013_Parent_Guard_Lang_Pref_Cd,
      us_me_CIS_100_CLIENT.Client_Id,
      us_me_CIS_100_CLIENT.Clothing_Description_Tx,
      us_me_CIS_100_CLIENT.Created_By_Tx,
      us_me_CIS_100_CLIENT.Created_On_Date,
      us_me_CIS_100_CLIENT.Dangerous_Not_Dangerous_Tx,
      us_me_CIS_100_CLIENT.Date_Of_Emancipation_Tx,
      us_me_CIS_100_CLIENT.Death_Certificate_Tx,
      us_me_CIS_100_CLIENT.Death_Date,
      us_me_CIS_100_CLIENT.Death_Description_Tx,
      us_me_CIS_100_CLIENT.Death_Ind,
      us_me_CIS_100_CLIENT.Delete_Comment_Tx,
      us_me_CIS_100_CLIENT.Dna_Required_Ind,
      us_me_CIS_100_CLIENT.Dna_Sample_Date,
      us_me_CIS_100_CLIENT.Email_Tx,
      us_me_CIS_100_CLIENT.Fingerprint_Classification_Tx,
      us_me_CIS_100_CLIENT.First_Name,
      us_me_CIS_100_CLIENT.Furlough_Date,
      us_me_CIS_100_CLIENT.Glasses_Ind,
      us_me_CIS_100_CLIENT.Grievance_Policy_Notification_Date,
      us_me_CIS_100_CLIENT.Height_Num,
      us_me_CIS_100_CLIENT.Hispanic_Ind,
      us_me_CIS_100_CLIENT.Home_Visit_Ind,
      us_me_CIS_100_CLIENT.Juv_Id,
      us_me_CIS_100_CLIENT.Juvenile_Ind,
      us_me_CIS_100_CLIENT.Last_Name,
      us_me_CIS_100_CLIENT.Laundry_Num,
      us_me_CIS_100_CLIENT.Literacy_Read_Ind,
      us_me_CIS_100_CLIENT.Literacy_Write_Ind,
      us_me_CIS_100_CLIENT.Lmtd_Engl_Spk_Ablty_Ind,
      us_me_CIS_100_CLIENT.Lock_Ind,
      us_me_CIS_100_CLIENT.Med_Marijuana_Program_Physician_Certificate_Ind,
      us_me_CIS_100_CLIENT.Middle_Name,
      us_me_CIS_100_CLIENT.Modified_By_Tx,
      us_me_CIS_100_CLIENT.Modified_On_Date,
      us_me_CIS_100_CLIENT.Mothers_Maiden_Name,
      us_me_CIS_100_CLIENT.Name_Suffix_Tx,
      us_me_CIS_100_CLIENT.Next_Reporting_Date,
      us_me_CIS_100_CLIENT.Note_Place_Tx,
      us_me_CIS_100_CLIENT.Phone_Pin_Num,
      us_me_CIS_100_CLIENT.Physical_App_Notes_Tx,
      us_me_CIS_100_CLIENT.Place_Of_Birth_Tx,
      us_me_CIS_100_CLIENT.Sex_Off_Not_Ind,
      us_me_CIS_100_CLIENT.Sex_Off_Reg_Ind,
      us_me_CIS_100_CLIENT.Single_Room_Ind,
      us_me_CIS_100_CLIENT.Soundex_First_Name,
      us_me_CIS_100_CLIENT.Soundex_Last_Name,
      us_me_CIS_100_CLIENT.Soundex_Middle_Name,
      us_me_CIS_100_CLIENT.Upper_First_Name,
      us_me_CIS_100_CLIENT.Upper_Last_Name,
      us_me_CIS_100_CLIENT.Upper_Middle_Name,
      us_me_CIS_100_CLIENT.Wabanaki_Descendent_Ind,
      us_me_CIS_100_CLIENT.Wabanaki_Ind,
      us_me_CIS_100_CLIENT.Wabanaki_Member_Ind,
      us_me_CIS_100_CLIENT.Weight_Num,
      us_me_CIS_100_CLIENT.Cis_1016_Insurance_Type_Cd,
      us_me_CIS_100_CLIENT.Current_Free_Phone_Minutes_Limit,
      us_me_CIS_100_CLIENT.Current_Kiosk_Property_Limit,
      us_me_CIS_100_CLIENT.Current_Kiosk_Spending_Limit,
      us_me_CIS_100_CLIENT.Current_Phone_Minutes_Limit,
      us_me_CIS_100_CLIENT.Current_Score_Tablet_Limit,
      us_me_CIS_100_CLIENT.Current_Visitation_Visits_Limit,
      us_me_CIS_100_CLIENT.Fixed_Free_Phone_Minutes_Limit,
      us_me_CIS_100_CLIENT.Fixed_Kiosk_Property_Limit,
      us_me_CIS_100_CLIENT.Fixed_Kiosk_Spending_Limit,
      us_me_CIS_100_CLIENT.Fixed_Phone_Minutes_Limit,
      us_me_CIS_100_CLIENT.Fixed_Score_Tablet_Limit,
      us_me_CIS_100_CLIENT.Fixed_Visitation_Visits_Limit,
      us_me_CIS_100_CLIENT.file_id,
      us_me_CIS_100_CLIENT.is_deleted]
    sorts: [us_me_CIS_100_CLIENT.Birth_Date__raw]
    note_display: hover
    note_text: "This table contains general details about each client in Maine's offender management system."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 0
    col: 0
    width: 24
    height: 6

  - name: CIS_102_ALERT_HISTORY
    title: CIS_102_ALERT_HISTORY
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_102_ALERT_HISTORY.primary_key,
      us_me_CIS_102_ALERT_HISTORY.ALERT_HISTORY_ID,
      us_me_CIS_102_ALERT_HISTORY.EFFCT_DATE__raw,
      us_me_CIS_102_ALERT_HISTORY.INEFFCT_DATE__raw,
      us_me_CIS_102_ALERT_HISTORY.NOTES_TX,
      us_me_CIS_102_ALERT_HISTORY.CIS_100_CLIENT_ID,
      us_me_CIS_102_ALERT_HISTORY.CIS_1020_ALERT_CD,
      us_me_CIS_102_ALERT_HISTORY.CIS_1020_ALERT_2_CD,
      us_me_CIS_102_ALERT_HISTORY.CREATED_BY_TX,
      us_me_CIS_102_ALERT_HISTORY.CREATED_ON_DATE__raw,
      us_me_CIS_102_ALERT_HISTORY.MODIFIED_BY_TX,
      us_me_CIS_102_ALERT_HISTORY.MODIFIED_ON_DATE__raw,
      us_me_CIS_102_ALERT_HISTORY.CCS_ID,
      us_me_CIS_102_ALERT_HISTORY.file_id,
      us_me_CIS_102_ALERT_HISTORY.is_deleted]
    sorts: [us_me_CIS_102_ALERT_HISTORY.EFFCT_DATE__raw]
    note_display: hover
    note_text: "Contains records of alerts associated with clients."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 6
    col: 0
    width: 24
    height: 6

  - name: CIS_106_ADDRESS
    title: CIS_106_ADDRESS
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_106_ADDRESS.primary_key,
      us_me_CIS_106_ADDRESS.Address_Id,
      us_me_CIS_106_ADDRESS.Care_Of_Name,
      us_me_CIS_106_ADDRESS.Cis_100_Client_Id,
      us_me_CIS_106_ADDRESS.Cis_1016_Relationship_Type_Cd,
      us_me_CIS_106_ADDRESS.Cis_1017_Liv_Arrgmt_Cd,
      us_me_CIS_106_ADDRESS.Cis_1017_Place_By_Cd,
      us_me_CIS_106_ADDRESS.Cis_109_Civilian_Id,
      us_me_CIS_106_ADDRESS.Cis_109_Victi_Id,
      us_me_CIS_106_ADDRESS.Cis_450_Agency_Id,
      us_me_CIS_106_ADDRESS.Cis_9002_Country_Cd,
      us_me_CIS_106_ADDRESS.Cis_9003_County_Cd,
      us_me_CIS_106_ADDRESS.Cis_9004_Prov_Cd,
      us_me_CIS_106_ADDRESS.Cis_9005_Address_Use_Cd,
      us_me_CIS_106_ADDRESS.Cis_908_Ccs_Location_Id,
      us_me_CIS_106_ADDRESS.Cis_9081_Place_Cd,
      us_me_CIS_106_ADDRESS.Comments_Tx,
      us_me_CIS_106_ADDRESS.Created_By_Tx,
      us_me_CIS_106_ADDRESS.Created_On_Date,
      us_me_CIS_106_ADDRESS.End_Date,
      us_me_CIS_106_ADDRESS.Interstate_Compact_No_Tx,
      us_me_CIS_106_ADDRESS.Logical_Delete_Ind,
      us_me_CIS_106_ADDRESS.Modified_By_Tx,
      us_me_CIS_106_ADDRESS.Modified_On_Date,
      us_me_CIS_106_ADDRESS.Note_Ind,
      us_me_CIS_106_ADDRESS.Other_Country_Tx,
      us_me_CIS_106_ADDRESS.Other_Relation_Tx,
      us_me_CIS_106_ADDRESS.Out_State_Place_Tx,
      us_me_CIS_106_ADDRESS.Po_Box_Tx,
      us_me_CIS_106_ADDRESS.Postal_Cd,
      us_me_CIS_106_ADDRESS.Primary_Address_Ind,
      us_me_CIS_106_ADDRESS.Residing_With_Tx,
      us_me_CIS_106_ADDRESS.Start_Date,
      us_me_CIS_106_ADDRESS.Street_Name_Tx,
      us_me_CIS_106_ADDRESS.Street_Num,
      us_me_CIS_106_ADDRESS.Street_Num_Tx,
      us_me_CIS_106_ADDRESS.Unit_Id,
      us_me_CIS_106_ADDRESS.Cis_9004_Country_Cd,
      us_me_CIS_106_ADDRESS.file_id,
      us_me_CIS_106_ADDRESS.is_deleted]
    sorts: [us_me_CIS_106_ADDRESS.Address_Id]
    note_display: hover
    note_text: "This table lists each client's location and address within the Start_Date period."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 12
    col: 0
    width: 24
    height: 6

  - name: CIS_112_CUSTODY_LEVEL
    title: CIS_112_CUSTODY_LEVEL
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_112_CUSTODY_LEVEL.primary_key,
      us_me_CIS_112_CUSTODY_LEVEL.LEVEL_ID,
      us_me_CIS_112_CUSTODY_LEVEL.CUSTODY_DATE__raw,
      us_me_CIS_112_CUSTODY_LEVEL.NOTES_TX,
      us_me_CIS_112_CUSTODY_LEVEL.CIS_100_CLIENT_ID,
      us_me_CIS_112_CUSTODY_LEVEL.CIS_1017_CLIENT_SYS_CD,
      us_me_CIS_112_CUSTODY_LEVEL.CIS_900_EMPLOYEE_ID,
      us_me_CIS_112_CUSTODY_LEVEL.CIS_908_CCS_LOCATION_ID,
      us_me_CIS_112_CUSTODY_LEVEL.LOGICAL_DELETE_IND,
      us_me_CIS_112_CUSTODY_LEVEL.CREATED_BY_TX,
      us_me_CIS_112_CUSTODY_LEVEL.CREATED_ON_DATE,
      us_me_CIS_112_CUSTODY_LEVEL.MODIFIED_BY_TX,
      us_me_CIS_112_CUSTODY_LEVEL.MODIFIED_ON_DATE,
      us_me_CIS_112_CUSTODY_LEVEL.file_id,
      us_me_CIS_112_CUSTODY_LEVEL.is_deleted]
    sorts: [us_me_CIS_112_CUSTODY_LEVEL.CUSTODY_DATE__raw]
    note_display: hover
    note_text: "This table lists the history of custody levels for persons under MEDOC jurisdiction. Each record represents a period of time during which the specified custody level was active."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 18
    col: 0
    width: 24
    height: 6

  - name: CIS_116_LSI_HISTORY
    title: CIS_116_LSI_HISTORY
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_116_LSI_HISTORY.primary_key,
      us_me_CIS_116_LSI_HISTORY.Cis_100_Client_Id,
      us_me_CIS_116_LSI_HISTORY.Cis_1009_Lsi_Type_Cd,
      us_me_CIS_116_LSI_HISTORY.Cis_1161_Comm_Lsi_Apprv_Rating_Date__raw,
      us_me_CIS_116_LSI_HISTORY.Cis_1161_Comm_Lsi_Effct_Date__raw,
      us_me_CIS_116_LSI_HISTORY.Cis_1161_Comm_Lsi_Or_Date__raw,
      us_me_CIS_116_LSI_HISTORY.Cis_1161_Comm_Lsi_Or_Rating_Cd,
      us_me_CIS_116_LSI_HISTORY.Cis_1161_Comm_Lsi_Rating_Cd,
      us_me_CIS_116_LSI_HISTORY.Cis_1162_Occupation_Cd,
      us_me_CIS_116_LSI_HISTORY.Cis_900_Approver_Id,
      us_me_CIS_116_LSI_HISTORY.Cis_900_Employee_Id,
      us_me_CIS_116_LSI_HISTORY.Citrix_Lsi_Id,
      us_me_CIS_116_LSI_HISTORY.Comm_Apprv_Notes_Tx,
      us_me_CIS_116_LSI_HISTORY.Comm_Override_Notes_Tx,
      us_me_CIS_116_LSI_HISTORY.Competency_Tx,
      us_me_CIS_116_LSI_HISTORY.Created_By_Tx,
      us_me_CIS_116_LSI_HISTORY.Created_On_Date__raw,
      us_me_CIS_116_LSI_HISTORY.Home_Visit_Ind,
      us_me_CIS_116_LSI_HISTORY.Lsi_Date__raw,
      us_me_CIS_116_LSI_HISTORY.Lsi_History_Id,
      us_me_CIS_116_LSI_HISTORY.Lsi_Score_Num,
      us_me_CIS_116_LSI_HISTORY.Lsi_Xml_Tx,
      us_me_CIS_116_LSI_HISTORY.Manual_Score_Ind,
      us_me_CIS_116_LSI_HISTORY.Modified_By_Tx,
      us_me_CIS_116_LSI_HISTORY.Modified_On_Date,
      us_me_CIS_116_LSI_HISTORY.Reassessment_Date__raw,
      us_me_CIS_116_LSI_HISTORY.Submit_Score_Ind,
      us_me_CIS_116_LSI_HISTORY.Cis_1161_Comm_Lsi_Apprv_Rating_Cd,
      us_me_CIS_116_LSI_HISTORY.Override_Type,
      us_me_CIS_116_LSI_HISTORY.file_id,
      us_me_CIS_116_LSI_HISTORY.is_deleted]
    sorts: [us_me_CIS_116_LSI_HISTORY.Cis_1161_Comm_Lsi_Apprv_Rating_Date__raw]
    note_display: hover
    note_text: "This table has LSI assessments results for a client and includes information about when the LSI assessment was  taken, whether the LSI rating was overridden, the reason it was overridden, the reassessment date,  and the date the score is effective from."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 24
    col: 0
    width: 24
    height: 6

  - name: CIS_124_SUPERVISION_HISTORY
    title: CIS_124_SUPERVISION_HISTORY
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_124_SUPERVISION_HISTORY.primary_key,
      us_me_CIS_124_SUPERVISION_HISTORY.Assignment_Date__raw,
      us_me_CIS_124_SUPERVISION_HISTORY.Cis_100_Client_Id,
      us_me_CIS_124_SUPERVISION_HISTORY.Cis_1240_Supervision_Type_Cd,
      us_me_CIS_124_SUPERVISION_HISTORY.Cis_1241_Super_Status_Cd,
      us_me_CIS_124_SUPERVISION_HISTORY.Cis_1242_Supervision_Category_Cd,
      us_me_CIS_124_SUPERVISION_HISTORY.Cis_900_Employee_Id,
      us_me_CIS_124_SUPERVISION_HISTORY.Cis_908_Ccs_Location_Id,
      us_me_CIS_124_SUPERVISION_HISTORY.Created_By_Tx,
      us_me_CIS_124_SUPERVISION_HISTORY.Created_On_Date,
      us_me_CIS_124_SUPERVISION_HISTORY.Modified_By_Tx,
      us_me_CIS_124_SUPERVISION_HISTORY.Modified_On_Date,
      us_me_CIS_124_SUPERVISION_HISTORY.Primary_Ind,
      us_me_CIS_124_SUPERVISION_HISTORY.Supervision_End_Date__raw,
      us_me_CIS_124_SUPERVISION_HISTORY.Supervision_History_Id,
      us_me_CIS_124_SUPERVISION_HISTORY.file_id,
      us_me_CIS_124_SUPERVISION_HISTORY.is_deleted]
    sorts: [us_me_CIS_124_SUPERVISION_HISTORY.Assignment_Date__raw]
    note_display: hover
    note_text: "This table records assignments for supervision officers. The Cis_908_Ccs_Location_Id field on this table more reliably represents a client's location than the location in the CIS_125_CURRENT_STATUS_HIST table. This is because someone could be assigned a new officer when moving locations and the officer updating the system may not create a transfer record in the CIS_314_TRANSFER table to reflect the change of location. A new record in the CIS_125_CURRENT_STATUS_HIST table is created when a transfer is created, and the current status location reflects the transfer location. Updates to the CIS_124_SUPERVISION_HISTORY table are not reflected in the CIS_125_CURRENT_STATUS_HIST table. It's not yet known how prevalant this mismatch in locations is."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 30
    col: 0
    width: 24
    height: 6

  - name: CIS_125_CURRENT_STATUS_HIST
    title: CIS_125_CURRENT_STATUS_HIST
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_125_CURRENT_STATUS_HIST.primary_key,
      us_me_CIS_125_CURRENT_STATUS_HIST.Cis_100_Client_Id,
      us_me_CIS_125_CURRENT_STATUS_HIST.Cis_1000_Current_Status_Cd,
      us_me_CIS_125_CURRENT_STATUS_HIST.Cis_3091_Mvmt_Reason_Cd,
      us_me_CIS_125_CURRENT_STATUS_HIST.Cis_314_Transfer_Id,
      us_me_CIS_125_CURRENT_STATUS_HIST.Cis_908_Ccs_Location_2_Id,
      us_me_CIS_125_CURRENT_STATUS_HIST.Cis_908_Ccs_Location_Id,
      us_me_CIS_125_CURRENT_STATUS_HIST.Cis_912_Unit_Id,
      us_me_CIS_125_CURRENT_STATUS_HIST.Cis_913_Pod_Id,
      us_me_CIS_125_CURRENT_STATUS_HIST.Cis_914_Cell_Id,
      us_me_CIS_125_CURRENT_STATUS_HIST.Cis_915_Bed_Id,
      us_me_CIS_125_CURRENT_STATUS_HIST.Community_Ind,
      us_me_CIS_125_CURRENT_STATUS_HIST.Created_By_Tx,
      us_me_CIS_125_CURRENT_STATUS_HIST.Created_On_Date,
      us_me_CIS_125_CURRENT_STATUS_HIST.Current_Status_Hist_Id,
      us_me_CIS_125_CURRENT_STATUS_HIST.Effct_Date__raw,
      us_me_CIS_125_CURRENT_STATUS_HIST.End_Date__raw,
      us_me_CIS_125_CURRENT_STATUS_HIST.End_Datetime,
      us_me_CIS_125_CURRENT_STATUS_HIST.Furlough_Ind,
      us_me_CIS_125_CURRENT_STATUS_HIST.Hospital_Ind,
      us_me_CIS_125_CURRENT_STATUS_HIST.Ineffct_Date__raw,
      us_me_CIS_125_CURRENT_STATUS_HIST.Modified_By_Tx,
      us_me_CIS_125_CURRENT_STATUS_HIST.Modified_On_Date,
      us_me_CIS_125_CURRENT_STATUS_HIST.Out_Pop_Count_Ind,
      us_me_CIS_125_CURRENT_STATUS_HIST.Prim_Mess_Ind,
      us_me_CIS_125_CURRENT_STATUS_HIST.Rca_Mess_Ind,
      us_me_CIS_125_CURRENT_STATUS_HIST.Start_Datetime,
      us_me_CIS_125_CURRENT_STATUS_HIST.file_id,
      us_me_CIS_125_CURRENT_STATUS_HIST.is_deleted]
    sorts: [us_me_CIS_125_CURRENT_STATUS_HIST.Effct_Date__raw]
    note_display: hover
    note_text: "This table contains all of the statuses from incarceration and supervision that any client has ever had. It is an append-only table that is updated when new records are created in the CIS_314_TRANSFER table."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 36
    col: 0
    width: 24
    height: 6

  - name: CIS_128_EMPLOYMENT_HISTORY
    title: CIS_128_EMPLOYMENT_HISTORY
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_128_EMPLOYMENT_HISTORY.primary_key,
      us_me_CIS_128_EMPLOYMENT_HISTORY.EMPLOYMENT_HISTORY_ID,
      us_me_CIS_128_EMPLOYMENT_HISTORY.START_DATE__raw,
      us_me_CIS_128_EMPLOYMENT_HISTORY.END_DATE__raw,
      us_me_CIS_128_EMPLOYMENT_HISTORY.INCOME_NUM,
      us_me_CIS_128_EMPLOYMENT_HISTORY.EMPLOYER_TX,
      us_me_CIS_128_EMPLOYMENT_HISTORY.OCCUPATION_TX,
      us_me_CIS_128_EMPLOYMENT_HISTORY.LOGICAL_DELETE_IND,
      us_me_CIS_128_EMPLOYMENT_HISTORY.PRIMARY_EMPLOYMENT_IND,
      us_me_CIS_128_EMPLOYMENT_HISTORY.INTERSTATE_COMPACT_IND,
      us_me_CIS_128_EMPLOYMENT_HISTORY.NOTES_TX,
      us_me_CIS_128_EMPLOYMENT_HISTORY.CIS_1280_EMPLOYMENT_STATUS_CD,
      us_me_CIS_128_EMPLOYMENT_HISTORY.CIS_100_CLIENT_ID,
      us_me_CIS_128_EMPLOYMENT_HISTORY.CREATED_BY_TX,
      us_me_CIS_128_EMPLOYMENT_HISTORY.CREATED_ON_DATE,
      us_me_CIS_128_EMPLOYMENT_HISTORY.MODIFIED_BY_TX,
      us_me_CIS_128_EMPLOYMENT_HISTORY.MODIFIED_ON_DATE,
      us_me_CIS_128_EMPLOYMENT_HISTORY.file_id,
      us_me_CIS_128_EMPLOYMENT_HISTORY.is_deleted]
    sorts: [us_me_CIS_128_EMPLOYMENT_HISTORY.START_DATE__raw]
    note_display: hover
    note_text: "Contains a record of employment status information for persons under MDOC custody."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 42
    col: 0
    width: 24
    height: 6

  - name: CIS_130_INVESTIGATION
    title: CIS_130_INVESTIGATION
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_130_INVESTIGATION.primary_key,
      us_me_CIS_130_INVESTIGATION.Cis_100_Client_Id,
      us_me_CIS_130_INVESTIGATION.Cis_900_Employee_Id,
      us_me_CIS_130_INVESTIGATION.Cis_908_Ccs_Location_Id,
      us_me_CIS_130_INVESTIGATION.Complete_Date__raw,
      us_me_CIS_130_INVESTIGATION.Complete_Date_Changed_Date__raw,
      us_me_CIS_130_INVESTIGATION.Created_By_Tx,
      us_me_CIS_130_INVESTIGATION.Created_On_Date__raw,
      us_me_CIS_130_INVESTIGATION.Due_Date__raw,
      us_me_CIS_130_INVESTIGATION.First_Name,
      us_me_CIS_130_INVESTIGATION.Investigation_Id,
      us_me_CIS_130_INVESTIGATION.Last_Name,
      us_me_CIS_130_INVESTIGATION.Middle_Name,
      us_me_CIS_130_INVESTIGATION.Modified_By_Tx,
      us_me_CIS_130_INVESTIGATION.Modified_On_Date__raw,
      us_me_CIS_130_INVESTIGATION.Notes_Tx,
      us_me_CIS_130_INVESTIGATION.Request_Date__raw,
      us_me_CIS_130_INVESTIGATION.Cis_1301_Type_Cd,
      us_me_CIS_130_INVESTIGATION.Cis_9902_Court_Cd,
      us_me_CIS_130_INVESTIGATION.file_id,
      us_me_CIS_130_INVESTIGATION.is_deleted]
    sorts: [us_me_CIS_130_INVESTIGATION.Complete_Date__raw]
    note_display: hover
    note_text: "This table lists details about investigations, each row is unique to a client and investigation ID."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 48
    col: 0
    width: 24
    height: 6

  - name: CIS_140_CLASSIFICATION_REVIEW
    title: CIS_140_CLASSIFICATION_REVIEW
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_140_CLASSIFICATION_REVIEW.primary_key,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CLASSIFICATION_ID,
      us_me_CIS_140_CLASSIFICATION_REVIEW.MEETING_DATE__raw,
      us_me_CIS_140_CLASSIFICATION_REVIEW.NEXT_REVIEW_DATE__raw,
      us_me_CIS_140_CLASSIFICATION_REVIEW.REQUEST_TX,
      us_me_CIS_140_CLASSIFICATION_REVIEW.ABSENT_TX,
      us_me_CIS_140_CLASSIFICATION_REVIEW.NOTES_TX,
      us_me_CIS_140_CLASSIFICATION_REVIEW.KEEP_SEPARATE_IND,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CONFLICT_TX,
      us_me_CIS_140_CLASSIFICATION_REVIEW.TRANSFER_NOTES_TX,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_100_CLIENT_ID,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_1401_TYPE_CD,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_1401_FROM_CD,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_1401_ABSENT_CD,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_1401_REQUEST_CD,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_900_EMPLOYEE_ID,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_900_SECURITY_ID,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_900_CHAIR_ID,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_112_CUSTODY_LEVEL_ID,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_908_ORIGINAL_LOC_ID,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_908_DESTINATION_LOC_ID,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_9084_HOUSE_LEVEL_CD,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CREATED_BY_TX,
      us_me_CIS_140_CLASSIFICATION_REVIEW.CREATED_ON_DATE__raw,
      us_me_CIS_140_CLASSIFICATION_REVIEW.MODIFIED_BY_TX,
      us_me_CIS_140_CLASSIFICATION_REVIEW.MODIFIED_ON_DATE__raw,
      us_me_CIS_140_CLASSIFICATION_REVIEW.file_id,
      us_me_CIS_140_CLASSIFICATION_REVIEW.is_deleted]
    sorts: [us_me_CIS_140_CLASSIFICATION_REVIEW.MEETING_DATE__raw]
    note_display: hover
    note_text: "This table lists details about classification review meetings, each row is unique to a client and classification ID."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 54
    col: 0
    width: 24
    height: 6

  - name: CIS_160_DRUG_SCREENING
    title: CIS_160_DRUG_SCREENING
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_160_DRUG_SCREENING.primary_key,
      us_me_CIS_160_DRUG_SCREENING.DRUG_SCREENING_ID,
      us_me_CIS_160_DRUG_SCREENING.SPECIMEN_NUM_TX,
      us_me_CIS_160_DRUG_SCREENING.TEST_DATE__raw,
      us_me_CIS_160_DRUG_SCREENING.POSTED_DATE__raw,
      us_me_CIS_160_DRUG_SCREENING.BAC_NUM,
      us_me_CIS_160_DRUG_SCREENING.NOTES_TX,
      us_me_CIS_160_DRUG_SCREENING.CIS_100_CLIENT_ID,
      us_me_CIS_160_DRUG_SCREENING.CIS_1603_DRUG_TEST_PANEL_CD,
      us_me_CIS_160_DRUG_SCREENING.CIS_1602_DRUG_TEST_REASON_CD,
      us_me_CIS_160_DRUG_SCREENING.REQUESTED_REASON_TX,
      us_me_CIS_160_DRUG_SCREENING.CIS_1601_DRUG_RESULT_CD,
      us_me_CIS_160_DRUG_SCREENING.REFUSED_TO_SUBMIT_IND,
      us_me_CIS_160_DRUG_SCREENING.CIS_900_REQUESTOR_EMP_ID,
      us_me_CIS_160_DRUG_SCREENING.CIS_900_ENTERED_BY_EMP_ID,
      us_me_CIS_160_DRUG_SCREENING.WITNESSED_BY_TX,
      us_me_CIS_160_DRUG_SCREENING.AUTHORIZED_BY_TX,
      us_me_CIS_160_DRUG_SCREENING.PROBLEM_OBTAINING_TX,
      us_me_CIS_160_DRUG_SCREENING.CIS_908_CCS_LOCATION_ID,
      us_me_CIS_160_DRUG_SCREENING.CREATED_BY_TX,
      us_me_CIS_160_DRUG_SCREENING.CREATED_ON_DATE,
      us_me_CIS_160_DRUG_SCREENING.MODIFIED_BY_TX,
      us_me_CIS_160_DRUG_SCREENING.MODIFIED_ON_DATE,
      us_me_CIS_160_DRUG_SCREENING.file_id,
      us_me_CIS_160_DRUG_SCREENING.is_deleted]
    sorts: [us_me_CIS_160_DRUG_SCREENING.TEST_DATE__raw]
    note_display: hover
    note_text: "Contains a record of each drug screen a client has taken."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 60
    col: 0
    width: 24
    height: 6

  - name: CIS_200_CASE_PLAN
    title: CIS_200_CASE_PLAN
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_200_CASE_PLAN.primary_key,
      us_me_CIS_200_CASE_PLAN.Cis_100_Client_Id,
      us_me_CIS_200_CASE_PLAN.Cis_112_Level_Id,
      us_me_CIS_200_CASE_PLAN.Cis_116_Lsi_History_Id,
      us_me_CIS_200_CASE_PLAN.Cis_119_Client_Bin_Id,
      us_me_CIS_200_CASE_PLAN.Cis_215_Sex_Offender_Assess_Id,
      us_me_CIS_200_CASE_PLAN.Cis_900_Employee_Id,
      us_me_CIS_200_CASE_PLAN.Cis_908_Ccs_Location_Id,
      us_me_CIS_200_CASE_PLAN.Close_Date,
      us_me_CIS_200_CASE_PLAN.Cp_Id,
      us_me_CIS_200_CASE_PLAN.Created_By_Tx,
      us_me_CIS_200_CASE_PLAN.Created_On_Date,
      us_me_CIS_200_CASE_PLAN.Message_Ind,
      us_me_CIS_200_CASE_PLAN.Modified_By_Tx,
      us_me_CIS_200_CASE_PLAN.Modified_On_Date,
      us_me_CIS_200_CASE_PLAN.Review_Ind,
      us_me_CIS_200_CASE_PLAN.Review_Sent_Ind,
      us_me_CIS_200_CASE_PLAN.Start_Date,
      us_me_CIS_200_CASE_PLAN.file_id,
      us_me_CIS_200_CASE_PLAN.is_deleted]
    sorts: [us_me_CIS_200_CASE_PLAN.Cis_100_Client_Id, us_me_CIS_200_CASE_PLAN.Cp_Id]
    note_display: hover
    note_text: "This table records the goals of a client."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 66
    col: 0
    width: 24
    height: 6

  - name: CIS_201_GOALS
    title: CIS_201_GOALS
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_201_GOALS.primary_key,
      us_me_CIS_201_GOALS.Cis_200_Cis_100_Client_Id,
      us_me_CIS_201_GOALS.Cis_200_Cp_Id,
      us_me_CIS_201_GOALS.Cis_2010_Goal_Type_Cd,
      us_me_CIS_201_GOALS.Cis_2011_Dmn_Goal_Cd,
      us_me_CIS_201_GOALS.Cis_2012_Goal_Status_Cd,
      us_me_CIS_201_GOALS.Cis_900_Employee_Id,
      us_me_CIS_201_GOALS.Close_Date,
      us_me_CIS_201_GOALS.Created_By_Tx,
      us_me_CIS_201_GOALS.Created_On_Date,
      us_me_CIS_201_GOALS.Goal_Id,
      us_me_CIS_201_GOALS.Goal_Tx,
      us_me_CIS_201_GOALS.Modified_By_Tx,
      us_me_CIS_201_GOALS.Modified_On_Date,
      us_me_CIS_201_GOALS.Open_Date,
      us_me_CIS_201_GOALS.Other,
      us_me_CIS_201_GOALS.file_id,
      us_me_CIS_201_GOALS.is_deleted]
    sorts: [us_me_CIS_201_GOALS.Goal_Id]
    note_display: hover
    note_text: "This table records the goals of a client."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 72
    col: 0
    width: 24
    height: 6

  - name: CIS_204_GEN_NOTE
    title: CIS_204_GEN_NOTE
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_204_GEN_NOTE.primary_key,
      us_me_CIS_204_GEN_NOTE.Cis_100_Client_Id,
      us_me_CIS_204_GEN_NOTE.Cis_2040_Contact_Mode_Cd,
      us_me_CIS_204_GEN_NOTE.Cis_2041_Note_Type_Cd,
      us_me_CIS_204_GEN_NOTE.Cis_2042_Person_Contacted_Cd,
      us_me_CIS_204_GEN_NOTE.Cis_900_Empl_Create_Id,
      us_me_CIS_204_GEN_NOTE.Cis_900_Employee_Id,
      us_me_CIS_204_GEN_NOTE.Cis_908_Ccs_Location_Id,
      us_me_CIS_204_GEN_NOTE.Created_By_Tx,
      us_me_CIS_204_GEN_NOTE.Created_On_Date__raw,
      us_me_CIS_204_GEN_NOTE.Message_Hi_Rca_Ind,
      us_me_CIS_204_GEN_NOTE.Message_Hi_Risk_Ind,
      us_me_CIS_204_GEN_NOTE.Message_Ind,
      us_me_CIS_204_GEN_NOTE.Message_Lo_Rca_Ind,
      us_me_CIS_204_GEN_NOTE.Message_Lo_Risk_Ind,
      us_me_CIS_204_GEN_NOTE.Modified_By_Tx,
      us_me_CIS_204_GEN_NOTE.Modified_On_Date__raw,
      us_me_CIS_204_GEN_NOTE.Note_Date__raw,
      us_me_CIS_204_GEN_NOTE.Note_Id,
      us_me_CIS_204_GEN_NOTE.Note_Tx,
      us_me_CIS_204_GEN_NOTE.Running_Rpt_Ind,
      us_me_CIS_204_GEN_NOTE.Short_Note_Tx,
      us_me_CIS_204_GEN_NOTE.Violation_Ind,
      us_me_CIS_204_GEN_NOTE.file_id,
      us_me_CIS_204_GEN_NOTE.is_deleted]
    sorts: [us_me_CIS_204_GEN_NOTE.Created_On_Date__raw]
    note_display: hover
    note_text: "This table lists contact notes and details recorded for supervision clients by supervising officers."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 78
    col: 0
    width: 24
    height: 6

  - name: CIS_210_JOB_ASSIGN
    title: CIS_210_JOB_ASSIGN
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_210_JOB_ASSIGN.primary_key,
      us_me_CIS_210_JOB_ASSIGN.ASSIGN_ID,
      us_me_CIS_210_JOB_ASSIGN.START_DATE__raw,
      us_me_CIS_210_JOB_ASSIGN.END_DATE__raw,
      us_me_CIS_210_JOB_ASSIGN.HOLD_IND,
      us_me_CIS_210_JOB_ASSIGN.FIRED_IND,
      us_me_CIS_210_JOB_ASSIGN.NOTES_TX,
      us_me_CIS_210_JOB_ASSIGN.SECURITY_IND,
      us_me_CIS_210_JOB_ASSIGN.KEEP_SEP_IND,
      us_me_CIS_210_JOB_ASSIGN.SEX_OFFENDER_IND,
      us_me_CIS_210_JOB_ASSIGN.JOB_RESTRIC_IND,
      us_me_CIS_210_JOB_ASSIGN.CASE_PLAN_IND,
      us_me_CIS_210_JOB_ASSIGN.OCCURRENCES_NUM,
      us_me_CIS_210_JOB_ASSIGN.SUNDAY_IND,
      us_me_CIS_210_JOB_ASSIGN.MONDAY_IND,
      us_me_CIS_210_JOB_ASSIGN.TUESDAY_IND,
      us_me_CIS_210_JOB_ASSIGN.WEDNESDAY_IND,
      us_me_CIS_210_JOB_ASSIGN.THURS_IND,
      us_me_CIS_210_JOB_ASSIGN.FRIDAY_IND,
      us_me_CIS_210_JOB_ASSIGN.SATURDAY_IND,
      us_me_CIS_210_JOB_ASSIGN.CONFLICT_TX,
      us_me_CIS_210_JOB_ASSIGN.CIS_208_JOB_ID,
      us_me_CIS_210_JOB_ASSIGN.CIS_2083_20831_TYPE_CD,
      us_me_CIS_210_JOB_ASSIGN.CIS_100_CLIENT_ID,
      us_me_CIS_210_JOB_ASSIGN.LOGICAL_DELETE_IND,
      us_me_CIS_210_JOB_ASSIGN.CREATED_BY_TX,
      us_me_CIS_210_JOB_ASSIGN.CREATED_ON_DATE__raw,
      us_me_CIS_210_JOB_ASSIGN.MODIFIED_BY_TX,
      us_me_CIS_210_JOB_ASSIGN.MODIFIED_ON_DATE__raw,
      us_me_CIS_210_JOB_ASSIGN.file_id,
      us_me_CIS_210_JOB_ASSIGN.is_deleted]
    sorts: [us_me_CIS_210_JOB_ASSIGN.START_DATE__raw]
    note_display: hover
    note_text: "TODO(#23225): Fill in the file description"
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 84
    col: 0
    width: 24
    height: 6

  - name: CIS_215_SEX_OFFENDER_ASSESS
    title: CIS_215_SEX_OFFENDER_ASSESS
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_215_SEX_OFFENDER_ASSESS.primary_key,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Assessment_Date__raw,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Assessment_Notes_Tx,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Assessment_Score_Num,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Cis_100_Client_Id,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Cis_1161_Lsi_Effct_Date__raw,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Cis_1161_Lsi_Effct_Over_Date__raw,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Cis_1161_Lsi_Rating_Cd,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Cis_1161_Lsi_Rating_Over_Cd,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Cis_2150_Assess_Type_Cd,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Cis_900_Employee_Id,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Created_By_Tx,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Created_On_Date__raw,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Modified_By_Tx,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Modified_On_Date__raw,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Notes_Tx,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Override_Ind,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.Sex_Offender_Assess_Id,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.file_id,
      us_me_CIS_215_SEX_OFFENDER_ASSESS.is_deleted]
    sorts: [us_me_CIS_215_SEX_OFFENDER_ASSESS.Assessment_Date__raw]
    note_display: hover
    note_text: "This table lists non-LSIR assessment results for a client. LSI assessments are found in CIS_116_LSI_HISTORY. The fact that this table is named for sex offense assessments is historical and no longer accurate. This includes both sex offense assessments and others. Nevertheless, there are columns that reference LSI ratings in this table, which appear to correspond to some sort of proxy rating for whatever assessment a given row in the file refers to, i.e. the results of these assessments are mapped to an LSI rating for normalized comparison against LSI assessments per MEDOC business logic."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 90
    col: 0
    width: 24
    height: 6

  - name: CIS_300_Personal_Property
    title: CIS_300_Personal_Property
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_300_Personal_Property.primary_key,
      us_me_CIS_300_Personal_Property.PP_ID,
      us_me_CIS_300_Personal_Property.ENTRY_DATE__raw,
      us_me_CIS_300_Personal_Property.DESCRIPTION_TX,
      us_me_CIS_300_Personal_Property.THIRD_PARTY_NAME,
      us_me_CIS_300_Personal_Property.MESSAGE_IND,
      us_me_CIS_300_Personal_Property.SERIAL_NUM_TX,
      us_me_CIS_300_Personal_Property.CIS_522_SALE_DTL_ID,
      us_me_CIS_300_Personal_Property.CIS_525_NONSTOCK_DTL_ID,
      us_me_CIS_300_Personal_Property.CIS_100_CLIENT_ID,
      us_me_CIS_300_Personal_Property.CIS_301_ADMISSION_ID,
      us_me_CIS_300_Personal_Property.CIS_3180_PP_TO_FROM_CD,
      us_me_CIS_300_Personal_Property.CIS_3030_PP_ITEM_TYPE_CD,
      us_me_CIS_300_Personal_Property.CIS_908_CCS_LOCATION_ID,
      us_me_CIS_300_Personal_Property.CIS_900_EMPLOYEE_ID,
      us_me_CIS_300_Personal_Property.CREATED_BY_TX,
      us_me_CIS_300_Personal_Property.CREATED_ON_DATE__raw,
      us_me_CIS_300_Personal_Property.MODIFIED_BY_TX,
      us_me_CIS_300_Personal_Property.MODIFIED_ON_DATE__raw,
      us_me_CIS_300_Personal_Property.file_id,
      us_me_CIS_300_Personal_Property.is_deleted]
    sorts: [us_me_CIS_300_Personal_Property.ENTRY_DATE__raw]
    note_display: hover
    note_text: "TODO(#23225): Fill in the file description"
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 96
    col: 0
    width: 24
    height: 6

  - name: CIS_309_MOVEMENT
    title: CIS_309_MOVEMENT
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_309_MOVEMENT.primary_key,
      us_me_CIS_309_MOVEMENT.Actual_Time,
      us_me_CIS_309_MOVEMENT.Cis_3090_Movement_Type_Cd,
      us_me_CIS_309_MOVEMENT.Cis_3092_Transport_Type_Cd,
      us_me_CIS_309_MOVEMENT.Cis_3093_Mvmt_Status_Cd,
      us_me_CIS_309_MOVEMENT.Cis_3095_Mvmt_Direction_Cd,
      us_me_CIS_309_MOVEMENT.Cis_310_Remand_Id,
      us_me_CIS_309_MOVEMENT.Cis_314_Transfer_Id,
      us_me_CIS_309_MOVEMENT.Cis_317_Ta_Tr_Days_Id,
      us_me_CIS_309_MOVEMENT.Cis_319_Term_Id,
      us_me_CIS_309_MOVEMENT.Cis_322_Escorted_Leave_Id,
      us_me_CIS_309_MOVEMENT.Cis_324_Awol_Id,
      us_me_CIS_309_MOVEMENT.Cis_908_Ccs_Location_2_Id,
      us_me_CIS_309_MOVEMENT.Cis_908_Ccs_Location_Id,
      us_me_CIS_309_MOVEMENT.Cis_Client_Id,
      us_me_CIS_309_MOVEMENT.Created_By_Tx,
      us_me_CIS_309_MOVEMENT.Created_On_Date,
      us_me_CIS_309_MOVEMENT.Estimated_Time,
      us_me_CIS_309_MOVEMENT.Modified_By_Tx,
      us_me_CIS_309_MOVEMENT.Modified_On_Date,
      us_me_CIS_309_MOVEMENT.Movement_Date__raw,
      us_me_CIS_309_MOVEMENT.Movement_Id,
      us_me_CIS_309_MOVEMENT.file_id,
      us_me_CIS_309_MOVEMENT.is_deleted]
    sorts: [us_me_CIS_309_MOVEMENT.Movement_Date__raw]
    note_display: hover
    note_text: "This table records movements to/from and between DOC facilities. It includes information about the movement type, status and direction. Movement types could refer to furloughs, admissions, transfers, releases to supervision, discharges to society, transports and escapes. The movement status refers to whether the movement is Pending, In Transit, Complete, or Cancelled. When a person is admitted to a DOC Facility, a movement record is auto-generated with a \"Pending\" status for their discharge or release date. When a person is released to supervision, two records are auto-generated: one with the movement type of \"Release\" from the facility and one with the movement type of \"Transfer\" to the supervision location. Transport movement types are when a person is being escorted to a location for the day either for work or medical reasons. When a movement is related to transfers or intakes (Sentence/Disposition, Transfer), a related record is generated in the CIS_314_TRANSFER table. Movement types of Furlough, Escape, Transport, Discharge, and Release do not generate transfer records."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 102
    col: 0
    width: 24
    height: 6

  - name: CIS_314_TRANSFER
    title: CIS_314_TRANSFER
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_314_TRANSFER.primary_key,
      us_me_CIS_314_TRANSFER.Cancelled_Ind,
      us_me_CIS_314_TRANSFER.Cancelled_Reason_Tx,
      us_me_CIS_314_TRANSFER.Cis_100_Client_Id,
      us_me_CIS_314_TRANSFER.Cis_3140_Transfer_Type_Cd,
      us_me_CIS_314_TRANSFER.Cis_3141_Transfer_Reason_Cd,
      us_me_CIS_314_TRANSFER.Cis_3142_Tran_Typ_Rsn_Cd,
      us_me_CIS_314_TRANSFER.Cis_319_Term_Cust_Id,
      us_me_CIS_314_TRANSFER.Cis_900_Employee_Id,
      us_me_CIS_314_TRANSFER.Cis_900_Weekly_Tfr_Employee_Id,
      us_me_CIS_314_TRANSFER.Cis_908_Ccs_Loc_Jur_From_Id,
      us_me_CIS_314_TRANSFER.Cis_908_Ccs_Loc_Jur_To_Id,
      us_me_CIS_314_TRANSFER.Cis_908_Ccs_Location_Frm_Id,
      us_me_CIS_314_TRANSFER.Cis_908_Ccs_Location_To_Id,
      us_me_CIS_314_TRANSFER.Cis_912_Unit_Id,
      us_me_CIS_314_TRANSFER.Created_By_Tx,
      us_me_CIS_314_TRANSFER.Created_On_Date__raw,
      us_me_CIS_314_TRANSFER.Hearing_Date,
      us_me_CIS_314_TRANSFER.Keep_Separate_Tx,
      us_me_CIS_314_TRANSFER.Location_Tx,
      us_me_CIS_314_TRANSFER.Modified_By_Tx,
      us_me_CIS_314_TRANSFER.Modified_On_Date__raw,
      us_me_CIS_314_TRANSFER.Notes_Tx,
      us_me_CIS_314_TRANSFER.Officer_Name,
      us_me_CIS_314_TRANSFER.Phone_Tx,
      us_me_CIS_314_TRANSFER.Recommended_Date,
      us_me_CIS_314_TRANSFER.Transfer_Date__raw,
      us_me_CIS_314_TRANSFER.Transfer_Id,
      us_me_CIS_314_TRANSFER.file_id,
      us_me_CIS_314_TRANSFER.is_deleted]
    sorts: [us_me_CIS_314_TRANSFER.Created_On_Date__raw]
    note_display: hover
    note_text: "This table records transfers in and out of of DOC facilities and supervision locations. Transfers related to DOC Facilities will also have a corresponding record in the CIS_309_MOVEMENT table."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 108
    col: 0
    width: 24
    height: 6

  - name: CIS_319_TERM
    title: CIS_319_TERM
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_319_TERM.primary_key,
      us_me_CIS_319_TERM.Cis_100_Client_Id,
      us_me_CIS_319_TERM.Cis_1200_Term_Status_Cd,
      us_me_CIS_319_TERM.Comm_Rel_Date__raw,
      us_me_CIS_319_TERM.Created_By_Tx,
      us_me_CIS_319_TERM.Created_On_Date__raw,
      us_me_CIS_319_TERM.Curr_Cust_Rel_Date__raw,
      us_me_CIS_319_TERM.Det_Release_Rsn_Tx,
      us_me_CIS_319_TERM.Detainers_Not_Ind,
      us_me_CIS_319_TERM.Detainers_Not_Tx,
      us_me_CIS_319_TERM.Early_Cust_Rel_Date__raw,
      us_me_CIS_319_TERM.Intake_Date__raw,
      us_me_CIS_319_TERM.Max_Cust_Rel_Date__raw,
      us_me_CIS_319_TERM.Message_30_Ind,
      us_me_CIS_319_TERM.Message_Ind,
      us_me_CIS_319_TERM.Modified_By_Tx,
      us_me_CIS_319_TERM.Modified_On_Date__raw,
      us_me_CIS_319_TERM.Rpt_Comm_Rel_Date,
      us_me_CIS_319_TERM.Rpt_Curr_Cust_Rel_Date,
      us_me_CIS_319_TERM.Rpt_Early_Cust_Rel_Date,
      us_me_CIS_319_TERM.Rpt_Max_Cust_Rel_Date,
      us_me_CIS_319_TERM.Term_Id,
      us_me_CIS_319_TERM.file_id,
      us_me_CIS_319_TERM.is_deleted]
    sorts: [us_me_CIS_319_TERM.Comm_Rel_Date__raw]
    note_display: hover
    note_text: "A client is entered into the DOC on a \"Term\" that can include multiple charges or dispositions. This table shows for each term that a client has with the DOC: their intake date, max release date, and earliest release date, with all sentences/dispositions taken into account. Then it shows the status of whether the \"Term\" is still active or if it is complete. \"Safekeepers\" and \"boarders\" will not have a Term because they do not have a DOC sentence, though their status will be listed as incarcerated. Juveniles also may not have an associated term. A new \"Society In\" transfer record would generate a new Term record. This table is updated daily to reflect new early and current release dates that are affected by good time earned, lost, and restored."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 114
    col: 0
    width: 24
    height: 6

  - name: CIS_324_AWOL
    title: CIS_324_AWOL
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_324_AWOL.primary_key,
      us_me_CIS_324_AWOL.AWOL_ID,
      us_me_CIS_324_AWOL.CIS_100_CLIENT_ID,
      us_me_CIS_324_AWOL.CIS_319_TERM_ID,
      us_me_CIS_324_AWOL.CIS_3240_AWOL_TYPE_CD,
      us_me_CIS_324_AWOL.CIS_908_ESCAPED_FROM_ID,
      us_me_CIS_324_AWOL.CIS_908_RETURNED_TO_ID,
      us_me_CIS_324_AWOL.AWOL_DATE__raw,
      us_me_CIS_324_AWOL.AWOL_TIME__raw,
      us_me_CIS_324_AWOL.INEFFCT_DATE__raw,
      us_me_CIS_324_AWOL.RETURN_DATE__raw,
      us_me_CIS_324_AWOL.RETURN_TIME__raw,
      us_me_CIS_324_AWOL.DAYS_UAL_NUM,
      us_me_CIS_324_AWOL.NOTES_TX,
      us_me_CIS_324_AWOL.CLOTHING_TX,
      us_me_CIS_324_AWOL.CONTACTS_TX,
      us_me_CIS_324_AWOL.CREATED_BY_TX,
      us_me_CIS_324_AWOL.CREATED_ON_DATE,
      us_me_CIS_324_AWOL.MODIFIED_BY_TX,
      us_me_CIS_324_AWOL.MODIFIED_ON_DATE,
      us_me_CIS_324_AWOL.file_id,
      us_me_CIS_324_AWOL.is_deleted]
    sorts: [us_me_CIS_324_AWOL.AWOL_DATE__raw]
    note_display: hover
    note_text: "This table lists all escapes from DOC Facilities. It also includes escapes from community confinement (SCCP) and Parole."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 120
    col: 0
    width: 24
    height: 6

  - name: CIS_400_CHARGE
    title: CIS_400_CHARGE
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_400_CHARGE.primary_key,
      us_me_CIS_400_CHARGE.Atn_Tx,
      us_me_CIS_400_CHARGE.Bind_Over_Result_Tx,
      us_me_CIS_400_CHARGE.Charge_Id,
      us_me_CIS_400_CHARGE.Charge_Outcome_Date,
      us_me_CIS_400_CHARGE.Charge_Outcome_Notes_Tx,
      us_me_CIS_400_CHARGE.Cis_100_Client_Id,
      us_me_CIS_400_CHARGE.Cis_4000_Charge_Outcome_Cd,
      us_me_CIS_400_CHARGE.Cis_4002_Plea_Type_Cd,
      us_me_CIS_400_CHARGE.Cis_4003_Offence_Type_Cd,
      us_me_CIS_400_CHARGE.Cis_4003_Pet_Offence_Type_Cd,
      us_me_CIS_400_CHARGE.Cis_4004_Charge_Mode_Cd,
      us_me_CIS_400_CHARGE.Cis_4006_Victim_Relation_Cd,
      us_me_CIS_400_CHARGE.Cis_4009_Det_Status_Cd,
      us_me_CIS_400_CHARGE.Cis_4009_Rel_Status_Cd,
      us_me_CIS_400_CHARGE.Cis_4009_Type_Offense_Cd,
      us_me_CIS_400_CHARGE.Cis_4020_Offense_Class_Cd,
      us_me_CIS_400_CHARGE.Cis_4020_Pet_Class_Cd,
      us_me_CIS_400_CHARGE.Cis_9003_Jurisd_County_Cd,
      us_me_CIS_400_CHARGE.Cis_9901_Police_Dept_Cd,
      us_me_CIS_400_CHARGE.Comments_Tx,
      us_me_CIS_400_CHARGE.Created_By_Tx,
      us_me_CIS_400_CHARGE.Created_On_Date,
      us_me_CIS_400_CHARGE.Ctn_Tx,
      us_me_CIS_400_CHARGE.Detention_Req_Date,
      us_me_CIS_400_CHARGE.Drugs_Alc_Inv_Ind,
      us_me_CIS_400_CHARGE.Interstate_Comp_In_Ind,
      us_me_CIS_400_CHARGE.Juvenile_Ind,
      us_me_CIS_400_CHARGE.Modified_By_Tx,
      us_me_CIS_400_CHARGE.Modified_On_Date,
      us_me_CIS_400_CHARGE.Offense_Date__raw,
      us_me_CIS_400_CHARGE.Offense_Note_Tx,
      us_me_CIS_400_CHARGE.Oos_Inv_Compl_Date,
      us_me_CIS_400_CHARGE.Oos_Inv_Req_Date,
      us_me_CIS_400_CHARGE.Petition_Outcome_Tx,
      us_me_CIS_400_CHARGE.Petition_Req_Date,
      us_me_CIS_400_CHARGE.Petition_Req_Ind,
      us_me_CIS_400_CHARGE.Police_Officer_Tx,
      us_me_CIS_400_CHARGE.Police_Rep_No_Tx,
      us_me_CIS_400_CHARGE.Referral_Date__raw,
      us_me_CIS_400_CHARGE.Summons_Date,
      us_me_CIS_400_CHARGE.ag_case_ind,
      us_me_CIS_400_CHARGE.sealed_juvenile_ind,
      us_me_CIS_400_CHARGE.sealed_record_comments_tx,
      us_me_CIS_400_CHARGE.file_id,
      us_me_CIS_400_CHARGE.is_deleted]
    sorts: [us_me_CIS_400_CHARGE.Offense_Date__raw]
    note_display: hover
    note_text: "This table lists all of the charges related to a single client."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 126
    col: 0
    width: 24
    height: 6

  - name: CIS_401_CRT_ORDER_HDR
    title: CIS_401_CRT_ORDER_HDR
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_401_CRT_ORDER_HDR.primary_key,
      us_me_CIS_401_CRT_ORDER_HDR.Adj_Days_Earned_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Adj_Days_Lost_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Adj_Days_Restored_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Amended_Comments_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Amended_Date,
      us_me_CIS_401_CRT_ORDER_HDR.Apply_Det_Days_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_319_Term_Id,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_400_Charge_Id,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_400_Cis_100_Client_Id,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_4003_Offence_Type_Cd,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_4009_Comm_Override_Rsn_Cd,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_4009_Cons_Conc_Cd,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_4009_Cust_Override_Rsn_Cd,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_4009_Sex_Offense_Cd,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_4009_Trial_Type_Cd,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_401_Court_Order_Id,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_4010_Crt_Order_Status_Cd,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_4012_Crt_Order_Type_Cd,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_4020_Offns_Class_Cd,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_404_Docket_Id,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_9003_Cnty_Jail_Snt_Cd,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_908_Ccs_Location_Id,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_9902_Court_Cd,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_9904_Judge_Prof_Id,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_9910_Def_Att_Id,
      us_me_CIS_401_CRT_ORDER_HDR.Cis_9910_Pros_Att_Id,
      us_me_CIS_401_CRT_ORDER_HDR.Comm_Override_Notes_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Comm_Override_Rel_Date__raw,
      us_me_CIS_401_CRT_ORDER_HDR.Comm_Override_Rel_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Comm_Rel_Date__raw,
      us_me_CIS_401_CRT_ORDER_HDR.Comments_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Count_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Court_Finding_Date__raw,
      us_me_CIS_401_CRT_ORDER_HDR.Court_Order_Date,
      us_me_CIS_401_CRT_ORDER_HDR.Court_Order_Id,
      us_me_CIS_401_CRT_ORDER_HDR.Court_Order_Sent_Date__raw,
      us_me_CIS_401_CRT_ORDER_HDR.Created_By_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Created_On_Date__raw,
      us_me_CIS_401_CRT_ORDER_HDR.Csw_Comments_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Csw_Due_Date,
      us_me_CIS_401_CRT_ORDER_HDR.Csw_Hours_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Csw_Sus_Hours_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Csw_Total_Hours_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Curr_Cust_Rel_Date__raw,
      us_me_CIS_401_CRT_ORDER_HDR.Cust_Override_Notes_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Cust_Override_Rel_Date__raw,
      us_me_CIS_401_CRT_ORDER_HDR.Cust_Override_Rel_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Days_Earned_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Days_Lost_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Days_Served_Det_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Days_Served_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Detention_Conduct_Days_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Detention_Days_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Dhs_Cust_Age_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Domestic_Violence_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Early_Cust_Rel_Date__raw,
      us_me_CIS_401_CRT_ORDER_HDR.Est_Sent_Start_Date__raw,
      us_me_CIS_401_CRT_ORDER_HDR.Fine_Amount_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Fine_Comments_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Fine_Due_Date,
      us_me_CIS_401_CRT_ORDER_HDR.Fine_Paid_Date,
      us_me_CIS_401_CRT_ORDER_HDR.Fine_Paid_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Fine_Paid_Not_By_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Fine_Pmt_Schd_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Fine_Sus_Amount_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Fine_Total_Amount_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Guardian_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Life_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Max_Cust_Rel_Date__raw,
      us_me_CIS_401_CRT_ORDER_HDR.Modified_By_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Modified_On_Date__raw,
      us_me_CIS_401_CRT_ORDER_HDR.Other_Court_Desc_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Pre_Disp_Crt_App_Date,
      us_me_CIS_401_CRT_ORDER_HDR.Pre_Sent_Rpt_Ord_Date,
      us_me_CIS_401_CRT_ORDER_HDR.Pre_Sent_Rpt_Sub_Date,
      us_me_CIS_401_CRT_ORDER_HDR.Prob_Consecutive_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Prob_Days_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Prob_Mths_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Prob_Start_Date__raw,
      us_me_CIS_401_CRT_ORDER_HDR.Prob_Yrs_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Probation_Term_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Process_Adjustment_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Resentence_Comments_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Resentence_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Rest_Amount_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Rest_Comments_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Rest_Cond_Prob_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Rest_Due_Date,
      us_me_CIS_401_CRT_ORDER_HDR.Rest_Paid_Date,
      us_me_CIS_401_CRT_ORDER_HDR.Rest_Paid_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Rest_Paid_Not_By_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Rest_Pmt_Schd_Tx,
      us_me_CIS_401_CRT_ORDER_HDR.Revocation_Age_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Revocation_Days_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Revocation_Mths_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Revocation_Yrs_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Serve_Days_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Serve_Mths_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Serve_Yrs_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Shock_Prob_Days_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Shock_Prob_Mths_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Shock_Prob_Yrs_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Shock_Serve_Days_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Shock_Stc_Days_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Statute_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Stayed_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.Stc_Days_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Stc_Mths_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Stc_To_Age_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Stc_Yrs_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Victim_Comp_Fee_Num,
      us_me_CIS_401_CRT_ORDER_HDR.Weekend_Holiday_Ind,
      us_me_CIS_401_CRT_ORDER_HDR.file_id,
      us_me_CIS_401_CRT_ORDER_HDR.is_deleted]
    sorts: [us_me_CIS_401_CRT_ORDER_HDR.Comm_Override_Rel_Date__raw]
    note_display: hover
    note_text: "This table includes information related to the client's sentence and is related to the CIS_319_TERM table. A term can have many sentences. The CIS_401_CRT_ORDER_HDR table is updated to include days earned or lost by time spent at the county jail awaiting sentencing, which then affects the early and current expected release dates on the term. This table also joins to the CIS_409_CRT_ORDER_CNDTION_HDR table and links a client to the conditions  imposed on their supervision sentence. The conditions are found in the CIS_403_CONDITION table."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 132
    col: 0
    width: 24
    height: 6

  - name: CIS_425_MAIN_PROG
    title: CIS_425_MAIN_PROG
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_425_MAIN_PROG.primary_key,
      us_me_CIS_425_MAIN_PROG.ENROLL_ID,
      us_me_CIS_425_MAIN_PROG.CIS_100_CLIENT_ID,
      us_me_CIS_425_MAIN_PROG.TERM_END_DATE__raw,
      us_me_CIS_425_MAIN_PROG.LSI_DATE__raw,
      us_me_CIS_425_MAIN_PROG.LSI_SCORE_NUM,
      us_me_CIS_425_MAIN_PROG.NOTES_TX,
      us_me_CIS_425_MAIN_PROG.UNADMIN_TX,
      us_me_CIS_425_MAIN_PROG.UNADMINISTER_IND,
      us_me_CIS_425_MAIN_PROG.COURT_ORDER_IND,
      us_me_CIS_425_MAIN_PROG.CONDITION_IND,
      us_me_CIS_425_MAIN_PROG.CASE_PLAN_IND,
      us_me_CIS_425_MAIN_PROG.KEEP_SEPARATE_IND,
      us_me_CIS_425_MAIN_PROG.KS_OVERRIDE_TX,
      us_me_CIS_425_MAIN_PROG.NEXT_SCHED_IND,
      us_me_CIS_425_MAIN_PROG.CIS_9900_STAT_TYPE_CD,
      us_me_CIS_425_MAIN_PROG.CIS_420_PROGRAM_ID,
      us_me_CIS_425_MAIN_PROG.CIS_900_EMPLOYEE_ID,
      us_me_CIS_425_MAIN_PROG.CIS_908_CCS_LOCATION_ID,
      us_me_CIS_425_MAIN_PROG.CREATED_BY_TX,
      us_me_CIS_425_MAIN_PROG.CREATED_ON_DATE,
      us_me_CIS_425_MAIN_PROG.MODIFIED_BY_TX,
      us_me_CIS_425_MAIN_PROG.MODIFIED_ON_DATE,
      us_me_CIS_425_MAIN_PROG.file_id,
      us_me_CIS_425_MAIN_PROG.is_deleted]
    sorts: [us_me_CIS_425_MAIN_PROG.TERM_END_DATE__raw]
    note_display: hover
    note_text: "This table lists the programs that a client is enrolled in."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 138
    col: 0
    width: 24
    height: 6

  - name: CIS_430_ASSESSMENT
    title: CIS_430_ASSESSMENT
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_430_ASSESSMENT.primary_key,
      us_me_CIS_430_ASSESSMENT.Assess_Id,
      us_me_CIS_430_ASSESSMENT.Assess_Reason_Tx,
      us_me_CIS_430_ASSESSMENT.Assessor_Notes_Tx,
      us_me_CIS_430_ASSESSMENT.Bill_Tx,
      us_me_CIS_430_ASSESSMENT.Cis_100_Client_Id,
      us_me_CIS_430_ASSESSMENT.Cis_4035_Assess_Prog_Type_Cd,
      us_me_CIS_430_ASSESSMENT.Cis_4036_Tool_Type_Cd,
      us_me_CIS_430_ASSESSMENT.Cis_4302_Request_Type_Cd,
      us_me_CIS_430_ASSESSMENT.Cis_4303_Degree_Cd,
      us_me_CIS_430_ASSESSMENT.Cis_4305_Recommendation_Action_Cd,
      us_me_CIS_430_ASSESSMENT.Cis_450_Agency_Id,
      us_me_CIS_430_ASSESSMENT.Cis_451_Contact_Id,
      us_me_CIS_430_ASSESSMENT.Cis_900_Employee_2_Id,
      us_me_CIS_430_ASSESSMENT.Cis_900_Employee_Id,
      us_me_CIS_430_ASSESSMENT.Cis_9902_Court_Cd,
      us_me_CIS_430_ASSESSMENT.Cis_9904_Proffesional_Cd,
      us_me_CIS_430_ASSESSMENT.Court_Ind,
      us_me_CIS_430_ASSESSMENT.Created_By_Tx,
      us_me_CIS_430_ASSESSMENT.Created_On_Date,
      us_me_CIS_430_ASSESSMENT.Dead_Line_Date__raw,
      us_me_CIS_430_ASSESSMENT.Education_Ind,
      us_me_CIS_430_ASSESSMENT.Ext_Int_Assess_Rb,
      us_me_CIS_430_ASSESSMENT.Intake_Tx,
      us_me_CIS_430_ASSESSMENT.Interview_Date__raw,
      us_me_CIS_430_ASSESSMENT.Logical_Delete_Ind,
      us_me_CIS_430_ASSESSMENT.Modified_By_Tx,
      us_me_CIS_430_ASSESSMENT.Modified_On_Date,
      us_me_CIS_430_ASSESSMENT.Other_Request_Tx,
      us_me_CIS_430_ASSESSMENT.Other_Type_Tx,
      us_me_CIS_430_ASSESSMENT.Result_Tx,
      us_me_CIS_430_ASSESSMENT.Score_Tx,
      us_me_CIS_430_ASSESSMENT.Start_Date__raw,
      us_me_CIS_430_ASSESSMENT.file_id,
      us_me_CIS_430_ASSESSMENT.is_deleted]
    sorts: [us_me_CIS_430_ASSESSMENT.Dead_Line_Date__raw]
    note_display: hover
    note_text: "This table includes all of the assessments that a client has completed."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 144
    col: 0
    width: 24
    height: 6

  - name: CIS_462_CLIENTS_INVOLVED
    title: CIS_462_CLIENTS_INVOLVED
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_462_CLIENTS_INVOLVED.primary_key,
      us_me_CIS_462_CLIENTS_INVOLVED.Cis_100_Client_Id,
      us_me_CIS_462_CLIENTS_INVOLVED.Cis_460_Incident_Id,
      us_me_CIS_462_CLIENTS_INVOLVED.Cis_460_Year_Id,
      us_me_CIS_462_CLIENTS_INVOLVED.Cis_9907_Injury_Cd,
      us_me_CIS_462_CLIENTS_INVOLVED.Cis_9907_Prea_Role_Client_Cd,
      us_me_CIS_462_CLIENTS_INVOLVED.Cis_9907_Restraint_Cd,
      us_me_CIS_462_CLIENTS_INVOLVED.Cis_9907_Restraint2_Cd,
      us_me_CIS_462_CLIENTS_INVOLVED.Cis_9907_Restraint3_Cd,
      us_me_CIS_462_CLIENTS_INVOLVED.Cis_9909_Action_Cd,
      us_me_CIS_462_CLIENTS_INVOLVED.Clients_Involved_Id,
      us_me_CIS_462_CLIENTS_INVOLVED.Comment_Tx,
      us_me_CIS_462_CLIENTS_INVOLVED.Created_By_Tx,
      us_me_CIS_462_CLIENTS_INVOLVED.Created_On_Date,
      us_me_CIS_462_CLIENTS_INVOLVED.Injury_Date,
      us_me_CIS_462_CLIENTS_INVOLVED.Logical_Delete_Ind,
      us_me_CIS_462_CLIENTS_INVOLVED.Medical_Ind,
      us_me_CIS_462_CLIENTS_INVOLVED.Mental_Health_Ind,
      us_me_CIS_462_CLIENTS_INVOLVED.Modified_By_Tx,
      us_me_CIS_462_CLIENTS_INVOLVED.Modified_On_Date,
      us_me_CIS_462_CLIENTS_INVOLVED.Type_Force_Used_Cd,
      us_me_CIS_462_CLIENTS_INVOLVED.file_id,
      us_me_CIS_462_CLIENTS_INVOLVED.is_deleted]
    sorts: [us_me_CIS_462_CLIENTS_INVOLVED.Clients_Involved_Id]
    note_display: hover
    note_text: "This table lists the clients involved in an incident and has additional notes about the incident and the client."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 150
    col: 0
    width: 24
    height: 6

  - name: CIS_480_VIOLATION
    title: CIS_480_VIOLATION
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_480_VIOLATION.primary_key,
      us_me_CIS_480_VIOLATION.Arrested_Warr_Date__raw,
      us_me_CIS_480_VIOLATION.Cis_100_Client_Id,
      us_me_CIS_480_VIOLATION.Cis_1016_Extradition_Cd,
      us_me_CIS_480_VIOLATION.Cis_4009_Client_Loc_Cd,
      us_me_CIS_480_VIOLATION.Cis_4009_Disposition_Cd,
      us_me_CIS_480_VIOLATION.Cis_4009_Served_At_Cd,
      us_me_CIS_480_VIOLATION.Cis_4009_Supervision_Type_Cd,
      us_me_CIS_480_VIOLATION.Cis_4009_Toll_Violation_Cd,
      us_me_CIS_480_VIOLATION.Cis_4009_Violation_Type_Cd,
      us_me_CIS_480_VIOLATION.Cis_4800_Violation_Finding_Cd,
      us_me_CIS_480_VIOLATION.Cis_900_Employee_Id,
      us_me_CIS_480_VIOLATION.Created_By_Tx,
      us_me_CIS_480_VIOLATION.Created_On_Date,
      us_me_CIS_480_VIOLATION.Extradition_Other_Tx,
      us_me_CIS_480_VIOLATION.Finding_Date__raw,
      us_me_CIS_480_VIOLATION.Finding_Notes_Tx,
      us_me_CIS_480_VIOLATION.Fta_Date__raw,
      us_me_CIS_480_VIOLATION.Fta_Ind,
      us_me_CIS_480_VIOLATION.Logical_Delete_Ind,
      us_me_CIS_480_VIOLATION.Modified_By_Tx,
      us_me_CIS_480_VIOLATION.Modified_On_Date,
      us_me_CIS_480_VIOLATION.Start_Date__raw,
      us_me_CIS_480_VIOLATION.Stayed_Ind,
      us_me_CIS_480_VIOLATION.Toll_End_Date__raw,
      us_me_CIS_480_VIOLATION.Toll_Start_Date__raw,
      us_me_CIS_480_VIOLATION.Violation_Descr_Tx,
      us_me_CIS_480_VIOLATION.Violation_Id,
      us_me_CIS_480_VIOLATION.Warning_Issued_Ind,
      us_me_CIS_480_VIOLATION.Serve_Days,
      us_me_CIS_480_VIOLATION.Serve_Mths,
      us_me_CIS_480_VIOLATION.Serve_Yrs,
      us_me_CIS_480_VIOLATION.file_id,
      us_me_CIS_480_VIOLATION.is_deleted]
    sorts: [us_me_CIS_480_VIOLATION.Arrested_Warr_Date__raw]
    note_display: hover
    note_text: "This table lists violations that occurred while someone was on community supervision. If a client was on SCCP (Supervised Community Confinement Program), the supervising officer might enter the violation info in either this table or in CIS_460_INCIDENTS, since they are still considered to be part of the incarcerated population."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 156
    col: 0
    width: 24
    height: 6

  - name: CIS_573_CLIENT_CASE_DETAIL
    title: CIS_573_CLIENT_CASE_DETAIL
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_573_CLIENT_CASE_DETAIL.primary_key,
      us_me_CIS_573_CLIENT_CASE_DETAIL.cis_100_client_id,
      us_me_CIS_573_CLIENT_CASE_DETAIL.cis_570_rest_case_id,
      us_me_CIS_573_CLIENT_CASE_DETAIL.cis_5750_disburse_to_other_id,
      us_me_CIS_573_CLIENT_CASE_DETAIL.cis_589_restitution_id,
      us_me_CIS_573_CLIENT_CASE_DETAIL.client_case_id,
      us_me_CIS_573_CLIENT_CASE_DETAIL.comments_tx,
      us_me_CIS_573_CLIENT_CASE_DETAIL.created_by_tx,
      us_me_CIS_573_CLIENT_CASE_DETAIL.created_on_date,
      us_me_CIS_573_CLIENT_CASE_DETAIL.jsc_max_to_receive_num,
      us_me_CIS_573_CLIENT_CASE_DETAIL.jsc_override_dist_ind,
      us_me_CIS_573_CLIENT_CASE_DETAIL.jsc_override_dist_pct,
      us_me_CIS_573_CLIENT_CASE_DETAIL.jsc_override_reason_tx,
      us_me_CIS_573_CLIENT_CASE_DETAIL.modified_by_tx,
      us_me_CIS_573_CLIENT_CASE_DETAIL.modified_on_date,
      us_me_CIS_573_CLIENT_CASE_DETAIL.total_owing_num,
      us_me_CIS_573_CLIENT_CASE_DETAIL.file_id,
      us_me_CIS_573_CLIENT_CASE_DETAIL.is_deleted]
    sorts: [us_me_CIS_573_CLIENT_CASE_DETAIL.client_case_id]
    note_display: hover
    note_text: "This table contains the adjusted restitution amount. It does not take into account any payments."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 162
    col: 0
    width: 24
    height: 6

  - name: CIS_916_ASSIGN_BED
    title: CIS_916_ASSIGN_BED
    explore: us_me_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_me_CIS_916_ASSIGN_BED.primary_key,
      us_me_CIS_916_ASSIGN_BED.Assign_Id,
      us_me_CIS_916_ASSIGN_BED.Cis_100_Client_Id,
      us_me_CIS_916_ASSIGN_BED.Cis_900_Employee_Id,
      us_me_CIS_916_ASSIGN_BED.Cis_9083_9082_Chng_Type_Cd,
      us_me_CIS_916_ASSIGN_BED.Cis_9083_9083_Chng_Rsn_Cd,
      us_me_CIS_916_ASSIGN_BED.Cis_9083_9084_Ooclass_Cd,
      us_me_CIS_916_ASSIGN_BED.Cis_9084_9089_Client_Stat_Cd,
      us_me_CIS_916_ASSIGN_BED.Cis_915_Bed_Id,
      us_me_CIS_916_ASSIGN_BED.Conflict_Tx,
      us_me_CIS_916_ASSIGN_BED.Created_By_Tx,
      us_me_CIS_916_ASSIGN_BED.Created_On_Date,
      us_me_CIS_916_ASSIGN_BED.End_Date__raw,
      us_me_CIS_916_ASSIGN_BED.Hold_Ind,
      us_me_CIS_916_ASSIGN_BED.Keep_Sep_Ind,
      us_me_CIS_916_ASSIGN_BED.Modified_By_Tx,
      us_me_CIS_916_ASSIGN_BED.Modified_On_Date,
      us_me_CIS_916_ASSIGN_BED.Notes_Tx,
      us_me_CIS_916_ASSIGN_BED.Out_Class_Ind,
      us_me_CIS_916_ASSIGN_BED.Override_Prea_Ind,
      us_me_CIS_916_ASSIGN_BED.Single_Bed_Ind,
      us_me_CIS_916_ASSIGN_BED.Start_Date__raw,
      us_me_CIS_916_ASSIGN_BED.Top_Bunk_Ind,
      us_me_CIS_916_ASSIGN_BED.file_id,
      us_me_CIS_916_ASSIGN_BED.is_deleted]
    sorts: [us_me_CIS_916_ASSIGN_BED.End_Date__raw]
    note_display: hover
    note_text: "This table lists a client's assignment to a bed in a DOC facility with a start and end date for that assignment."
    listen: 
      View Type: us_me_CIS_100_CLIENT.view_type
      US_ME_DOC: us_me_CIS_100_CLIENT.Client_Id
    row: 168
    col: 0
    width: 24
    height: 6

