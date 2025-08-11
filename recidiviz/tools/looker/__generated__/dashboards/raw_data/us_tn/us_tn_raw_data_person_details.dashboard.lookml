# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_tn_raw_data_person_details
  title: Tennessee Raw Data Person Details
  description: For examining individuals in US_TN's raw data tables
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
    explore: us_tn_raw_data
    field: us_tn_OffenderName.view_type

  - name: US_TN_DOC
    title: US_TN_DOC
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{project_id}"
    explore: us_tn_raw_data
    field: us_tn_OffenderName.OffenderID

  elements:
  - name: OffenderName
    title: OffenderName
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_OffenderName.primary_key,
      us_tn_OffenderName.OffenderID,
      us_tn_OffenderName.SequenceNumber,
      us_tn_OffenderName.FirstName,
      us_tn_OffenderName.MiddleName,
      us_tn_OffenderName.LastName,
      us_tn_OffenderName.Suffix,
      us_tn_OffenderName.NameType,
      us_tn_OffenderName.OffenderStatus,
      us_tn_OffenderName.ActualSiteID,
      us_tn_OffenderName.Race,
      us_tn_OffenderName.Sex,
      us_tn_OffenderName.BirthDate__raw,
      us_tn_OffenderName.SocialSecurityNumber,
      us_tn_OffenderName.STGNicknameFlag,
      us_tn_OffenderName.LastUpdateUserID,
      us_tn_OffenderName.LastUpdateDate__raw,
      us_tn_OffenderName.file_id,
      us_tn_OffenderName.is_deleted]
    sorts: [us_tn_OffenderName.BirthDate__raw]
    note_display: hover
    note_text: "This entity stores a record for each alias (name) or nickname for people."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 0
    col: 0
    width: 24
    height: 6

  - name: Address
    title: Address
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_Address.primary_key,
      us_tn_Address.PersonID,
      us_tn_Address.AddressLine1,
      us_tn_Address.AddressLine2,
      us_tn_Address.AddressCity,
      us_tn_Address.AddressState,
      us_tn_Address.AddressZip,
      us_tn_Address.PhoneNumber,
      us_tn_Address.PhoneNumberType,
      us_tn_Address.AlternatePhoneNumber,
      us_tn_Address.LastUpdateUserID,
      us_tn_Address.LastUpdateDate__raw,
      us_tn_Address.file_id,
      us_tn_Address.is_deleted]
    sorts: [us_tn_Address.LastUpdateDate__raw]
    note_display: hover
    note_text: "This table contains the address information for any person in the TOMIS system."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 6
    col: 0
    width: 24
    height: 6

  - name: AssignedStaff
    title: AssignedStaff
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_AssignedStaff.primary_key,
      us_tn_AssignedStaff.StaffID,
      us_tn_AssignedStaff.OffenderID,
      us_tn_AssignedStaff.AssignmentType,
      us_tn_AssignedStaff.StartDate__raw,
      us_tn_AssignedStaff.EndDate__raw,
      us_tn_AssignedStaff.CaseType,
      us_tn_AssignedStaff.AssignmentBeginReason,
      us_tn_AssignedStaff.AssignmentEndReason,
      us_tn_AssignedStaff.AssignmentDueDate__raw,
      us_tn_AssignedStaff.SiteID,
      us_tn_AssignedStaff.LastUpdateUserID,
      us_tn_AssignedStaff.LastUpdateDate__raw,
      us_tn_AssignedStaff.file_id,
      us_tn_AssignedStaff.is_deleted]
    sorts: [us_tn_AssignedStaff.StartDate__raw]
    note_display: hover
    note_text: "This table contains at least one occurrence of each staff member assigned to a person, whether in custody, on parole or on probation."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 12
    col: 0
    width: 24
    height: 6

  - name: CAFScore
    title: CAFScore
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_CAFScore.primary_key,
      us_tn_CAFScore.OffenderID,
      us_tn_CAFScore.CAFDate,
      us_tn_CAFScore.CAFScore,
      us_tn_CAFScore.ScheduleAScore,
      us_tn_CAFScore.ScheduleBScore,
      us_tn_CAFScore.CAFCustodyLevel,
      us_tn_CAFScore.CategoryScore1,
      us_tn_CAFScore.CategoryScore2,
      us_tn_CAFScore.CategoryScore3,
      us_tn_CAFScore.CategoryScore4,
      us_tn_CAFScore.CategoryScore5,
      us_tn_CAFScore.CategoryScore6,
      us_tn_CAFScore.CategoryScore7,
      us_tn_CAFScore.CategoryScore8,
      us_tn_CAFScore.CategoryScore9,
      us_tn_CAFScore.CategoryScore10,
      us_tn_CAFScore.CategoryScore11,
      us_tn_CAFScore.CAFType,
      us_tn_CAFScore.CAFSiteID,
      us_tn_CAFScore.LastUpdateUserID,
      us_tn_CAFScore.LastUpdateDate,
      us_tn_CAFScore.file_id,
      us_tn_CAFScore.is_deleted]
    sorts: [us_tn_CAFScore.OffenderID, us_tn_CAFScore.CAFDate]
    note_display: hover
    note_text: "This table contains scores for CAF assessments."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 18
    col: 0
    width: 24
    height: 6

  - name: CellBedAssignment
    title: CellBedAssignment
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_CellBedAssignment.primary_key,
      us_tn_CellBedAssignment.OffenderID,
      us_tn_CellBedAssignment.AssignmentDateTime__raw,
      us_tn_CellBedAssignment.AssignedSiteID,
      us_tn_CellBedAssignment.AssignedUnitID,
      us_tn_CellBedAssignment.AssignedCellID,
      us_tn_CellBedAssignment.AssignedBedID,
      us_tn_CellBedAssignment.RequestedSiteID,
      us_tn_CellBedAssignment.RequestedUnitID,
      us_tn_CellBedAssignment.RequestedCellID,
      us_tn_CellBedAssignment.RequestedBedID,
      us_tn_CellBedAssignment.ActualSiteID,
      us_tn_CellBedAssignment.ActualUnitID,
      us_tn_CellBedAssignment.ActualCellID,
      us_tn_CellBedAssignment.ActualBedId,
      us_tn_CellBedAssignment.CustodyLevel,
      us_tn_CellBedAssignment.MoveReason1,
      us_tn_CellBedAssignment.MoveReason2,
      us_tn_CellBedAssignment.EndDate__raw,
      us_tn_CellBedAssignment.LastUpdateUserID,
      us_tn_CellBedAssignment.LastUpdateDate__raw,
      us_tn_CellBedAssignment.file_id,
      us_tn_CellBedAssignment.is_deleted]
    sorts: [us_tn_CellBedAssignment.AssignmentDateTime__raw]
    note_display: hover
    note_text: "This table contains information about individuals cell and bed assignemnts."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 24
    col: 0
    width: 24
    height: 6

  - name: Classification
    title: Classification
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_Classification.primary_key,
      us_tn_Classification.OffenderID,
      us_tn_Classification.CAFDate__raw,
      us_tn_Classification.ClassificationSequenceNumber,
      us_tn_Classification.ClassificationDueDate__raw,
      us_tn_Classification.HearingDateTime__raw,
      us_tn_Classification.WaiverSignedDate__raw,
      us_tn_Classification.NoticeWaiverReason,
      us_tn_Classification.PostedDate__raw,
      us_tn_Classification.ClassificationDate__raw,
      us_tn_Classification.ClassificationType,
      us_tn_Classification.StaffID,
      us_tn_Classification.RecommendedCustody,
      us_tn_Classification.TransferSiteID,
      us_tn_Classification.OverrideReason,
      us_tn_Classification.ClassificationDecisionDate__raw,
      us_tn_Classification.ClassificationDecision,
      us_tn_Classification.ApprovingAuthorityStaffID,
      us_tn_Classification.AppealRespondentStaffID,
      us_tn_Classification.ClassificationCommitteeStaffID1,
      us_tn_Classification.ClassificationCommitteeStaffID2,
      us_tn_Classification.ClassificationCommitteeStaffID3,
      us_tn_Classification.DenialReason,
      us_tn_Classification.AppealDecision,
      us_tn_Classification.AppealReceivedDate__raw,
      us_tn_Classification.AppealDecisionDate__raw,
      us_tn_Classification.AppealReasonComments,
      us_tn_Classification.AppealDecisionComments,
      us_tn_Classification.ProgramID1,
      us_tn_Classification.ProgramID2,
      us_tn_Classification.ProgramID3,
      us_tn_Classification.JobID1,
      us_tn_Classification.JobID2,
      us_tn_Classification.JobID3,
      us_tn_Classification.ClassID1,
      us_tn_Classification.ClassID2,
      us_tn_Classification.ClassID3,
      us_tn_Classification.ClassificationComments,
      us_tn_Classification.SegregationFlag,
      us_tn_Classification.LastUpdateUserID,
      us_tn_Classification.LastUpdateDate__raw,
      us_tn_Classification.file_id,
      us_tn_Classification.is_deleted]
    sorts: [us_tn_Classification.CAFDate__raw]
    note_display: hover
    note_text: "This entity stores information about the initial and all subsequent classifications of a person. This includes appeals and overrides and results of decisions related to the classification."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 30
    col: 0
    width: 24
    height: 6

  - name: ContactNoteType
    title: ContactNoteType
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_ContactNoteType.primary_key,
      us_tn_ContactNoteType.OffenderID,
      us_tn_ContactNoteType.ContactNoteDateTime,
      us_tn_ContactNoteType.ContactNoteType,
      us_tn_ContactNoteType.LastUpdateUserID,
      us_tn_ContactNoteType.LastUpdateDateTime,
      us_tn_ContactNoteType.file_id,
      us_tn_ContactNoteType.is_deleted]
    sorts: [us_tn_ContactNoteType.OffenderID, us_tn_ContactNoteType.ContactNoteDateTime, us_tn_ContactNoteType.ContactNoteType]
    note_display: hover
    note_text: "This table contains one occurrence for each contact between a staff and person."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 36
    col: 0
    width: 24
    height: 6

  - name: Disciplinary
    title: Disciplinary
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_Disciplinary.primary_key,
      us_tn_Disciplinary.OffenderID,
      us_tn_Disciplinary.IncidentID,
      us_tn_Disciplinary.IncidentDate__raw,
      us_tn_Disciplinary.DisciplinaryClass,
      us_tn_Disciplinary.ClassID,
      us_tn_Disciplinary.ClassSectionID,
      us_tn_Disciplinary.JobID,
      us_tn_Disciplinary.PositionID,
      us_tn_Disciplinary.OffenderAccount,
      us_tn_Disciplinary.WeaponsUsed,
      us_tn_Disciplinary.ViolenceLevel,
      us_tn_Disciplinary.RefuseToSignDate__raw,
      us_tn_Disciplinary.DispositionDate__raw,
      us_tn_Disciplinary.Disposition,
      us_tn_Disciplinary.AdvisorOffenderID,
      us_tn_Disciplinary.AdvisorStaffID,
      us_tn_Disciplinary.DecisionPersonID1,
      us_tn_Disciplinary.Decision1,
      us_tn_Disciplinary.DecisionPersonID2,
      us_tn_Disciplinary.Decision2,
      us_tn_Disciplinary.DecisionPersonID3,
      us_tn_Disciplinary.Decision3,
      us_tn_Disciplinary.DecisionPersonID4,
      us_tn_Disciplinary.Decision4,
      us_tn_Disciplinary.DecisionPersonID5,
      us_tn_Disciplinary.Decision5,
      us_tn_Disciplinary.PostedDateTime__raw,
      us_tn_Disciplinary.PostedByStaffID,
      us_tn_Disciplinary.LastUpdateUserID,
      us_tn_Disciplinary.LastUpdateDate__raw,
      us_tn_Disciplinary.file_id,
      us_tn_Disciplinary.is_deleted]
    sorts: [us_tn_Disciplinary.IncidentDate__raw]
    note_display: hover
    note_text: "This table contains all information relating to disciplinaries. Disciplinaries may result from an incident, or a job/class related action."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 42
    col: 0
    width: 24
    height: 6

  - name: DisciplinarySentence
    title: DisciplinarySentence
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_DisciplinarySentence.primary_key,
      us_tn_DisciplinarySentence.OffenderID,
      us_tn_DisciplinarySentence.IncidentID,
      us_tn_DisciplinarySentence.SentenceType,
      us_tn_DisciplinarySentence.RevisedPayRateAmount,
      us_tn_DisciplinarySentence.SentenceDate__raw,
      us_tn_DisciplinarySentence.TrustFundDeductionAmount,
      us_tn_DisciplinarySentence.SentenceHours,
      us_tn_DisciplinarySentence.SentenceDays,
      us_tn_DisciplinarySentence.SentenceWeeks,
      us_tn_DisciplinarySentence.SentenceMonths,
      us_tn_DisciplinarySentence.LastUpdateUserID,
      us_tn_DisciplinarySentence.LastUpdateDate__raw,
      us_tn_DisciplinarySentence.file_id,
      us_tn_DisciplinarySentence.is_deleted]
    sorts: [us_tn_DisciplinarySentence.SentenceDate__raw]
    note_display: hover
    note_text: "This table contains information about the sentences which result from a disciplinary."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 48
    col: 0
    width: 24
    height: 6

  - name: Diversion
    title: Diversion
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_Diversion.primary_key,
      us_tn_Diversion.OffenderID,
      us_tn_Diversion.ConvictionCounty,
      us_tn_Diversion.CaseYear,
      us_tn_Diversion.CaseNumber,
      us_tn_Diversion.CountNumber,
      us_tn_Diversion.DiversionType,
      us_tn_Diversion.DiversionStatus,
      us_tn_Diversion.StatusDate__raw,
      us_tn_Diversion.DiversionGrantedDate__raw,
      us_tn_Diversion.Offense,
      us_tn_Diversion.ExpirationDate__raw,
      us_tn_Diversion.JudicialCloseDate__raw,
      us_tn_Diversion.StaffID,
      us_tn_Diversion.LastModificationDate__raw,
      us_tn_Diversion.LastUpdateUserID,
      us_tn_Diversion.LastUpdateDate,
      us_tn_Diversion.file_id,
      us_tn_Diversion.is_deleted]
    sorts: [us_tn_Diversion.StatusDate__raw]
    note_display: hover
    note_text: "This table contains one occurrence for each diversion that applies to the specified case. These diversions apply to people who have not yet been sentenced."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 54
    col: 0
    width: 24
    height: 6

  - name: ISCRelatedSentence
    title: ISCRelatedSentence
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_ISCRelatedSentence.primary_key,
      us_tn_ISCRelatedSentence.OffenderID,
      us_tn_ISCRelatedSentence.Jurisdication,
      us_tn_ISCRelatedSentence.CaseYear,
      us_tn_ISCRelatedSentence.CaseNumber,
      us_tn_ISCRelatedSentence.CountNumber,
      us_tn_ISCRelatedSentence.RelatedJurisidicationCounty,
      us_tn_ISCRelatedSentence.RelatedCaseYear,
      us_tn_ISCRelatedSentence.RelatedCaseNumber,
      us_tn_ISCRelatedSentence.RelatedCountNumber,
      us_tn_ISCRelatedSentence.RelatedSentenceType,
      us_tn_ISCRelatedSentence.CrimeType,
      us_tn_ISCRelatedSentence.OriginalSentence,
      us_tn_ISCRelatedSentence.LastUpdateUserID,
      us_tn_ISCRelatedSentence.LastUpdateDate,
      us_tn_ISCRelatedSentence.file_id,
      us_tn_ISCRelatedSentence.is_deleted]
    sorts: [us_tn_ISCRelatedSentence.OffenderID, us_tn_ISCRelatedSentence.Jurisdication, us_tn_ISCRelatedSentence.CaseYear, us_tn_ISCRelatedSentence.CaseNumber, us_tn_ISCRelatedSentence.CountNumber, us_tn_ISCRelatedSentence.RelatedJurisidicationCounty, us_tn_ISCRelatedSentence.RelatedCaseYear, us_tn_ISCRelatedSentence.RelatedCaseNumber, us_tn_ISCRelatedSentence.RelatedCountNumber]
    note_display: hover
    note_text: "This table contains one occurrence for each sentence that is related to an ISC sentence. An ISC related sentence can be either consecutive to or concurrent with the \"parent\" ISC sentence."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 60
    col: 0
    width: 24
    height: 6

  - name: ISCSentence
    title: ISCSentence
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_ISCSentence.primary_key,
      us_tn_ISCSentence.OffenderID,
      us_tn_ISCSentence.Jurisdiction,
      us_tn_ISCSentence.CaseYear,
      us_tn_ISCSentence.CaseNumber,
      us_tn_ISCSentence.CountNumber,
      us_tn_ISCSentence.ForeignJurisdication,
      us_tn_ISCSentence.IndictmentNumber,
      us_tn_ISCSentence.ConvictedOffense,
      us_tn_ISCSentence.OffenseDate__raw,
      us_tn_ISCSentence.ISCSentencyType,
      us_tn_ISCSentence.WarrantNumber,
      us_tn_ISCSentence.Sentence,
      us_tn_ISCSentence.SentenceImposedDate__raw,
      us_tn_ISCSentence.DispositionCase,
      us_tn_ISCSentence.JudgeName,
      us_tn_ISCSentence.ReleaseEligibilityDate__raw,
      us_tn_ISCSentence.ExpirationDate__raw,
      us_tn_ISCSentence.ActualReleaseDAte__raw,
      us_tn_ISCSentence.ISCCreditDays,
      us_tn_ISCSentence.ISCCreditDeadTime,
      us_tn_ISCSentence.ISCPretrialCredit,
      us_tn_ISCSentence.LastUpdateUserID,
      us_tn_ISCSentence.LastUpdateDate,
      us_tn_ISCSentence.file_id,
      us_tn_ISCSentence.is_deleted]
    sorts: [us_tn_ISCSentence.OffenseDate__raw]
    note_display: hover
    note_text: "This table contains one occurrence for each out of Tennessee jurisdiction sentence tracked for the person."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 66
    col: 0
    width: 24
    height: 6

  - name: JOCharge
    title: JOCharge
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_JOCharge.primary_key,
      us_tn_JOCharge.OffenderID,
      us_tn_JOCharge.ConvictionCounty,
      us_tn_JOCharge.CaseYear,
      us_tn_JOCharge.CaseNumber,
      us_tn_JOCharge.CountNumber,
      us_tn_JOCharge.OffenderPlea,
      us_tn_JOCharge.Verdict,
      us_tn_JOCharge.ChargeOffense,
      us_tn_JOCharge.OffenseCounty,
      us_tn_JOCharge.AmendedChargeOffense,
      us_tn_JOCharge.ConvictionOffense,
      us_tn_JOCharge.ConvictionClass,
      us_tn_JOCharge.IndictmentClass,
      us_tn_JOCharge.CrimeType,
      us_tn_JOCharge.OffenseDate__raw,
      us_tn_JOCharge.PleaDate__raw,
      us_tn_JOCharge.SentenceImposedDate__raw,
      us_tn_JOCharge.SentenceLaw,
      us_tn_JOCharge.SentenceOffenderType,
      us_tn_JOCharge.SentencedTo,
      us_tn_JOCharge.WorkRelease,
      us_tn_JOCharge.SuspendedToProbation,
      us_tn_JOCharge.SplitConfinementBalanceType,
      us_tn_JOCharge.MultipleRapistFlag,
      us_tn_JOCharge.ChildRapistFlag,
      us_tn_JOCharge.RepeatViolentOffenderFlag,
      us_tn_JOCharge.SchoolZoneFlag,
      us_tn_JOCharge.MethRelatedFlag,
      us_tn_JOCharge.FirearmFlag,
      us_tn_JOCharge.GangRelatedFlag,
      us_tn_JOCharge.ChildPredatorFlag,
      us_tn_JOCharge.AlternatePercentRange,
      us_tn_JOCharge.AggrevatedRapeFlag,
      us_tn_JOCharge.ChildSexAbuseFlag,
      us_tn_JOCharge.MultipleFirearmFlag,
      us_tn_JOCharge.LastUpdateUserID,
      us_tn_JOCharge.LastUpdateDate,
      us_tn_JOCharge.file_id,
      us_tn_JOCharge.is_deleted]
    sorts: [us_tn_JOCharge.OffenseDate__raw]
    note_display: hover
    note_text: "This table contains one occurrence for each judgment order received by a person. This table contains information about the charge contained on the judgment order."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 72
    col: 0
    width: 24
    height: 6

  - name: JOIdentification
    title: JOIdentification
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_JOIdentification.primary_key,
      us_tn_JOIdentification.OffenderID,
      us_tn_JOIdentification.ConvictionCounty,
      us_tn_JOIdentification.CaseYear,
      us_tn_JOIdentification.CaseNumber,
      us_tn_JOIdentification.CountNumber,
      us_tn_JOIdentification.FirstName,
      us_tn_JOIdentification.MiddleName,
      us_tn_JOIdentification.LastName,
      us_tn_JOIdentification.SuffixName,
      us_tn_JOIdentification.BirthDate__raw,
      us_tn_JOIdentification.SocialSecurityNumber,
      us_tn_JOIdentification.IndictmentNumber,
      us_tn_JOIdentification.WarrantNumber,
      us_tn_JOIdentification.JudicialDistrict,
      us_tn_JOIdentification.JudicialDivision,
      us_tn_JOIdentification.DefenseAttorneyName,
      us_tn_JOIdentification.DistrictAttorneyName,
      us_tn_JOIdentification.DefenseAttorneyType,
      us_tn_JOIdentification.SIDNumber,
      us_tn_JOIdentification.CountyOffenderID,
      us_tn_JOIdentification.PostedDate__raw,
      us_tn_JOIdentification.VictimRelation,
      us_tn_JOIdentification.VictimAge,
      us_tn_JOIdentification.ArrestedDate__raw,
      us_tn_JOIdentification.LastUpdateUserID,
      us_tn_JOIdentification.LastUpdateDate,
      us_tn_JOIdentification.file_id,
      us_tn_JOIdentification.is_deleted]
    sorts: [us_tn_JOIdentification.BirthDate__raw]
    note_display: hover
    note_text: "This table contains one occurrence for each judgment order received by a person, and contains information to identify that judgment order."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 78
    col: 0
    width: 24
    height: 6

  - name: JOMiscellaneous
    title: JOMiscellaneous
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_JOMiscellaneous.primary_key,
      us_tn_JOMiscellaneous.OffenderID,
      us_tn_JOMiscellaneous.ConvictionCounty,
      us_tn_JOMiscellaneous.CaseYear,
      us_tn_JOMiscellaneous.CaseNumber,
      us_tn_JOMiscellaneous.CountNumber,
      us_tn_JOMiscellaneous.JudgeName,
      us_tn_JOMiscellaneous.JOStatusDate__raw,
      us_tn_JOMiscellaneous.JOStatus,
      us_tn_JOMiscellaneous.OverridenToLegalFlag,
      us_tn_JOMiscellaneous.JOEntryDate__raw,
      us_tn_JOMiscellaneous.LastLetterCreatedDate__raw,
      us_tn_JOMiscellaneous.PostConvictionReliefConvictionCounty,
      us_tn_JOMiscellaneous.PostConvictionReliefCaseYear,
      us_tn_JOMiscellaneous.PostConvictionReliefCaseNumber,
      us_tn_JOMiscellaneous.PostConvictionReliefCountNumber,
      us_tn_JOMiscellaneous.LastAppealFinalizedDate__raw,
      us_tn_JOMiscellaneous.LastAmendmentFiledDate__raw,
      us_tn_JOMiscellaneous.LastPostConvictionReliefDate__raw,
      us_tn_JOMiscellaneous.LastAppealDecision,
      us_tn_JOMiscellaneous.LastJudgmentOrderChangeDate__raw,
      us_tn_JOMiscellaneous.LastUpdateUserID,
      us_tn_JOMiscellaneous.LastUpdateDate,
      us_tn_JOMiscellaneous.file_id,
      us_tn_JOMiscellaneous.is_deleted]
    sorts: [us_tn_JOMiscellaneous.JOStatusDate__raw]
    note_display: hover
    note_text: "This table contains one occurrence for each judgment order received by an offender. This table contains miscellaneous information about the judgment order."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 84
    col: 0
    width: 24
    height: 6

  - name: JOSentence
    title: JOSentence
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_JOSentence.primary_key,
      us_tn_JOSentence.OffenderID,
      us_tn_JOSentence.ConvictionCounty,
      us_tn_JOSentence.CaseYear,
      us_tn_JOSentence.CaseNumber,
      us_tn_JOSentence.CountNumber,
      us_tn_JOSentence.ExecutionDate__raw,
      us_tn_JOSentence.FirstConfinementCompletedDate__raw,
      us_tn_JOSentence.CalculatedPretrialCredits,
      us_tn_JOSentence.CalculatedSheriffCredits,
      us_tn_JOSentence.PretrialJailCredits,
      us_tn_JOSentence.CalculatedPretrialJailBehaviorCredits,
      us_tn_JOSentence.CriminalInjuryCompensationAmount,
      us_tn_JOSentence.SupervisionFeeAmount,
      us_tn_JOSentence.ChildSupportAmount,
      us_tn_JOSentence.CourtCostAmount,
      us_tn_JOSentence.CourtFineAmount,
      us_tn_JOSentence.RenderedInfamousFlag,
      us_tn_JOSentence.EnhancementYears,
      us_tn_JOSentence.MaximumSentenceYears,
      us_tn_JOSentence.MaximumSentenceMonths,
      us_tn_JOSentence.MaximumSentenceDays,
      us_tn_JOSentence.MaximumSentenceWeekends,
      us_tn_JOSentence.MinimumSentenceYears,
      us_tn_JOSentence.MinimumSentenceMonths,
      us_tn_JOSentence.MinimumSentenceDays,
      us_tn_JOSentence.WorkReleaseYears,
      us_tn_JOSentence.WorkReleaseMonths,
      us_tn_JOSentence.WorkReleaseDays,
      us_tn_JOSentence.ProbationSentenceYears,
      us_tn_JOSentence.ProbationSentenceMonths,
      us_tn_JOSentence.ProbationSentenceDays,
      us_tn_JOSentence.FirstSplitConfinementYears,
      us_tn_JOSentence.FirstSplitConfinementMonths,
      us_tn_JOSentence.FirstSplitConfinementDays,
      us_tn_JOSentence.SecondSplitConfinementYears,
      us_tn_JOSentence.SecondSplitConfinementMonths,
      us_tn_JOSentence.SecondSplitConfinementDays,
      us_tn_JOSentence.Period,
      us_tn_JOSentence.LifeDeathHabitual,
      us_tn_JOSentence.UnpaidCommunityServiceHours,
      us_tn_JOSentence.UnpaidCommunityServiceDays,
      us_tn_JOSentence.UnpaidCommunityServiceWeeks,
      us_tn_JOSentence.UnpaidCommunityServiceMonths,
      us_tn_JOSentence.LifetimeSupervision,
      us_tn_JOSentence.DrugCourtFlag,
      us_tn_JOSentence.UnsupervisedPrivateProbationFlag,
      us_tn_JOSentence.HIVSpecimenFlag,
      us_tn_JOSentence.SexOffenderTaxAmount,
      us_tn_JOSentence.MandatoryMinimumStartDate__raw,
      us_tn_JOSentence.AbuseRegisterFlag,
      us_tn_JOSentence.StayExecutionDate__raw,
      us_tn_JOSentence.CommunityCorrectionYears,
      us_tn_JOSentence.CommunityCorrectionMonths,
      us_tn_JOSentence.CommunityCorrectionDays,
      us_tn_JOSentence.LastUpdateUserID,
      us_tn_JOSentence.LastUpdateDate,
      us_tn_JOSentence.file_id,
      us_tn_JOSentence.is_deleted]
    sorts: [us_tn_JOSentence.ExecutionDate__raw]
    note_display: hover
    note_text: "Each row in this table is for a single sentence of a single  judgment order received by a person. It can be linked to other 'JO' tables."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 90
    col: 0
    width: 24
    height: 6

  - name: JOSpecialConditions
    title: JOSpecialConditions
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_JOSpecialConditions.primary_key,
      us_tn_JOSpecialConditions.OffenderID,
      us_tn_JOSpecialConditions.ConvictionCounty,
      us_tn_JOSpecialConditions.CaseYear,
      us_tn_JOSpecialConditions.CaseNumber,
      us_tn_JOSpecialConditions.CountNumber,
      us_tn_JOSpecialConditions.PageNumber,
      us_tn_JOSpecialConditions.LineNumber,
      us_tn_JOSpecialConditions.SpecialConditions,
      us_tn_JOSpecialConditions.LastUpdateUserID,
      us_tn_JOSpecialConditions.LastUpdateDate,
      us_tn_JOSpecialConditions.file_id,
      us_tn_JOSpecialConditions.is_deleted]
    sorts: [us_tn_JOSpecialConditions.OffenderID, us_tn_JOSpecialConditions.ConvictionCounty, us_tn_JOSpecialConditions.CaseYear, us_tn_JOSpecialConditions.CaseNumber, us_tn_JOSpecialConditions.CountNumber, us_tn_JOSpecialConditions.PageNumber, us_tn_JOSpecialConditions.LineNumber]
    note_display: hover
    note_text: "This table contains multiple occurences for each Judgement Order Special Condition."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 96
    col: 0
    width: 24
    height: 6

  - name: Offender
    title: Offender
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_Offender.primary_key,
      us_tn_Offender.OffenderID,
      us_tn_Offender.AssignedSiteID,
      us_tn_Offender.AssignedUnitID,
      us_tn_Offender.AssignedCellID,
      us_tn_Offender.AssignedBedID,
      us_tn_Offender.ActualUnitID,
      us_tn_Offender.ActualCellID,
      us_tn_Offender.ActualBedID,
      us_tn_Offender.CustodyLevel,
      us_tn_Offender.NumberOffenderVisitor,
      us_tn_Offender.EscapeFlag,
      us_tn_Offender.EscapeHistoryFlag,
      us_tn_Offender.Jurisdication,
      us_tn_Offender.ArchiveDate__raw,
      us_tn_Offender.AbscondedParoleFlag,
      us_tn_Offender.AbscondedProbationFlag,
      us_tn_Offender.AbscondedCommunityCorrectionFlag,
      us_tn_Offender.ParoleOfficeID,
      us_tn_Offender.ProbationOfficeID,
      us_tn_Offender.CommunityCorrectionOfficeID,
      us_tn_Offender.NotInCustodyDate__raw,
      us_tn_Offender.NotIncustoryReason,
      us_tn_Offender.VisitorRelationshipResetDate,
      us_tn_Offender.PostedByStaffID,
      us_tn_Offender.PostedDate__raw,
      us_tn_Offender.LastUpdateUserID,
      us_tn_Offender.LastUpdateDate__raw,
      us_tn_Offender.file_id,
      us_tn_Offender.is_deleted]
    sorts: [us_tn_Offender.ArchiveDate__raw]
    note_display: hover
    note_text: "This table contains one occurrence for each person in the TOMIS system. It contains location information used throughout the system. It is the \"parent\" table for associated person tables."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 102
    col: 0
    width: 24
    height: 6

  - name: OffenderAttributes
    title: OffenderAttributes
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_OffenderAttributes.primary_key,
      us_tn_OffenderAttributes.OffenderID,
      us_tn_OffenderAttributes.Weight,
      us_tn_OffenderAttributes.HeightFeet,
      us_tn_OffenderAttributes.HeightInches,
      us_tn_OffenderAttributes.EyeColor,
      us_tn_OffenderAttributes.HairColor,
      us_tn_OffenderAttributes.Complexion,
      us_tn_OffenderAttributes.MaritalStatus,
      us_tn_OffenderAttributes.DriverLicenseState,
      us_tn_OffenderAttributes.DriverLicenseNumber,
      us_tn_OffenderAttributes.ScarsMarksTattoos,
      us_tn_OffenderAttributes.Religion,
      us_tn_OffenderAttributes.BirthCounty,
      us_tn_OffenderAttributes.BirthState,
      us_tn_OffenderAttributes.NCICFingerprintID,
      us_tn_OffenderAttributes.TennesseeFingerprintID,
      us_tn_OffenderAttributes.DeathType,
      us_tn_OffenderAttributes.DeathDate__raw,
      us_tn_OffenderAttributes.DeathLocation,
      us_tn_OffenderAttributes.DisposalType,
      us_tn_OffenderAttributes.DisposalDate__raw,
      us_tn_OffenderAttributes.DisposalLocation,
      us_tn_OffenderAttributes.DeathPostedByStaffID,
      us_tn_OffenderAttributes.OBSCISID,
      us_tn_OffenderAttributes.RestoreDate__raw,
      us_tn_OffenderAttributes.DeathSiteID,
      us_tn_OffenderAttributes.DeathCertificateFlag,
      us_tn_OffenderAttributes.OldFBINumber,
      us_tn_OffenderAttributes.OldSTGAffiliation,
      us_tn_OffenderAttributes.OldSTG,
      us_tn_OffenderAttributes.OldSTGComments,
      us_tn_OffenderAttributes.CitizenshipCountry,
      us_tn_OffenderAttributes.BirthPlace,
      us_tn_OffenderAttributes.AlienID,
      us_tn_OffenderAttributes.FBINumber,
      us_tn_OffenderAttributes.SIDNumber,
      us_tn_OffenderAttributes.LastUpdateUserID,
      us_tn_OffenderAttributes.LastUpdateDate__raw,
      us_tn_OffenderAttributes.file_id,
      us_tn_OffenderAttributes.is_deleted]
    sorts: [us_tn_OffenderAttributes.DeathDate__raw]
    note_display: hover
    note_text: "This table contains miscellaneous information about a person, such as height, weight, hair color, etc."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 108
    col: 0
    width: 24
    height: 6

  - name: OffenderMovement
    title: OffenderMovement
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_OffenderMovement.primary_key,
      us_tn_OffenderMovement.OffenderID,
      us_tn_OffenderMovement.MovementDateTime,
      us_tn_OffenderMovement.MovementType,
      us_tn_OffenderMovement.MovementReason,
      us_tn_OffenderMovement.FromLocationID,
      us_tn_OffenderMovement.ToLocationID,
      us_tn_OffenderMovement.ArrivalDepartureFlag,
      us_tn_OffenderMovement.LastUpdateUserID,
      us_tn_OffenderMovement.LastUpdateDate,
      us_tn_OffenderMovement.file_id,
      us_tn_OffenderMovement.is_deleted]
    sorts: [us_tn_OffenderMovement.OffenderID, us_tn_OffenderMovement.MovementDateTime]
    note_display: hover
    note_text: "This table contains one occurrence for every arrivel and/or departure of a person  from an institution."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 114
    col: 0
    width: 24
    height: 6

  - name: Sentence
    title: Sentence
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_Sentence.primary_key,
      us_tn_Sentence.OffenderID,
      us_tn_Sentence.ConvictionCounty,
      us_tn_Sentence.CaseYear,
      us_tn_Sentence.CaseNumber,
      us_tn_Sentence.CountNumber,
      us_tn_Sentence.SentenceChangedDate__raw,
      us_tn_Sentence.SentenceStatus,
      us_tn_Sentence.SentenceStatusDate__raw,
      us_tn_Sentence.SentenceEffectiveDate__raw,
      us_tn_Sentence.FullExpirationDate__raw,
      us_tn_Sentence.ExpirationDate__raw,
      us_tn_Sentence.SafetyValveDate__raw,
      us_tn_Sentence.ReleaseEligibilityDate__raw,
      us_tn_Sentence.EarliestPossibleReleaseDate__raw,
      us_tn_Sentence.MandatoryParoleDate__raw,
      us_tn_Sentence.RegularParoleDate__raw,
      us_tn_Sentence.ProbationaryParoleDate__raw,
      us_tn_Sentence.TotalPPSCCredits,
      us_tn_Sentence.TotalBehaviorCredits,
      us_tn_Sentence.TotalProgramCredits,
      us_tn_Sentence.TotalGEDCredits,
      us_tn_Sentence.TotalLiteraryCredits,
      us_tn_Sentence.TotalDrugAlcoholCredits,
      us_tn_Sentence.TotalEducationAttendanceCredits,
      us_tn_Sentence.TotalLostGoodConductCredits,
      us_tn_Sentence.TotalDeadTime,
      us_tn_Sentence.TotalDelinquentTime,
      us_tn_Sentence.TotalStreetTime,
      us_tn_Sentence.RangePercent,
      us_tn_Sentence.InactivePardonedFlag,
      us_tn_Sentence.InactiveDismissedFlag,
      us_tn_Sentence.InactiveCommutedFlag,
      us_tn_Sentence.InactiveExpiredFlag,
      us_tn_Sentence.InactiveAwaitingRetrialFlag,
      us_tn_Sentence.InactiveCourtOrderFlag,
      us_tn_Sentence.ConfinementExpirationDate__raw,
      us_tn_Sentence.ConvertedPPSCCredits,
      us_tn_Sentence.ConvertedBehaviorCredits,
      us_tn_Sentence.ConvertedProgramCredits,
      us_tn_Sentence.ConvertedDeadTime,
      us_tn_Sentence.ConvertedLostGoodConductCredits,
      us_tn_Sentence.ConfinementFullExpirationDate__raw,
      us_tn_Sentence.SlagleFlag,
      us_tn_Sentence.ViolentOneHundredPercentFlag,
      us_tn_Sentence.NoPreviousParoleFlag,
      us_tn_Sentence.OriginalBehaviorCredits,
      us_tn_Sentence.OriginalProgramCredits,
      us_tn_Sentence.OriginalPPSCCredits,
      us_tn_Sentence.RemovedBehaviorCredits,
      us_tn_Sentence.RemovedProgramCredits,
      us_tn_Sentence.RemovedPPSCCredits,
      us_tn_Sentence.ReinstatedBehaviorCredits,
      us_tn_Sentence.ReinstatedProgramCredits,
      us_tn_Sentence.ReinstatedPPSCCredits,
      us_tn_Sentence.ConsecutiveConvictionCounty,
      us_tn_Sentence.ConsecutiveCaseYear,
      us_tn_Sentence.ConsecutiveCaseNumber,
      us_tn_Sentence.ConsecutiveCountNumber,
      us_tn_Sentence.AuthorizedLostGoodConductFlag,
      us_tn_Sentence.SafetyValveOverrideFlag,
      us_tn_Sentence.TotalTreatmentCredits,
      us_tn_Sentence.TotalJailEd,
      us_tn_Sentence.LastUpdateUserID,
      us_tn_Sentence.LastUpdateDate,
      us_tn_Sentence.file_id,
      us_tn_Sentence.is_deleted]
    sorts: [us_tn_Sentence.SentenceChangedDate__raw]
    note_display: hover
    note_text: "This entity stores information about each sentence which applies to a certain offense.  This includes suspendedsentences, probation, SAIU, etc. Fields starting with 'Consecutive' denote sentence information that this sentence is *consecutive to*, meaning the sentence information in the 'Consecutive' fields is this row's parent sentence and will have been served before this row's sentence."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 120
    col: 0
    width: 24
    height: 6

  - name: SentenceAction
    title: SentenceAction
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_SentenceAction.primary_key,
      us_tn_SentenceAction.OffenderID,
      us_tn_SentenceAction.ConvictionCounty,
      us_tn_SentenceAction.CaseYear,
      us_tn_SentenceAction.CaseNumber,
      us_tn_SentenceAction.CountNumber,
      us_tn_SentenceAction.ActionDate__raw,
      us_tn_SentenceAction.SequenceNumber,
      us_tn_SentenceAction.SentenceAction,
      us_tn_SentenceAction.StaffID,
      us_tn_SentenceAction.LastUpdateUserID,
      us_tn_SentenceAction.LastUpdateDate,
      us_tn_SentenceAction.file_id,
      us_tn_SentenceAction.is_deleted]
    sorts: [us_tn_SentenceAction.ActionDate__raw]
    note_display: hover
    note_text: "This table contains one occurrence for sentence action that is posted for the specified sentence."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 126
    col: 0
    width: 24
    height: 6

  - name: SentenceMiscellaneous
    title: SentenceMiscellaneous
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_SentenceMiscellaneous.primary_key,
      us_tn_SentenceMiscellaneous.OffenderID,
      us_tn_SentenceMiscellaneous.ConvictionCounty,
      us_tn_SentenceMiscellaneous.CaseYear,
      us_tn_SentenceMiscellaneous.CaseNumber,
      us_tn_SentenceMiscellaneous.CountNumber,
      us_tn_SentenceMiscellaneous.BOPDate__raw,
      us_tn_SentenceMiscellaneous.CustodialParoleDate__raw,
      us_tn_SentenceMiscellaneous.AlternateSentenceImposeDate__raw,
      us_tn_SentenceMiscellaneous.CustodialParoleDateFlag,
      us_tn_SentenceMiscellaneous.BOPFlag,
      us_tn_SentenceMiscellaneous.StaffID,
      us_tn_SentenceMiscellaneous.DeclineToExpireFlag,
      us_tn_SentenceMiscellaneous.CustodialParoleDateCode,
      us_tn_SentenceMiscellaneous.ConvictOffenderParole,
      us_tn_SentenceMiscellaneous.LastModificationDate__raw,
      us_tn_SentenceMiscellaneous.LastUpatedUserID,
      us_tn_SentenceMiscellaneous.LastUpdateDate,
      us_tn_SentenceMiscellaneous.file_id,
      us_tn_SentenceMiscellaneous.is_deleted]
    sorts: [us_tn_SentenceMiscellaneous.BOPDate__raw]
    note_display: hover
    note_text: "This table contains one occurrence for each sentence received by the person. It contains miscellaneous information about the sentence."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 132
    col: 0
    width: 24
    height: 6

  - name: SupervisionPlan
    title: SupervisionPlan
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_SupervisionPlan.primary_key,
      us_tn_SupervisionPlan.OffenderID,
      us_tn_SupervisionPlan.PlanStartDate__raw,
      us_tn_SupervisionPlan.PlanType,
      us_tn_SupervisionPlan.PlanEndDate__raw,
      us_tn_SupervisionPlan.StandardStartDate__raw,
      us_tn_SupervisionPlan.StandardEndDate__raw,
      us_tn_SupervisionPlan.SupervisionLevel,
      us_tn_SupervisionPlan.OPIScore,
      us_tn_SupervisionPlan.GroupReportFlag,
      us_tn_SupervisionPlan.ProgramID1,
      us_tn_SupervisionPlan.ProgramFrequency1,
      us_tn_SupervisionPlan.ProgramPeriod1,
      us_tn_SupervisionPlan.ProgramID2,
      us_tn_SupervisionPlan.ProgramFrequency2,
      us_tn_SupervisionPlan.ProgramPeriod2,
      us_tn_SupervisionPlan.ProgramID3,
      us_tn_SupervisionPlan.ProgramFrequency3,
      us_tn_SupervisionPlan.ProgramPeriod3,
      us_tn_SupervisionPlan.ProgramID4,
      us_tn_SupervisionPlan.ProgramFrequency4,
      us_tn_SupervisionPlan.ProgramPeriod4,
      us_tn_SupervisionPlan.ProgramID5,
      us_tn_SupervisionPlan.ProgramFrequency5,
      us_tn_SupervisionPlan.ProgramPeriod5,
      us_tn_SupervisionPlan.Condition1,
      us_tn_SupervisionPlan.ConditionFrequency1,
      us_tn_SupervisionPlan.ConditionPeriod1,
      us_tn_SupervisionPlan.Condition2,
      us_tn_SupervisionPlan.ConditionFrequency2,
      us_tn_SupervisionPlan.ConditionPeriod2,
      us_tn_SupervisionPlan.Condition3,
      us_tn_SupervisionPlan.ConditionFrequency3,
      us_tn_SupervisionPlan.ConditionPeriod3,
      us_tn_SupervisionPlan.Condition4,
      us_tn_SupervisionPlan.ConditionFrequency4,
      us_tn_SupervisionPlan.ConditionPeriod4,
      us_tn_SupervisionPlan.Condition5,
      us_tn_SupervisionPlan.ConditionFrequency5,
      us_tn_SupervisionPlan.ConditionPeriod5,
      us_tn_SupervisionPlan.SiteID,
      us_tn_SupervisionPlan.StaffID,
      us_tn_SupervisionPlan.PostedByStaffID,
      us_tn_SupervisionPlan.PostedDate__raw,
      us_tn_SupervisionPlan.LastUpdateStaffID,
      us_tn_SupervisionPlan.LastUpdateUserID,
      us_tn_SupervisionPlan.LastUpdateDate,
      us_tn_SupervisionPlan.SexOffenderSupervisionFlag,
      us_tn_SupervisionPlan.SexOffenderSupervisionType,
      us_tn_SupervisionPlan.GPSSupervisionFlag,
      us_tn_SupervisionPlan.RFSupervisionFlag,
      us_tn_SupervisionPlan.IOTSupervisionFlag,
      us_tn_SupervisionPlan.WarrantOnBond,
      us_tn_SupervisionPlan.file_id,
      us_tn_SupervisionPlan.is_deleted]
    sorts: [us_tn_SupervisionPlan.PlanStartDate__raw]
    note_display: hover
    note_text: "This table contains one occurance for each plan of supervision entered for an offender."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 138
    col: 0
    width: 24
    height: 6

  - name: Violations
    title: Violations
    explore: us_tn_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_tn_Violations.primary_key,
      us_tn_Violations.TriggerNumber,
      us_tn_Violations.OffenderID,
      us_tn_Violations.ContactNoteDate__raw,
      us_tn_Violations.ContactNoteTime,
      us_tn_Violations.ContactNoteType,
      us_tn_Violations.LastUpdateUserId,
      us_tn_Violations.file_id,
      us_tn_Violations.is_deleted]
    sorts: [us_tn_Violations.ContactNoteDate__raw]
    note_display: hover
    note_text: "TODO(#11323): fill out description once received."
    listen: 
      View Type: us_tn_OffenderName.view_type
      US_TN_DOC: us_tn_OffenderName.OffenderID
    row: 144
    col: 0
    width: 24
    height: 6

