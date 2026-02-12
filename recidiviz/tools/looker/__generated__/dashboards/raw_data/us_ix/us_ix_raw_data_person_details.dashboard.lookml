# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_ix_raw_data_person_details
  title: Idaho ATLAS Raw Data Person Details
  description: For examining individuals in US_IX's raw data tables
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
    explore: us_ix_raw_data
    field: us_ix_ind_Offender.view_type

  - name: US_IX_DOC
    title: US_IX_DOC
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{model_name}"
    explore: us_ix_raw_data
    field: us_ix_ind_Offender.OffenderId

  elements:
  - name: ind_Offender
    title: ind_Offender
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_ind_Offender.primary_key,
      us_ix_ind_Offender.OffenderId,
      us_ix_ind_Offender.ODS_NUM,
      us_ix_ind_Offender.BirthDate__raw,
      us_ix_ind_Offender.ReligionId,
      us_ix_ind_Offender.GenderId,
      us_ix_ind_Offender.RaceId,
      us_ix_ind_Offender.EmployeeId,
      us_ix_ind_Offender.MaritalStatusId,
      us_ix_ind_Offender.EthnicOriginId,
      us_ix_ind_Offender.EnglishNotSpoken,
      us_ix_ind_Offender.LanguageId,
      us_ix_ind_Offender.CitizenshipId,
      us_ix_ind_Offender.RegistrationDate,
      us_ix_ind_Offender.RegisteredSexOffender,
      us_ix_ind_Offender.DnaNotRequired,
      us_ix_ind_Offender.DnaSampleDate,
      us_ix_ind_Offender.ReportToIns,
      us_ix_ind_Offender.PlaceOfBirthCity,
      us_ix_ind_Offender.PlaceOfBirthCountryId,
      us_ix_ind_Offender.PlaceOfBirthStateId,
      us_ix_ind_Offender.Height,
      us_ix_ind_Offender.HeightFeet,
      us_ix_ind_Offender.HeightInches,
      us_ix_ind_Offender.Weight,
      us_ix_ind_Offender.HasGlasses,
      us_ix_ind_Offender.HasContacts,
      us_ix_ind_Offender.EyeColorId,
      us_ix_ind_Offender.HairColorId,
      us_ix_ind_Offender.ComplexionId,
      us_ix_ind_Offender.PhysicalAppearanceNotes,
      us_ix_ind_Offender.LiterateReadSelfReport,
      us_ix_ind_Offender.LiterateWriteSelfReport,
      us_ix_ind_Offender.LiterateReadTest,
      us_ix_ind_Offender.LiterateWriteTest,
      us_ix_ind_Offender.EducationHighestLevel,
      us_ix_ind_Offender.LastSchoolAttended,
      us_ix_ind_Offender.OccupationId,
      us_ix_ind_Offender.NeverEmployed,
      us_ix_ind_Offender.Locking,
      us_ix_ind_Offender.InsertUserId,
      us_ix_ind_Offender.InsertDate__raw,
      us_ix_ind_Offender.UpdateUserId,
      us_ix_ind_Offender.UpdateDate__raw,
      us_ix_ind_Offender.CurrentLocationId,
      us_ix_ind_Offender.AnnualReviewDate,
      us_ix_ind_Offender.LatestDNAConfirmationDate,
      us_ix_ind_Offender.MaximumVisitationHour,
      us_ix_ind_Offender.InterviewDay,
      us_ix_ind_Offender.ApproximateAgeRecordedDate,
      us_ix_ind_Offender.AgeEstimated,
      us_ix_ind_Offender.DNARefusalDate,
      us_ix_ind_Offender.HeightCm,
      us_ix_ind_Offender.WeightGr,
      us_ix_ind_Offender.OccupationInterestedIn,
      us_ix_ind_Offender.OccupationOther,
      us_ix_ind_Offender.file_id,
      us_ix_ind_Offender.is_deleted]
    sorts: [us_ix_ind_Offender.BirthDate__raw]
    note_display: hover
    note_text: "This table contains demographic information for each individual.   Each row represents one individual found in the Atlas system."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 0
    col: 0
    width: 24
    height: 6

  - name: asm_Assessment
    title: asm_Assessment
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_asm_Assessment.primary_key,
      us_ix_asm_Assessment.AssessmentId,
      us_ix_asm_Assessment.OffenderId,
      us_ix_asm_Assessment.AssessmentTypeId,
      us_ix_asm_Assessment.AssessmentToolId,
      us_ix_asm_Assessment.AssessmentDegreeId,
      us_ix_asm_Assessment.AssessorTypeId,
      us_ix_asm_Assessment.ServiceProviderId,
      us_ix_asm_Assessment.EmployeeId,
      us_ix_asm_Assessment.ExternalAssessorNameDesc,
      us_ix_asm_Assessment.Notes,
      us_ix_asm_Assessment.OtherRequestorDesc,
      us_ix_asm_Assessment.OverallScore,
      us_ix_asm_Assessment.RequestDate__raw,
      us_ix_asm_Assessment.RequestorTypeId,
      us_ix_asm_Assessment.ResultNote,
      us_ix_asm_Assessment.Result,
      us_ix_asm_Assessment.ScheduleDate__raw,
      us_ix_asm_Assessment.ScoreSheetId,
      us_ix_asm_Assessment.CompletionDate__raw,
      us_ix_asm_Assessment.CostToDOC,
      us_ix_asm_Assessment.CostToOffender,
      us_ix_asm_Assessment.Locking,
      us_ix_asm_Assessment.InsertUserId,
      us_ix_asm_Assessment.InsertDate,
      us_ix_asm_Assessment.UpdateUserId,
      us_ix_asm_Assessment.UpdateDate,
      us_ix_asm_Assessment.AssessmentStatusId,
      us_ix_asm_Assessment.AssessmentTypeResultId,
      us_ix_asm_Assessment.file_id,
      us_ix_asm_Assessment.is_deleted]
    sorts: [us_ix_asm_Assessment.RequestDate__raw]
    note_display: hover
    note_text: "A table describing assessments in Idaho, with one occurrence for each assessment instance."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 6
    col: 0
    width: 24
    height: 6

  - name: com_CommunityServiceRecord
    title: com_CommunityServiceRecord
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_com_CommunityServiceRecord.primary_key,
      us_ix_com_CommunityServiceRecord.CommunityServiceRecordId,
      us_ix_com_CommunityServiceRecord.OffenderId,
      us_ix_com_CommunityServiceRecord.StartDate__raw,
      us_ix_com_CommunityServiceRecord.EndDate__raw,
      us_ix_com_CommunityServiceRecord.TimeServedSeconds,
      us_ix_com_CommunityServiceRecord.WorkSite,
      us_ix_com_CommunityServiceRecord.Comment,
      us_ix_com_CommunityServiceRecord.Inactive,
      us_ix_com_CommunityServiceRecord.Locking,
      us_ix_com_CommunityServiceRecord.InsertUserId,
      us_ix_com_CommunityServiceRecord.InsertDate__raw,
      us_ix_com_CommunityServiceRecord.UpdateUserId,
      us_ix_com_CommunityServiceRecord.UpdateDate__raw,
      us_ix_com_CommunityServiceRecord.file_id,
      us_ix_com_CommunityServiceRecord.is_deleted]
    sorts: [us_ix_com_CommunityServiceRecord.StartDate__raw]
    note_display: hover
    note_text: "Atlas table for community service records"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 12
    col: 0
    width: 24
    height: 6

  - name: com_Investigation
    title: com_Investigation
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_com_Investigation.primary_key,
      us_ix_com_Investigation.InvestigationId,
      us_ix_com_Investigation.RequestDate__raw,
      us_ix_com_Investigation.RequestingAgencyId,
      us_ix_com_Investigation.InvestigationTypeId,
      us_ix_com_Investigation.RequestingStaffId,
      us_ix_com_Investigation.ReceivingDOCLocationId,
      us_ix_com_Investigation.TransferReasonId,
      us_ix_com_Investigation.RequestNotes,
      us_ix_com_Investigation.AssignedById,
      us_ix_com_Investigation.AssignedToId,
      us_ix_com_Investigation.AssignedDate,
      us_ix_com_Investigation.DueDate,
      us_ix_com_Investigation.CompletionDate,
      us_ix_com_Investigation.AssociatedReportId,
      us_ix_com_Investigation.Accepted,
      us_ix_com_Investigation.Cancelled,
      us_ix_com_Investigation.ResponseNotes,
      us_ix_com_Investigation.OffenderId,
      us_ix_com_Investigation.Locking,
      us_ix_com_Investigation.InsertUserId,
      us_ix_com_Investigation.InsertDate,
      us_ix_com_Investigation.UpdateUserId,
      us_ix_com_Investigation.UpdateDate,
      us_ix_com_Investigation.ProposedOffenderAddressId,
      us_ix_com_Investigation.OtherDOCLocationId,
      us_ix_com_Investigation.OtherAssignedById,
      us_ix_com_Investigation.OtherAssignedToId,
      us_ix_com_Investigation.OtherAssignedDate,
      us_ix_com_Investigation.OtherDueDate,
      us_ix_com_Investigation.InvestigationStatusId,
      us_ix_com_Investigation.file_id,
      us_ix_com_Investigation.is_deleted]
    sorts: [us_ix_com_Investigation.RequestDate__raw]
    note_display: hover
    note_text: "TODO(#15329): Fill in the file description"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 18
    col: 0
    width: 24
    height: 6

  - name: com_PSIReport
    title: com_PSIReport
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_com_PSIReport.primary_key,
      us_ix_com_PSIReport.PSIReportId,
      us_ix_com_PSIReport.LocationId,
      us_ix_com_PSIReport.CourtId,
      us_ix_com_PSIReport.AssignedByUserId,
      us_ix_com_PSIReport.AssignedToUserId,
      us_ix_com_PSIReport.OrderDate__raw,
      us_ix_com_PSIReport.ReceivedDate__raw,
      us_ix_com_PSIReport.SentenceDate__raw,
      us_ix_com_PSIReport.CompletedReasonId,
      us_ix_com_PSIReport.CompletedDate__raw,
      us_ix_com_PSIReport.DueDate__raw,
      us_ix_com_PSIReport.FinalizedReasonId,
      us_ix_com_PSIReport.AssignedDate__raw,
      us_ix_com_PSIReport.FinalizedDate__raw,
      us_ix_com_PSIReport.Notes,
      us_ix_com_PSIReport.Addendum,
      us_ix_com_PSIReport.TrialDisposition,
      us_ix_com_PSIReport.OffenderId,
      us_ix_com_PSIReport.PrintDropdownId,
      us_ix_com_PSIReport.IsLegacy,
      us_ix_com_PSIReport.Locking,
      us_ix_com_PSIReport.InsertUserId,
      us_ix_com_PSIReport.InsertDate__raw,
      us_ix_com_PSIReport.UpdateUserId,
      us_ix_com_PSIReport.UpdateDate__raw,
      us_ix_com_PSIReport.InvestigationStatusId,
      us_ix_com_PSIReport.ApprovedByEmployeeId,
      us_ix_com_PSIReport.ApprovedDate__raw,
      us_ix_com_PSIReport.ValuesAndOutlook,
      us_ix_com_PSIReport.Recommendation,
      us_ix_com_PSIReport.OpeningStatement,
      us_ix_com_PSIReport.file_id,
      us_ix_com_PSIReport.is_deleted]
    sorts: [us_ix_com_PSIReport.OrderDate__raw]
    note_display: hover
    note_text: "This table contains a record of each pre-sentence investigation record in Idaho.  Contains information from the module that pre-sentence investigators fill out when they're first assigned a PSI report."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 24
    col: 0
    width: 24
    height: 6

  - name: com_PhysicalLocation
    title: com_PhysicalLocation
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_com_PhysicalLocation.primary_key,
      us_ix_com_PhysicalLocation.PhysicalLocationId,
      us_ix_com_PhysicalLocation.OffenderId,
      us_ix_com_PhysicalLocation.LocationChangeStartDate,
      us_ix_com_PhysicalLocation.LocationChangeEndDate,
      us_ix_com_PhysicalLocation.PhysicalLocationTypeId,
      us_ix_com_PhysicalLocation.LocationId,
      us_ix_com_PhysicalLocation.OtherLocation,
      us_ix_com_PhysicalLocation.Notes,
      us_ix_com_PhysicalLocation.Locking,
      us_ix_com_PhysicalLocation.InsertUserId,
      us_ix_com_PhysicalLocation.InsertDate,
      us_ix_com_PhysicalLocation.UpdateUserId,
      us_ix_com_PhysicalLocation.UpdateDate,
      us_ix_com_PhysicalLocation.file_id,
      us_ix_com_PhysicalLocation.is_deleted]
    sorts: [us_ix_com_PhysicalLocation.PhysicalLocationId]
    note_display: hover
    note_text: "TODO(#15329): Fill in the file description"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 30
    col: 0
    width: 24
    height: 6

  - name: com_Transfer
    title: com_Transfer
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_com_Transfer.primary_key,
      us_ix_com_Transfer.TransferId,
      us_ix_com_Transfer.OffenderId,
      us_ix_com_Transfer.TransferDate__raw,
      us_ix_com_Transfer.TransferTypeId,
      us_ix_com_Transfer.TransferReasonId,
      us_ix_com_Transfer.DOCLocationFromId,
      us_ix_com_Transfer.DOCLocationToId,
      us_ix_com_Transfer.JurisdictionFromId,
      us_ix_com_Transfer.JurisdictionToId,
      us_ix_com_Transfer.Notes,
      us_ix_com_Transfer.OfficerName,
      us_ix_com_Transfer.OfficerLocation,
      us_ix_com_Transfer.OfficerTelephone,
      us_ix_com_Transfer.CustodyReleaseDate__raw,
      us_ix_com_Transfer.SupervisionReleaseDate__raw,
      us_ix_com_Transfer.HearingDate__raw,
      us_ix_com_Transfer.ScheduledDate__raw,
      us_ix_com_Transfer.CancelledReason,
      us_ix_com_Transfer.ScheduledFor,
      us_ix_com_Transfer.ApprovedBy,
      us_ix_com_Transfer.Locking,
      us_ix_com_Transfer.InsertUserId,
      us_ix_com_Transfer.InsertDate__raw,
      us_ix_com_Transfer.UpdateUserId,
      us_ix_com_Transfer.UpdateDate__raw,
      us_ix_com_Transfer.TransferStatusId,
      us_ix_com_Transfer.CancellationDate__raw,
      us_ix_com_Transfer.ApprovedDate__raw,
      us_ix_com_Transfer.TermId,
      us_ix_com_Transfer.BedTypeId,
      us_ix_com_Transfer.KeepSeparateReason,
      us_ix_com_Transfer.TransferApprovalId,
      us_ix_com_Transfer.EmergencyTransfer,
      us_ix_com_Transfer.file_id,
      us_ix_com_Transfer.is_deleted]
    sorts: [us_ix_com_Transfer.TransferDate__raw]
    note_display: hover
    note_text: "This table contains a record of each movement into and out of DOC jurisdiction, and movements between different locations within the DOC."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 36
    col: 0
    width: 24
    height: 6

  - name: crs_OfdCourseEnrollment
    title: crs_OfdCourseEnrollment
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_crs_OfdCourseEnrollment.primary_key,
      us_ix_crs_OfdCourseEnrollment.OfdCourseEnrollmentId,
      us_ix_crs_OfdCourseEnrollment.OffenderId,
      us_ix_crs_OfdCourseEnrollment.CourseId,
      us_ix_crs_OfdCourseEnrollment.PendingEnrollment,
      us_ix_crs_OfdCourseEnrollment.PendingEnrollmentDate__raw,
      us_ix_crs_OfdCourseEnrollment.CourseSectionId,
      us_ix_crs_OfdCourseEnrollment.OfdEnrollmentStatusId,
      us_ix_crs_OfdCourseEnrollment.StartDate__raw,
      us_ix_crs_OfdCourseEnrollment.EndDate__raw,
      us_ix_crs_OfdCourseEnrollment.EnrollmentStatusDate__raw,
      us_ix_crs_OfdCourseEnrollment.Locking,
      us_ix_crs_OfdCourseEnrollment.InsertUserId,
      us_ix_crs_OfdCourseEnrollment.InsertDate__raw,
      us_ix_crs_OfdCourseEnrollment.UpdateUserId,
      us_ix_crs_OfdCourseEnrollment.UpdateDate__raw,
      us_ix_crs_OfdCourseEnrollment.MasterPass,
      us_ix_crs_OfdCourseEnrollment.Priority,
      us_ix_crs_OfdCourseEnrollment.PendingEnrollmentComment,
      us_ix_crs_OfdCourseEnrollment.RemovalPendingDate__raw,
      us_ix_crs_OfdCourseEnrollment.WaitingListRemovalReasonId,
      us_ix_crs_OfdCourseEnrollment.OtherRemovalDescription,
      us_ix_crs_OfdCourseEnrollment.RemovalPendingComment,
      us_ix_crs_OfdCourseEnrollment.IsCourtOrdered,
      us_ix_crs_OfdCourseEnrollment.IsSanctioned,
      us_ix_crs_OfdCourseEnrollment.ParticipantId,
      us_ix_crs_OfdCourseEnrollment.EventParticipantId,
      us_ix_crs_OfdCourseEnrollment.IsRequestedByOffender,
      us_ix_crs_OfdCourseEnrollment.CollegeGradeId,
      us_ix_crs_OfdCourseEnrollment.FinalGradeOther,
      us_ix_crs_OfdCourseEnrollment.IsRequiredForRelease,
      us_ix_crs_OfdCourseEnrollment.file_id,
      us_ix_crs_OfdCourseEnrollment.is_deleted]
    sorts: [us_ix_crs_OfdCourseEnrollment.PendingEnrollmentDate__raw]
    note_display: hover
    note_text: "Atlas table holding programming course enrollment information"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 42
    col: 0
    width: 24
    height: 6

  - name: drg_DrugTestResult
    title: drg_DrugTestResult
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_drg_DrugTestResult.primary_key,
      us_ix_drg_DrugTestResult.DrugTestResultId,
      us_ix_drg_DrugTestResult.OffenderId,
      us_ix_drg_DrugTestResult.CollectionDate__raw,
      us_ix_drg_DrugTestResult.ColorGroupId,
      us_ix_drg_DrugTestResult.NoShow,
      us_ix_drg_DrugTestResult.ExcusedException,
      us_ix_drg_DrugTestResult.UnableToProvide,
      us_ix_drg_DrugTestResult.AllNegative,
      us_ix_drg_DrugTestResult.Bac,
      us_ix_drg_DrugTestResult.PrimaryOfficerId,
      us_ix_drg_DrugTestResult.LocationId,
      us_ix_drg_DrugTestResult.Locking,
      us_ix_drg_DrugTestResult.InsertUserId,
      us_ix_drg_DrugTestResult.InsertDate,
      us_ix_drg_DrugTestResult.PostedDate,
      us_ix_drg_DrugTestResult.UpdateUserId,
      us_ix_drg_DrugTestResult.UpdateDate,
      us_ix_drg_DrugTestResult.CollectedById,
      us_ix_drg_DrugTestResult.TestingMethodId,
      us_ix_drg_DrugTestResult.FacTestReasonId,
      us_ix_drg_DrugTestResult.IsDrugTestFacility,
      us_ix_drg_DrugTestResult.Cancelled,
      us_ix_drg_DrugTestResult.SampleRejected,
      us_ix_drg_DrugTestResult.AbnormalResult,
      us_ix_drg_DrugTestResult.RefusalToSubmit,
      us_ix_drg_DrugTestResult.TakingMedications,
      us_ix_drg_DrugTestResult.Medication,
      us_ix_drg_DrugTestResult.VerifiedByMedicalDepartment,
      us_ix_drg_DrugTestResult.RejectionOrAbnormalComments,
      us_ix_drg_DrugTestResult.GCMSComments,
      us_ix_drg_DrugTestResult.DateSentToEx__raw,
      us_ix_drg_DrugTestResult.DateReturnedFromEx__raw,
      us_ix_drg_DrugTestResult.SampleNumber,
      us_ix_drg_DrugTestResult.FacTestResultId,
      us_ix_drg_DrugTestResult.SentForConfirmation,
      us_ix_drg_DrugTestResult.TestCancellationReasonId,
      us_ix_drg_DrugTestResult.file_id,
      us_ix_drg_DrugTestResult.is_deleted]
    sorts: [us_ix_drg_DrugTestResult.CollectionDate__raw]
    note_display: hover
    note_text: "TODO(#15329): Fill in the file description"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 48
    col: 0
    width: 24
    height: 6

  - name: dsc_DACase
    title: dsc_DACase
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_dsc_DACase.primary_key,
      us_ix_dsc_DACase.DACaseId,
      us_ix_dsc_DACase.CaseNo,
      us_ix_dsc_DACase.TermId,
      us_ix_dsc_DACase.LocationId,
      us_ix_dsc_DACase.OffenseDateTime__raw,
      us_ix_dsc_DACase.OffenseDesc,
      us_ix_dsc_DACase.PhysicalEvidence,
      us_ix_dsc_DACase.ImmediateAction,
      us_ix_dsc_DACase.DiscOffenseRptId,
      us_ix_dsc_DACase.AuditHearingDate__raw,
      us_ix_dsc_DACase.AuditAppealDate__raw,
      us_ix_dsc_DACase.PreviousActionId,
      us_ix_dsc_DACase.InternalLocationId,
      us_ix_dsc_DACase.InternalLocationAreaId,
      us_ix_dsc_DACase.LocationOfOffenceOther,
      us_ix_dsc_DACase.FacilityLevelId,
      us_ix_dsc_DACase.DAProcedureStatusId,
      us_ix_dsc_DACase.OffenderId,
      us_ix_dsc_DACase.Locking,
      us_ix_dsc_DACase.InsertUserId,
      us_ix_dsc_DACase.InsertDate__raw,
      us_ix_dsc_DACase.UpdateUserId,
      us_ix_dsc_DACase.UpdateDate__raw,
      us_ix_dsc_DACase.IncidentReportId,
      us_ix_dsc_DACase.ViolenceLevelId,
      us_ix_dsc_DACase.file_id,
      us_ix_dsc_DACase.is_deleted]
    sorts: [us_ix_dsc_DACase.OffenseDateTime__raw]
    note_display: hover
    note_text: "Contains information about DA cases."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 54
    col: 0
    width: 24
    height: 6

  - name: dsc_DAProcedure
    title: dsc_DAProcedure
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_dsc_DAProcedure.primary_key,
      us_ix_dsc_DAProcedure.DAProcedureId,
      us_ix_dsc_DAProcedure.DACaseId,
      us_ix_dsc_DAProcedure.DAProcedureTypeId,
      us_ix_dsc_DAProcedure.DorOffenseTypeId,
      us_ix_dsc_DAProcedure.InsertUserId,
      us_ix_dsc_DAProcedure.InsertDate__raw,
      us_ix_dsc_DAProcedure.UpdateUserId,
      us_ix_dsc_DAProcedure.UpdateDate__raw,
      us_ix_dsc_DAProcedure.Locking,
      us_ix_dsc_DAProcedure.OffenderId,
      us_ix_dsc_DAProcedure.file_id,
      us_ix_dsc_DAProcedure.is_deleted]
    sorts: [us_ix_dsc_DAProcedure.InsertDate__raw]
    note_display: hover
    note_text: "Atlas table with disciplinary action (DA) procedure information"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 60
    col: 0
    width: 24
    height: 6

  - name: fin_AccountOwner
    title: fin_AccountOwner
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_fin_AccountOwner.primary_key,
      us_ix_fin_AccountOwner.AccountOwnerId,
      us_ix_fin_AccountOwner.TrustAccountTypeId,
      us_ix_fin_AccountOwner.GeneralLedgerGroupId,
      us_ix_fin_AccountOwner.LocationId,
      us_ix_fin_AccountOwner.OffenderId,
      us_ix_fin_AccountOwner.InsertUserId,
      us_ix_fin_AccountOwner.InsertDate__raw,
      us_ix_fin_AccountOwner.UpdateUserId,
      us_ix_fin_AccountOwner.UpdateDate__raw,
      us_ix_fin_AccountOwner.Locking,
      us_ix_fin_AccountOwner.file_id,
      us_ix_fin_AccountOwner.is_deleted]
    sorts: [us_ix_fin_AccountOwner.InsertDate__raw]
    note_display: hover
    note_text: "Contains account owner records linking offenders to their financial accounts. Each row represents an account owner, associating an offender with a trust account type and location."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 66
    col: 0
    width: 24
    height: 6

  - name: gsm_ParticipantOffender
    title: gsm_ParticipantOffender
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_gsm_ParticipantOffender.primary_key,
      us_ix_gsm_ParticipantOffender.ParticipantId,
      us_ix_gsm_ParticipantOffender.ClassCode,
      us_ix_gsm_ParticipantOffender.OffenderId,
      us_ix_gsm_ParticipantOffender.InsertUserId,
      us_ix_gsm_ParticipantOffender.Locking,
      us_ix_gsm_ParticipantOffender.InsertDate__raw,
      us_ix_gsm_ParticipantOffender.UpdateUserId,
      us_ix_gsm_ParticipantOffender.UpdateDate__raw,
      us_ix_gsm_ParticipantOffender.file_id,
      us_ix_gsm_ParticipantOffender.is_deleted]
    sorts: [us_ix_gsm_ParticipantOffender.InsertDate__raw]
    note_display: hover
    note_text: "Maps participant information to offender information"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 72
    col: 0
    width: 24
    height: 6

  - name: hsn_BedAssignment
    title: hsn_BedAssignment
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_hsn_BedAssignment.primary_key,
      us_ix_hsn_BedAssignment.BedAssignmentId,
      us_ix_hsn_BedAssignment.BedId,
      us_ix_hsn_BedAssignment.OffenderId,
      us_ix_hsn_BedAssignment.FromDate,
      us_ix_hsn_BedAssignment.ToDate,
      us_ix_hsn_BedAssignment.ChangeReasonId,
      us_ix_hsn_BedAssignment.Locking,
      us_ix_hsn_BedAssignment.InsertUserId,
      us_ix_hsn_BedAssignment.UpdateUserId,
      us_ix_hsn_BedAssignment.InsertDate,
      us_ix_hsn_BedAssignment.UpdateDate,
      us_ix_hsn_BedAssignment.AuditSecurityLevelId,
      us_ix_hsn_BedAssignment.file_id,
      us_ix_hsn_BedAssignment.is_deleted]
    sorts: [us_ix_hsn_BedAssignment.BedAssignmentId]
    note_display: hover
    note_text: "Describes periods over which a given offender is assigned to a given bed."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 78
    col: 0
    width: 24
    height: 6

  - name: ind_AliasName
    title: ind_AliasName
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_ind_AliasName.primary_key,
      us_ix_ind_AliasName.AliasNameId,
      us_ix_ind_AliasName.AliasNameTypeId,
      us_ix_ind_AliasName.OffenderId,
      us_ix_ind_AliasName.FirstName,
      us_ix_ind_AliasName.MiddleName,
      us_ix_ind_AliasName.LastName,
      us_ix_ind_AliasName.NameSuffixTypeId,
      us_ix_ind_AliasName.SoundexFirstName,
      us_ix_ind_AliasName.SoundexMiddleName,
      us_ix_ind_AliasName.SoundexLastName,
      us_ix_ind_AliasName.Notes,
      us_ix_ind_AliasName.Inactive,
      us_ix_ind_AliasName.Locking,
      us_ix_ind_AliasName.InsertUserId,
      us_ix_ind_AliasName.InsertDate__raw,
      us_ix_ind_AliasName.UpdateUserId,
      us_ix_ind_AliasName.UpdateDate__raw,
      us_ix_ind_AliasName.DisplayName,
      us_ix_ind_AliasName.OrganizationalUnitId,
      us_ix_ind_AliasName.file_id,
      us_ix_ind_AliasName.is_deleted]
    sorts: [us_ix_ind_AliasName.InsertDate__raw]
    note_display: hover
    note_text: "This table contains information about recorded names/aliases for each person.  A person may have one or multiple alias records in this table."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 84
    col: 0
    width: 24
    height: 6

  - name: ind_EmploymentHistory
    title: ind_EmploymentHistory
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_ind_EmploymentHistory.primary_key,
      us_ix_ind_EmploymentHistory.EmploymentHistoryId,
      us_ix_ind_EmploymentHistory.StartDate,
      us_ix_ind_EmploymentHistory.EndDate,
      us_ix_ind_EmploymentHistory.MonthlySalary,
      us_ix_ind_EmploymentHistory.EmployerId,
      us_ix_ind_EmploymentHistory.JobTitle,
      us_ix_ind_EmploymentHistory.Shift,
      us_ix_ind_EmploymentHistory.EmploymentStatusId,
      us_ix_ind_EmploymentHistory.PrimaryEmployment,
      us_ix_ind_EmploymentHistory.ActualProposedId,
      us_ix_ind_EmploymentHistory.Comments,
      us_ix_ind_EmploymentHistory.OffenderId,
      us_ix_ind_EmploymentHistory.Inactive,
      us_ix_ind_EmploymentHistory.Locking,
      us_ix_ind_EmploymentHistory.InsertUserId,
      us_ix_ind_EmploymentHistory.InsertDate,
      us_ix_ind_EmploymentHistory.UpdateUserId,
      us_ix_ind_EmploymentHistory.UpdateDate,
      us_ix_ind_EmploymentHistory.Wage,
      us_ix_ind_EmploymentHistory.EmploymentWageFrequencyId,
      us_ix_ind_EmploymentHistory.HoursPerWeek,
      us_ix_ind_EmploymentHistory.SupervisorName,
      us_ix_ind_EmploymentHistory.file_id,
      us_ix_ind_EmploymentHistory.is_deleted]
    sorts: [us_ix_ind_EmploymentHistory.EmploymentHistoryId]
    note_display: hover
    note_text: "Details each offender's employment history in the form of distinct employment periods."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 90
    col: 0
    width: 24
    height: 6

  - name: ind_OffenderInternalStatus
    title: ind_OffenderInternalStatus
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_ind_OffenderInternalStatus.primary_key,
      us_ix_ind_OffenderInternalStatus.OffenderInternalStatusId,
      us_ix_ind_OffenderInternalStatus.OffenderId,
      us_ix_ind_OffenderInternalStatus.ApprovedDate__raw,
      us_ix_ind_OffenderInternalStatus.InternalStatusId,
      us_ix_ind_OffenderInternalStatus.ApprovedBy,
      us_ix_ind_OffenderInternalStatus.AuditLocationId,
      us_ix_ind_OffenderInternalStatus.Inactive,
      us_ix_ind_OffenderInternalStatus.Locking,
      us_ix_ind_OffenderInternalStatus.InsertUserId,
      us_ix_ind_OffenderInternalStatus.InsertDate__raw,
      us_ix_ind_OffenderInternalStatus.UpdateUserId,
      us_ix_ind_OffenderInternalStatus.UpdateDate__raw,
      us_ix_ind_OffenderInternalStatus.Comment,
      us_ix_ind_OffenderInternalStatus.EffectiveDate__raw,
      us_ix_ind_OffenderInternalStatus.file_id,
      us_ix_ind_OffenderInternalStatus.is_deleted]
    sorts: [us_ix_ind_OffenderInternalStatus.ApprovedDate__raw]
    note_display: hover
    note_text: "Contains records of internal status assignments for offenders. Each row represents an internal status assigned to an offender, including the approval date, approving user, and any associated comments."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 96
    col: 0
    width: 24
    height: 6

  - name: ind_OffenderNoteInfo
    title: ind_OffenderNoteInfo
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_ind_OffenderNoteInfo.primary_key,
      us_ix_ind_OffenderNoteInfo.OffenderNoteInfoId,
      us_ix_ind_OffenderNoteInfo.OffenderId,
      us_ix_ind_OffenderNoteInfo.NoteTypeId,
      us_ix_ind_OffenderNoteInfo.DOCLocationId,
      us_ix_ind_OffenderNoteInfo.StaffId,
      us_ix_ind_OffenderNoteInfo.CaseReviewId,
      us_ix_ind_OffenderNoteInfo.SupervisorId,
      us_ix_ind_OffenderNoteInfo.SupervisorReviewDate,
      us_ix_ind_OffenderNoteInfo.Locking,
      us_ix_ind_OffenderNoteInfo.InsertUserId,
      us_ix_ind_OffenderNoteInfo.InsertDate__raw,
      us_ix_ind_OffenderNoteInfo.UpdateUserId,
      us_ix_ind_OffenderNoteInfo.UpdateDate__raw,
      us_ix_ind_OffenderNoteInfo.NoteDate__raw,
      us_ix_ind_OffenderNoteInfo.OffenderNoteStatusId,
      us_ix_ind_OffenderNoteInfo.DraftNote,
      us_ix_ind_OffenderNoteInfo.IsManual,
      us_ix_ind_OffenderNoteInfo.file_id,
      us_ix_ind_OffenderNoteInfo.is_deleted]
    sorts: [us_ix_ind_OffenderNoteInfo.InsertDate__raw]
    note_display: hover
    note_text: "This table contains metadata for notes written by supervision officers and other agents about their interactions or observations of individuals on supervision. This table can also be used to group notes into contacts, since any notes from the same contact will share the same OffenderNoteInfoId."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 102
    col: 0
    width: 24
    height: 6

  - name: ind_OffenderSecurityLevel
    title: ind_OffenderSecurityLevel
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_ind_OffenderSecurityLevel.primary_key,
      us_ix_ind_OffenderSecurityLevel.OffenderSecurityLevelId,
      us_ix_ind_OffenderSecurityLevel.OffenderId,
      us_ix_ind_OffenderSecurityLevel.ApprovedDate__raw,
      us_ix_ind_OffenderSecurityLevel.ApprovedBy,
      us_ix_ind_OffenderSecurityLevel.Comment,
      us_ix_ind_OffenderSecurityLevel.Score,
      us_ix_ind_OffenderSecurityLevel.SecurityLevelSegregationId,
      us_ix_ind_OffenderSecurityLevel.SecurityLevelOverrideId,
      us_ix_ind_OffenderSecurityLevel.AuditLocationID,
      us_ix_ind_OffenderSecurityLevel.SecurityLevelReviewTypeId,
      us_ix_ind_OffenderSecurityLevel.Locking,
      us_ix_ind_OffenderSecurityLevel.InsertUserId,
      us_ix_ind_OffenderSecurityLevel.UpdateUserId,
      us_ix_ind_OffenderSecurityLevel.InsertDate__raw,
      us_ix_ind_OffenderSecurityLevel.UpdateDate__raw,
      us_ix_ind_OffenderSecurityLevel.SecurityLevelMandatoryOverrideId,
      us_ix_ind_OffenderSecurityLevel.AdjustedSecurityLevelId,
      us_ix_ind_OffenderSecurityLevel.CalculatedSecurityLevelId,
      us_ix_ind_OffenderSecurityLevel.SecurityLevelId,
      us_ix_ind_OffenderSecurityLevel.file_id,
      us_ix_ind_OffenderSecurityLevel.is_deleted]
    sorts: [us_ix_ind_OffenderSecurityLevel.ApprovedDate__raw]
    note_display: hover
    note_text: "This table contains Atlas information on each person's security level"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 108
    col: 0
    width: 24
    height: 6

  - name: ind_Offender_Address
    title: ind_Offender_Address
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_ind_Offender_Address.primary_key,
      us_ix_ind_Offender_Address.Offender_AddressId,
      us_ix_ind_Offender_Address.AddressId,
      us_ix_ind_Offender_Address.OffenderId,
      us_ix_ind_Offender_Address.PrimaryAddress,
      us_ix_ind_Offender_Address.ResidingWith,
      us_ix_ind_Offender_Address.RelationshipQualifierTypeId,
      us_ix_ind_Offender_Address.RelationshipTypeId,
      us_ix_ind_Offender_Address.StartDate__raw,
      us_ix_ind_Offender_Address.EndDate__raw,
      us_ix_ind_Offender_Address.ReportedDate,
      us_ix_ind_Offender_Address.ValidatedDate,
      us_ix_ind_Offender_Address.AddressTypeId,
      us_ix_ind_Offender_Address.Locking,
      us_ix_ind_Offender_Address.InsertUserId,
      us_ix_ind_Offender_Address.InsertDate__raw,
      us_ix_ind_Offender_Address.UpdateUserId,
      us_ix_ind_Offender_Address.UpdateDate__raw,
      us_ix_ind_Offender_Address.ReleasePlanTypeId,
      us_ix_ind_Offender_Address.OffenderPhoneId,
      us_ix_ind_Offender_Address.HomelessTypeId,
      us_ix_ind_Offender_Address.file_id,
      us_ix_ind_Offender_Address.is_deleted]
    sorts: [us_ix_ind_Offender_Address.StartDate__raw]
    note_display: hover
    note_text: "This table contains information on periods of residence for each individual and address.  Each row of this table links an individual with a specific address record and contains information on the starting and ending date of residence at that address."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 114
    col: 0
    width: 24
    height: 6

  - name: ind_Offender_Alert
    title: ind_Offender_Alert
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_ind_Offender_Alert.primary_key,
      us_ix_ind_Offender_Alert.Offender_AlertId,
      us_ix_ind_Offender_Alert.StartDate__raw,
      us_ix_ind_Offender_Alert.EndDate,
      us_ix_ind_Offender_Alert.FrequencyId,
      us_ix_ind_Offender_Alert.Notes,
      us_ix_ind_Offender_Alert.OffenderId,
      us_ix_ind_Offender_Alert.AlertId,
      us_ix_ind_Offender_Alert.Locking,
      us_ix_ind_Offender_Alert.InsertUserId,
      us_ix_ind_Offender_Alert.InsertDate__raw,
      us_ix_ind_Offender_Alert.UpdateUserId,
      us_ix_ind_Offender_Alert.UpdateDate__raw,
      us_ix_ind_Offender_Alert.NotesUpdateUserId,
      us_ix_ind_Offender_Alert.NotesUpdateDate,
      us_ix_ind_Offender_Alert.file_id,
      us_ix_ind_Offender_Alert.is_deleted]
    sorts: [us_ix_ind_Offender_Alert.StartDate__raw]
    note_display: hover
    note_text: "This table contains records of IDOC clients' alerts."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 120
    col: 0
    width: 24
    height: 6

  - name: ind_Offender_EmailAddress
    title: ind_Offender_EmailAddress
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_ind_Offender_EmailAddress.primary_key,
      us_ix_ind_Offender_EmailAddress.Offender_EmailAddressId,
      us_ix_ind_Offender_EmailAddress.EmailAddressId,
      us_ix_ind_Offender_EmailAddress.OffenderId,
      us_ix_ind_Offender_EmailAddress.PrimaryEmailAddress,
      us_ix_ind_Offender_EmailAddress.Locking,
      us_ix_ind_Offender_EmailAddress.InsertUserId,
      us_ix_ind_Offender_EmailAddress.InsertDate__raw,
      us_ix_ind_Offender_EmailAddress.UpdateUserId,
      us_ix_ind_Offender_EmailAddress.UpdateDate__raw,
      us_ix_ind_Offender_EmailAddress.file_id,
      us_ix_ind_Offender_EmailAddress.is_deleted]
    sorts: [us_ix_ind_Offender_EmailAddress.InsertDate__raw]
    note_display: hover
    note_text: "This Atlas-generated table contains email addresses for each JII in the IDOC system"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 126
    col: 0
    width: 24
    height: 6

  - name: ind_Offender_Phone
    title: ind_Offender_Phone
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_ind_Offender_Phone.primary_key,
      us_ix_ind_Offender_Phone.Offender_PhoneId,
      us_ix_ind_Offender_Phone.PhoneId,
      us_ix_ind_Offender_Phone.OffenderId,
      us_ix_ind_Offender_Phone.PrimaryPhone,
      us_ix_ind_Offender_Phone.Locking,
      us_ix_ind_Offender_Phone.InsertUserId,
      us_ix_ind_Offender_Phone.InsertDate__raw,
      us_ix_ind_Offender_Phone.UpdateUserId,
      us_ix_ind_Offender_Phone.UpdateDate__raw,
      us_ix_ind_Offender_Phone.file_id,
      us_ix_ind_Offender_Phone.is_deleted]
    sorts: [us_ix_ind_Offender_Phone.InsertDate__raw]
    note_display: hover
    note_text: "This table contains records of IDOC clients' phone numbers. These records must be joined to the table `ref_Phone` to get the acutal phone number."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 132
    col: 0
    width: 24
    height: 6

  - name: ind_Offender_QuestionnaireTemplate
    title: ind_Offender_QuestionnaireTemplate
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_ind_Offender_QuestionnaireTemplate.primary_key,
      us_ix_ind_Offender_QuestionnaireTemplate.OffenderQuestionnaireTemplateId,
      us_ix_ind_Offender_QuestionnaireTemplate.QuestionnaireTemplateId,
      us_ix_ind_Offender_QuestionnaireTemplate.OffenderId,
      us_ix_ind_Offender_QuestionnaireTemplate.Response,
      us_ix_ind_Offender_QuestionnaireTemplate.Locking,
      us_ix_ind_Offender_QuestionnaireTemplate.InsertUserId,
      us_ix_ind_Offender_QuestionnaireTemplate.InsertDate__raw,
      us_ix_ind_Offender_QuestionnaireTemplate.UpdateUserId,
      us_ix_ind_Offender_QuestionnaireTemplate.UpdateDate__raw,
      us_ix_ind_Offender_QuestionnaireTemplate.AssignedDate__raw,
      us_ix_ind_Offender_QuestionnaireTemplate.AssignedByEmployeeId,
      us_ix_ind_Offender_QuestionnaireTemplate.CompletedDate__raw,
      us_ix_ind_Offender_QuestionnaireTemplate.CompletedByEmployeeId,
      us_ix_ind_Offender_QuestionnaireTemplate.file_id,
      us_ix_ind_Offender_QuestionnaireTemplate.is_deleted]
    sorts: [us_ix_ind_Offender_QuestionnaireTemplate.InsertDate__raw]
    note_display: hover
    note_text: "This table contains information about recorded questionnaires/surveys that were completed."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 138
    col: 0
    width: 24
    height: 6

  - name: movement
    title: movement
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_movement.primary_key,
      us_ix_movement.move_srl,
      us_ix_movement.docno,
      us_ix_movement.incrno,
      us_ix_movement.move_dtd__raw,
      us_ix_movement.move_typ,
      us_ix_movement.fac_cd,
      us_ix_movement.lu_cd,
      us_ix_movement.loc_cd,
      us_ix_movement.move_pod,
      us_ix_movement.move_tier,
      us_ix_movement.move_cell,
      us_ix_movement.move_bunk,
      us_ix_movement.cnty_cd,
      us_ix_movement.perm_fac_cd,
      us_ix_movement.file_id,
      us_ix_movement.is_deleted]
    sorts: [us_ix_movement.move_dtd__raw]
    note_display: hover
    note_text: "Contains a new row for every movement that happens for a person under IDOC authority (either on supervision or incarcerated)."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 144
    col: 0
    width: 24
    height: 6

  - name: prb_PBCase
    title: prb_PBCase
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_prb_PBCase.primary_key,
      us_ix_prb_PBCase.PBCaseId,
      us_ix_prb_PBCase.OffenderId,
      us_ix_prb_PBCase.TermId,
      us_ix_prb_PBCase.PBDocketId,
      us_ix_prb_PBCase.PBCaseStageId,
      us_ix_prb_PBCase.PBExceptionId,
      us_ix_prb_PBCase.VotingMemberId,
      us_ix_prb_PBCase.ForwardDate__raw,
      us_ix_prb_PBCase.InProgress,
      us_ix_prb_PBCase.PriorityCase,
      us_ix_prb_PBCase.ViolationId,
      us_ix_prb_PBCase.RescissionReconsiderationDate__raw,
      us_ix_prb_PBCase.Notes,
      us_ix_prb_PBCase.Locking,
      us_ix_prb_PBCase.InsertUserId,
      us_ix_prb_PBCase.InsertDate__raw,
      us_ix_prb_PBCase.UpdateUserId,
      us_ix_prb_PBCase.UpdateDate__raw,
      us_ix_prb_PBCase.CurrentVotingRound,
      us_ix_prb_PBCase.PBCaseTypeId,
      us_ix_prb_PBCase.PBCaseSubTypeId,
      us_ix_prb_PBCase.PBCasePeriodId,
      us_ix_prb_PBCase.file_id,
      us_ix_prb_PBCase.is_deleted]
    sorts: [us_ix_prb_PBCase.ForwardDate__raw]
    note_display: hover
    note_text: "Atlas table for parole board cases (or probation cases according to ID research?"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 150
    col: 0
    width: 24
    height: 6

  - name: scb_OffenderJcbInternalStatus
    title: scb_OffenderJcbInternalStatus
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_scb_OffenderJcbInternalStatus.primary_key,
      us_ix_scb_OffenderJcbInternalStatus.OffenderJcbInternalStatusId,
      us_ix_scb_OffenderJcbInternalStatus.OffenderId,
      us_ix_scb_OffenderJcbInternalStatus.OffenderStartInternalStatusId,
      us_ix_scb_OffenderJcbInternalStatus.OffenderEndInternalStatusId,
      us_ix_scb_OffenderJcbInternalStatus.Inactive,
      us_ix_scb_OffenderJcbInternalStatus.StartDate__raw,
      us_ix_scb_OffenderJcbInternalStatus.EndDate__raw,
      us_ix_scb_OffenderJcbInternalStatus.SubmittedDate__raw,
      us_ix_scb_OffenderJcbInternalStatus.Locking,
      us_ix_scb_OffenderJcbInternalStatus.InsertUserId,
      us_ix_scb_OffenderJcbInternalStatus.InsertDate__raw,
      us_ix_scb_OffenderJcbInternalStatus.UpdateUserId,
      us_ix_scb_OffenderJcbInternalStatus.UpdateDate__raw,
      us_ix_scb_OffenderJcbInternalStatus.file_id,
      us_ix_scb_OffenderJcbInternalStatus.is_deleted]
    sorts: [us_ix_scb_OffenderJcbInternalStatus.StartDate__raw]
    note_display: hover
    note_text: "Contains scoreboard records tracking offender internal status spans. Each row represents a period during which an offender held a particular internal status, with start and end dates and references to the corresponding status assignment records."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 156
    col: 0
    width: 24
    height: 6

  - name: scl_Charge
    title: scl_Charge
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_scl_Charge.primary_key,
      us_ix_scl_Charge.ChargeId,
      us_ix_scl_Charge.OffenderId,
      us_ix_scl_Charge.Docket,
      us_ix_scl_Charge.InformationSource,
      us_ix_scl_Charge.WarrantDate__raw,
      us_ix_scl_Charge.ArrestDate__raw,
      us_ix_scl_Charge.CourtId,
      us_ix_scl_Charge.StateId,
      us_ix_scl_Charge.OtherCourt,
      us_ix_scl_Charge.ChargeStatusId,
      us_ix_scl_Charge.ChargePleaId,
      us_ix_scl_Charge.Comments,
      us_ix_scl_Charge.ChargedOffenseId,
      us_ix_scl_Charge.VictimId,
      us_ix_scl_Charge.NextCourtDateTime__raw,
      us_ix_scl_Charge.NextCourtComments,
      us_ix_scl_Charge.OutcomeDate__raw,
      us_ix_scl_Charge.HasPleaAgreement,
      us_ix_scl_Charge.ChargeOutcomeTypeId,
      us_ix_scl_Charge.OtherOutcomeType,
      us_ix_scl_Charge.AmendedOffenseTypeId,
      us_ix_scl_Charge.AmendedOffenseComments,
      us_ix_scl_Charge.Inactive,
      us_ix_scl_Charge.Locking,
      us_ix_scl_Charge.InsertUserId,
      us_ix_scl_Charge.InsertDate__raw,
      us_ix_scl_Charge.UpdateUserId,
      us_ix_scl_Charge.UpdateDate__raw,
      us_ix_scl_Charge.file_id,
      us_ix_scl_Charge.is_deleted]
    sorts: [us_ix_scl_Charge.WarrantDate__raw]
    note_display: hover
    note_text: "Contains details about each charge, identifying the associated offender and docket."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 162
    col: 0
    width: 24
    height: 6

  - name: scl_DiscOffenseRpt
    title: scl_DiscOffenseRpt
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_scl_DiscOffenseRpt.primary_key,
      us_ix_scl_DiscOffenseRpt.DiscOffenseRptId,
      us_ix_scl_DiscOffenseRpt.InfractionDate__raw,
      us_ix_scl_DiscOffenseRpt.PreDetentionInDate__raw,
      us_ix_scl_DiscOffenseRpt.PreDetentionOutDate__raw,
      us_ix_scl_DiscOffenseRpt.PreDetentionServedDays,
      us_ix_scl_DiscOffenseRpt.IsolationInDate__raw,
      us_ix_scl_DiscOffenseRpt.IsolationOutDate__raw,
      us_ix_scl_DiscOffenseRpt.IsolationServedDays,
      us_ix_scl_DiscOffenseRpt.PenaltyLostSgtDays,
      us_ix_scl_DiscOffenseRpt.PenaltyOtherDesc,
      us_ix_scl_DiscOffenseRpt.DeputyDirApproved,
      us_ix_scl_DiscOffenseRpt.LostAllSgt,
      us_ix_scl_DiscOffenseRpt.ApprovalDate,
      us_ix_scl_DiscOffenseRpt.Inactive,
      us_ix_scl_DiscOffenseRpt.DorOffenseTypeId,
      us_ix_scl_DiscOffenseRpt.LocationId,
      us_ix_scl_DiscOffenseRpt.TermId,
      us_ix_scl_DiscOffenseRpt.OffenderId,
      us_ix_scl_DiscOffenseRpt.ApprovalStateId,
      us_ix_scl_DiscOffenseRpt.ApprovalEmployeeId,
      us_ix_scl_DiscOffenseRpt.Locking,
      us_ix_scl_DiscOffenseRpt.InsertUserId,
      us_ix_scl_DiscOffenseRpt.InsertDate__raw,
      us_ix_scl_DiscOffenseRpt.UpdateUserId,
      us_ix_scl_DiscOffenseRpt.UpdateDate__raw,
      us_ix_scl_DiscOffenseRpt.Comment,
      us_ix_scl_DiscOffenseRpt.Overturned,
      us_ix_scl_DiscOffenseRpt.file_id,
      us_ix_scl_DiscOffenseRpt.is_deleted]
    sorts: [us_ix_scl_DiscOffenseRpt.InfractionDate__raw]
    note_display: hover
    note_text: "Atlas table holding disciplinary offense report information"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 168
    col: 0
    width: 24
    height: 6

  - name: scl_Escape
    title: scl_Escape
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_scl_Escape.primary_key,
      us_ix_scl_Escape.EscapeId,
      us_ix_scl_Escape.EscapeDate__raw,
      us_ix_scl_Escape.ReturnDocDate__raw,
      us_ix_scl_Escape.ArrestDate__raw,
      us_ix_scl_Escape.EscapeDays,
      us_ix_scl_Escape.SentDaysLeft,
      us_ix_scl_Escape.SentMonthsLeft,
      us_ix_scl_Escape.SentYearsLeft,
      us_ix_scl_Escape.Notes,
      us_ix_scl_Escape.Clothing,
      us_ix_scl_Escape.Contacts,
      us_ix_scl_Escape.OtherReason,
      us_ix_scl_Escape.ApprovedDate,
      us_ix_scl_Escape.Inactive,
      us_ix_scl_Escape.OffenderId,
      us_ix_scl_Escape.TermId,
      us_ix_scl_Escape.EscapeTypeId,
      us_ix_scl_Escape.TobeTriedId,
      us_ix_scl_Escape.NewCrimeId,
      us_ix_scl_Escape.ArrestedLocationId,
      us_ix_scl_Escape.EscapeLocationId,
      us_ix_scl_Escape.ReturnLocationId,
      us_ix_scl_Escape.ApprovalStateId,
      us_ix_scl_Escape.ApprovedById,
      us_ix_scl_Escape.Locking,
      us_ix_scl_Escape.InsertUserId,
      us_ix_scl_Escape.InsertDate,
      us_ix_scl_Escape.UpdateUserId,
      us_ix_scl_Escape.UpdateDate,
      us_ix_scl_Escape.file_id,
      us_ix_scl_Escape.is_deleted]
    sorts: [us_ix_scl_Escape.EscapeDate__raw]
    note_display: hover
    note_text: "Contains information about each escape from custody."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 174
    col: 0
    width: 24
    height: 6

  - name: scl_MasterTerm
    title: scl_MasterTerm
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_scl_MasterTerm.primary_key,
      us_ix_scl_MasterTerm.MasterTermId,
      us_ix_scl_MasterTerm.ParoleMED,
      us_ix_scl_MasterTerm.MaxMED,
      us_ix_scl_MasterTerm.MasterTermStatusId,
      us_ix_scl_MasterTerm.OffenderId,
      us_ix_scl_MasterTerm.Locking,
      us_ix_scl_MasterTerm.InsertUserId,
      us_ix_scl_MasterTerm.InsertDate,
      us_ix_scl_MasterTerm.UpdateUserId,
      us_ix_scl_MasterTerm.UpdateDate,
      us_ix_scl_MasterTerm.SupervisionStartDate,
      us_ix_scl_MasterTerm.SupervisionStartTermId,
      us_ix_scl_MasterTerm.SupervisionStartTransferId,
      us_ix_scl_MasterTerm.file_id,
      us_ix_scl_MasterTerm.is_deleted]
    sorts: [us_ix_scl_MasterTerm.MasterTermId]
    note_display: hover
    note_text: "TODO(#15329): Fill in the file description"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 180
    col: 0
    width: 24
    height: 6

  - name: scl_Parole
    title: scl_Parole
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_scl_Parole.primary_key,
      us_ix_scl_Parole.ParoleId,
      us_ix_scl_Parole.ReleaseDate__raw,
      us_ix_scl_Parole.AdminDischargeDate,
      us_ix_scl_Parole.ReturnToDocDate,
      us_ix_scl_Parole.ReturnStartDate,
      us_ix_scl_Parole.RevocationDate,
      us_ix_scl_Parole.ParoleContinuedDate,
      us_ix_scl_Parole.TlsYear,
      us_ix_scl_Parole.TlsMonth,
      us_ix_scl_Parole.TlsDays,
      us_ix_scl_Parole.RgtAfterYears,
      us_ix_scl_Parole.RgtAfterMonths,
      us_ix_scl_Parole.RgtAfterDays,
      us_ix_scl_Parole.RgtBeforeYears,
      us_ix_scl_Parole.RgtBeforeMonths,
      us_ix_scl_Parole.RgtBeforeDays,
      us_ix_scl_Parole.ApprovalDate,
      us_ix_scl_Parole.Inactive,
      us_ix_scl_Parole.ParoleTypeId,
      us_ix_scl_Parole.ViolationTypeId,
      us_ix_scl_Parole.TermId,
      us_ix_scl_Parole.OffenderId,
      us_ix_scl_Parole.ApprovalStateId,
      us_ix_scl_Parole.ApprovalEmployeeId,
      us_ix_scl_Parole.Locking,
      us_ix_scl_Parole.InsertUserId,
      us_ix_scl_Parole.InsertDate,
      us_ix_scl_Parole.UpdateUserId,
      us_ix_scl_Parole.UpdateDate,
      us_ix_scl_Parole.file_id,
      us_ix_scl_Parole.is_deleted]
    sorts: [us_ix_scl_Parole.ReleaseDate__raw]
    note_display: hover
    note_text: "Provides details for each parole term, indicated by ParoleId."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 186
    col: 0
    width: 24
    height: 6

  - name: scl_Sentence
    title: scl_Sentence
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_scl_Sentence.primary_key,
      us_ix_scl_Sentence.SentenceId,
      us_ix_scl_Sentence.CourtOrderSignedDate,
      us_ix_scl_Sentence.CourtOrderReceivedDate,
      us_ix_scl_Sentence.OrigCourtFindingDate,
      us_ix_scl_Sentence.CourtFindingDate,
      us_ix_scl_Sentence.EstSentStartDate,
      us_ix_scl_Sentence.TerminateOverrideDate,
      us_ix_scl_Sentence.OffenseDate,
      us_ix_scl_Sentence.AmendedDate,
      us_ix_scl_Sentence.AmendedComments,
      us_ix_scl_Sentence.SentenceSequence,
      us_ix_scl_Sentence.Docket,
      us_ix_scl_Sentence.Count,
      us_ix_scl_Sentence.TerminateOverrideNotes,
      us_ix_scl_Sentence.OtherCourt,
      us_ix_scl_Sentence.CostAmount,
      us_ix_scl_Sentence.CostComments,
      us_ix_scl_Sentence.FineAmount,
      us_ix_scl_Sentence.FineDueDate,
      us_ix_scl_Sentence.FineSuspAmount,
      us_ix_scl_Sentence.FinePmtSchd,
      us_ix_scl_Sentence.FineComments,
      us_ix_scl_Sentence.FinePaid,
      us_ix_scl_Sentence.FinePaidDate,
      us_ix_scl_Sentence.FinePaidNotBy,
      us_ix_scl_Sentence.VictimCompFee,
      us_ix_scl_Sentence.SentDetnDays,
      us_ix_scl_Sentence.SentDetnGoodTimeDays,
      us_ix_scl_Sentence.CswHours,
      us_ix_scl_Sentence.CswDueDate,
      us_ix_scl_Sentence.CswSuspHours,
      us_ix_scl_Sentence.CswTotalHours,
      us_ix_scl_Sentence.CswComments,
      us_ix_scl_Sentence.Comments,
      us_ix_scl_Sentence.Ttl,
      us_ix_scl_Sentence.EnableConsecTo,
      us_ix_scl_Sentence.TerminateOverride,
      us_ix_scl_Sentence.ApprovalDate,
      us_ix_scl_Sentence.Inactive,
      us_ix_scl_Sentence.Stayed,
      us_ix_scl_Sentence.TermId,
      us_ix_scl_Sentence.MasterTermId,
      us_ix_scl_Sentence.OffenderId,
      us_ix_scl_Sentence.ConsecSentenceId,
      us_ix_scl_Sentence.OffenseTypeId,
      us_ix_scl_Sentence.FtiLevelTypeId,
      us_ix_scl_Sentence.SentenceTypeId,
      us_ix_scl_Sentence.SentenceStatusId,
      us_ix_scl_Sentence.SentenceRelationshipId,
      us_ix_scl_Sentence.CourtOrderTypeId,
      us_ix_scl_Sentence.CourtId,
      us_ix_scl_Sentence.ApprovalStateId,
      us_ix_scl_Sentence.ApprovalEmployeeId,
      us_ix_scl_Sentence.SupervisionId,
      us_ix_scl_Sentence.Locking,
      us_ix_scl_Sentence.InsertUserId,
      us_ix_scl_Sentence.InsertDate,
      us_ix_scl_Sentence.UpdateUserId,
      us_ix_scl_Sentence.UpdateDate,
      us_ix_scl_Sentence.GoodTimeTypeId,
      us_ix_scl_Sentence.CostsUnknown,
      us_ix_scl_Sentence.JudgeId,
      us_ix_scl_Sentence.Appealed,
      us_ix_scl_Sentence.AliasNameId,
      us_ix_scl_Sentence.RevocationOffenseTypeId,
      us_ix_scl_Sentence.DocumentControlNumber,
      us_ix_scl_Sentence.OffenseTrackingNumber,
      us_ix_scl_Sentence.ExtendedOffenseDesc,
      us_ix_scl_Sentence.EarlyDischargeLetter,
      us_ix_scl_Sentence.EarlyDischargeDate,
      us_ix_scl_Sentence.DischargeDate,
      us_ix_scl_Sentence.DischargeUpdateDate,
      us_ix_scl_Sentence.DischargeUpdateByEmployeeId,
      us_ix_scl_Sentence.CustodyComments,
      us_ix_scl_Sentence.file_id,
      us_ix_scl_Sentence.is_deleted]
    sorts: [us_ix_scl_Sentence.SentenceId]
    note_display: hover
    note_text: "Provides details on sentences, although most of the data is missing."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 192
    col: 0
    width: 24
    height: 6

  - name: scl_SentenceOrder
    title: scl_SentenceOrder
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_scl_SentenceOrder.primary_key,
      us_ix_scl_SentenceOrder.SentenceOrderId,
      us_ix_scl_SentenceOrder.ParentSentenceOrderId,
      us_ix_scl_SentenceOrder.StateId,
      us_ix_scl_SentenceOrder.CountyId,
      us_ix_scl_SentenceOrder.Sequence,
      us_ix_scl_SentenceOrder.EffectiveDate,
      us_ix_scl_SentenceOrder.OffenderId,
      us_ix_scl_SentenceOrder.TermId,
      us_ix_scl_SentenceOrder.JudgeLegistId,
      us_ix_scl_SentenceOrder.DefendantAliasNameId,
      us_ix_scl_SentenceOrder.ProsecutingAuthorityLocationId,
      us_ix_scl_SentenceOrder.SentenceDate__raw,
      us_ix_scl_SentenceOrder.ReceiveDate,
      us_ix_scl_SentenceOrder.Comment,
      us_ix_scl_SentenceOrder.Locking,
      us_ix_scl_SentenceOrder.InsertUserId,
      us_ix_scl_SentenceOrder.InsertDate,
      us_ix_scl_SentenceOrder.UpdateUserId,
      us_ix_scl_SentenceOrder.UpdateDate,
      us_ix_scl_SentenceOrder.SentenceOrderTypeId,
      us_ix_scl_SentenceOrder.DefendantAttorney,
      us_ix_scl_SentenceOrder.DistrictAttorney,
      us_ix_scl_SentenceOrder.ChargeId,
      us_ix_scl_SentenceOrder.SentenceOrderStatusId,
      us_ix_scl_SentenceOrder.SentenceOrderEventTypeId,
      us_ix_scl_SentenceOrder.IsApproved,
      us_ix_scl_SentenceOrder.CorrectionsCompactStartDate,
      us_ix_scl_SentenceOrder.CorrectionsCompactEndDate,
      us_ix_scl_SentenceOrder.SentenceOrderReasonId,
      us_ix_scl_SentenceOrder.IsCalculated,
      us_ix_scl_SentenceOrder.IsStartUponTransferToDOC,
      us_ix_scl_SentenceOrder.HistoricalLegalStatusId,
      us_ix_scl_SentenceOrder.RJRelinquishedDate,
      us_ix_scl_SentenceOrder.HistoricalDocket,
      us_ix_scl_SentenceOrder.HistoricalChargeOutcomeTypeId,
      us_ix_scl_SentenceOrder.file_id,
      us_ix_scl_SentenceOrder.is_deleted]
    sorts: [us_ix_scl_SentenceOrder.SentenceDate__raw]
    note_display: hover
    note_text: "Contains details on each sentence order."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 198
    col: 0
    width: 24
    height: 6

  - name: scl_Supervision
    title: scl_Supervision
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_scl_Supervision.primary_key,
      us_ix_scl_Supervision.SupervisionId,
      us_ix_scl_Supervision.MasterTermId,
      us_ix_scl_Supervision.SupervisionTypeId,
      us_ix_scl_Supervision.ImposedDate__raw,
      us_ix_scl_Supervision.CourtAuthority,
      us_ix_scl_Supervision.JudgeId,
      us_ix_scl_Supervision.StartDate__raw,
      us_ix_scl_Supervision.SupervisionMEDDate,
      us_ix_scl_Supervision.SupervisionStatusId,
      us_ix_scl_Supervision.AliasNameId,
      us_ix_scl_Supervision.InterstateTypeParole,
      us_ix_scl_Supervision.InterstateTypeProbation,
      us_ix_scl_Supervision.InterstateTypeOther,
      us_ix_scl_Supervision.ReportingInstruction,
      us_ix_scl_Supervision.ReleaseNotes,
      us_ix_scl_Supervision.ParoleId,
      us_ix_scl_Supervision.Locking,
      us_ix_scl_Supervision.InsertUserId,
      us_ix_scl_Supervision.InsertDate,
      us_ix_scl_Supervision.UpdateUserId,
      us_ix_scl_Supervision.UpdateDate,
      us_ix_scl_Supervision.OriginalOffense,
      us_ix_scl_Supervision.OffenderId,
      us_ix_scl_Supervision.file_id,
      us_ix_scl_Supervision.is_deleted]
    sorts: [us_ix_scl_Supervision.ImposedDate__raw]
    note_display: hover
    note_text: "Contains details about each supervision assignment."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 204
    col: 0
    width: 24
    height: 6

  - name: scl_Term
    title: scl_Term
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_scl_Term.primary_key,
      us_ix_scl_Term.TermId,
      us_ix_scl_Term.MasterTermId,
      us_ix_scl_Term.AliasNameId,
      us_ix_scl_Term.InmateNumber,
      us_ix_scl_Term.SpnrNum,
      us_ix_scl_Term.DrcDate__raw,
      us_ix_scl_Term.DrcOverrideDate__raw,
      us_ix_scl_Term.DrcOverrideApprovedDate__raw,
      us_ix_scl_Term.DpedApprovedDate__raw,
      us_ix_scl_Term.DpedPendingDate__raw,
      us_ix_scl_Term.MprdApprovedDate__raw,
      us_ix_scl_Term.MprdPendingDate__raw,
      us_ix_scl_Term.MaxprdApprovedDate__raw,
      us_ix_scl_Term.MaxprdPendingDate__raw,
      us_ix_scl_Term.GtrdApprovedDate__raw,
      us_ix_scl_Term.GtrdPendingDate__raw,
      us_ix_scl_Term.FtrdApprovedDate__raw,
      us_ix_scl_Term.FtrdPendingDate__raw,
      us_ix_scl_Term.AdoOverride,
      us_ix_scl_Term.AdoOverrideDays,
      us_ix_scl_Term.AdjustedDaysApplied,
      us_ix_scl_Term.AdjustedDaysAppliedTo,
      us_ix_scl_Term.DpedInelgible,
      us_ix_scl_Term.FtoOverrideLevel,
      us_ix_scl_Term.FtoOverrideEffectDate__raw,
      us_ix_scl_Term.LastCalculateDate__raw,
      us_ix_scl_Term.ApprovalDate__raw,
      us_ix_scl_Term.Inactive,
      us_ix_scl_Term.ActiveDetainer,
      us_ix_scl_Term.CsoOverrideTypeId,
      us_ix_scl_Term.TermStatusId,
      us_ix_scl_Term.LastCalculateById,
      us_ix_scl_Term.ApprovalStateId,
      us_ix_scl_Term.ApprovalEmployeeId,
      us_ix_scl_Term.Locking,
      us_ix_scl_Term.InsertUserId,
      us_ix_scl_Term.InsertDate__raw,
      us_ix_scl_Term.UpdateUserId,
      us_ix_scl_Term.UpdateDate__raw,
      us_ix_scl_Term.LocStatusEffDate__raw,
      us_ix_scl_Term.LocationId,
      us_ix_scl_Term.OffenderStatusId,
      us_ix_scl_Term.LastPendingDate__raw,
      us_ix_scl_Term.Pending,
      us_ix_scl_Term.PendingTotalSentenceYears,
      us_ix_scl_Term.PendingTotalSentenceMonths,
      us_ix_scl_Term.PendingTotalSentenceDays,
      us_ix_scl_Term.ApprovedTotalSentenceYears,
      us_ix_scl_Term.ApprovedTotalSentenceMonths,
      us_ix_scl_Term.ApprovedTotalSentenceDays,
      us_ix_scl_Term.PendingTotalSentenceLifeDeathTx,
      us_ix_scl_Term.PendingStateResponsible,
      us_ix_scl_Term.PendingLocalResponsible,
      us_ix_scl_Term.PendingMostSeriousOffenseId,
      us_ix_scl_Term.ApprovedTotalSentenceLifeDeathTx,
      us_ix_scl_Term.ApprovedStateResponsible,
      us_ix_scl_Term.ApprovedLocalResponsible,
      us_ix_scl_Term.ApprovedMostSeriousOffenseId,
      us_ix_scl_Term.ReleaseDate__raw,
      us_ix_scl_Term.OffenderId,
      us_ix_scl_Term.TermStartDate__raw,
      us_ix_scl_Term.TentativeParoleDate__raw,
      us_ix_scl_Term.NextHearingDate__raw,
      us_ix_scl_Term.TermSentenceDate__raw,
      us_ix_scl_Term.SegmentStartDate__raw,
      us_ix_scl_Term.IndeterminateStartDate__raw,
      us_ix_scl_Term.IndeterminateEndDate__raw,
      us_ix_scl_Term.ProbationStartDate__raw,
      us_ix_scl_Term.ProbationExpirationDate__raw,
      us_ix_scl_Term.ProbationCalculationStatusId,
      us_ix_scl_Term.ProbationCalculationDate__raw,
      us_ix_scl_Term.ProbationCalculationEmployeeId,
      us_ix_scl_Term.RiderCalculationStatusId,
      us_ix_scl_Term.RiderStartDate__raw,
      us_ix_scl_Term.RiderExpiryDate__raw,
      us_ix_scl_Term.InitialParoleHearingDate__raw,
      us_ix_scl_Term.file_id,
      us_ix_scl_Term.is_deleted]
    sorts: [us_ix_scl_Term.DrcDate__raw]
    note_display: hover
    note_text: "Contains data about each term of incarceration/supervision."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 210
    col: 0
    width: 24
    height: 6

  - name: scl_Violation
    title: scl_Violation
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_scl_Violation.primary_key,
      us_ix_scl_Violation.ViolationId,
      us_ix_scl_Violation.ReportStatusId,
      us_ix_scl_Violation.ReportSubmissionDate,
      us_ix_scl_Violation.SupervisionPlan,
      us_ix_scl_Violation.ArrestDate,
      us_ix_scl_Violation.SupervisionAdjustment,
      us_ix_scl_Violation.OffenderLocationId,
      us_ix_scl_Violation.Recommendation,
      us_ix_scl_Violation.CreatingReportOfficerId,
      us_ix_scl_Violation.DOCLocationId,
      us_ix_scl_Violation.PreviousSanctions,
      us_ix_scl_Violation.SubmittingReportOfficerId,
      us_ix_scl_Violation.ApprovingSupervisorId,
      us_ix_scl_Violation.OutcomeDate,
      us_ix_scl_Violation.ViolationTypeId,
      us_ix_scl_Violation.ViolationOutcomeId,
      us_ix_scl_Violation.OtherOutcome,
      us_ix_scl_Violation.AdditionalComments,
      us_ix_scl_Violation.SupervisionId,
      us_ix_scl_Violation.OffenderId,
      us_ix_scl_Violation.Locking,
      us_ix_scl_Violation.InsertUserId,
      us_ix_scl_Violation.InsertDate,
      us_ix_scl_Violation.UpdateUserId,
      us_ix_scl_Violation.UpdateDate,
      us_ix_scl_Violation.PreliminaryHearingId,
      us_ix_scl_Violation.ReportCreationDate,
      us_ix_scl_Violation.OtherOffenderLocation,
      us_ix_scl_Violation.ViolationDispositionTypeId,
      us_ix_scl_Violation.file_id,
      us_ix_scl_Violation.is_deleted]
    sorts: [us_ix_scl_Violation.ViolationId]
    note_display: hover
    note_text: "Documents supervision violations."
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 216
    col: 0
    width: 24
    height: 6

  - name: scl_Warrant
    title: scl_Warrant
    explore: us_ix_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_ix_scl_Warrant.primary_key,
      us_ix_scl_Warrant.WarrantId,
      us_ix_scl_Warrant.WarrantTypeId,
      us_ix_scl_Warrant.LocatedAt,
      us_ix_scl_Warrant.WarrantIssuedDate__raw,
      us_ix_scl_Warrant.WarrantExecutedDate__raw,
      us_ix_scl_Warrant.POExecutionAwareDate__raw,
      us_ix_scl_Warrant.WarrantInactiveDate__raw,
      us_ix_scl_Warrant.InactiveReasonId,
      us_ix_scl_Warrant.WarrantIssuedById,
      us_ix_scl_Warrant.IssuingDOCLocationId,
      us_ix_scl_Warrant.WarrantApprovedById,
      us_ix_scl_Warrant.DocumentRetrievedDate__raw,
      us_ix_scl_Warrant.Notes,
      us_ix_scl_Warrant.OffenderId,
      us_ix_scl_Warrant.Locking,
      us_ix_scl_Warrant.InsertUserId,
      us_ix_scl_Warrant.InsertDate__raw,
      us_ix_scl_Warrant.UpdateUserId,
      us_ix_scl_Warrant.UpdateDate__raw,
      us_ix_scl_Warrant.WarrantApprovedDate__raw,
      us_ix_scl_Warrant.LastArchivedDate__raw,
      us_ix_scl_Warrant.ViolationId,
      us_ix_scl_Warrant.WarrantReasonId,
      us_ix_scl_Warrant.CommissionStatusId,
      us_ix_scl_Warrant.ParoleeStatusId,
      us_ix_scl_Warrant.NCIC,
      us_ix_scl_Warrant.ExtraditionLimitId,
      us_ix_scl_Warrant.OfficerWarrantLocationId,
      us_ix_scl_Warrant.file_id,
      us_ix_scl_Warrant.is_deleted]
    sorts: [us_ix_scl_Warrant.WarrantIssuedDate__raw]
    note_display: hover
    note_text: "This table contains information about warrants"
    listen: 
      View Type: us_ix_ind_Offender.view_type
      US_IX_DOC: us_ix_ind_Offender.OffenderId
    row: 222
    col: 0
    width: 24
    height: 6

