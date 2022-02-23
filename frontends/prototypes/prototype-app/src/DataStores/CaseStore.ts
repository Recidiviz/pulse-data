// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
// =============================================================================
import {
  DocumentData,
  onSnapshot,
  QuerySnapshot,
  Timestamp,
} from "firebase/firestore";
import { assign, xor } from "lodash";
import { makeAutoObservable, when } from "mobx";

import {
  compliantReportingCasesQuery,
  compliantReportingReferralsQuery,
  compliantReportingStatusQuery,
  compliantReportingUpdatesQuery,
  officersQuery,
  saveCompliantReportingNote,
  saveCompliantReportingStatus,
  upcomingDischargeCasesQuery,
} from "../firebase";
import type { RootStore } from "./RootStore";

type ConstructorProps = {
  rootStore: RootStore;
};

export type CompliantReportingExportedReferral = {
  poFirstName: string;
  poLastName: string;
  clientFirstName: string;
  clientLastName: string;
  dateToday: string;
  tdocId: string;
  physicalAddress: string;
  currentEmployer: string;
  driversLicense: string;
  driversLicenseSuspended: string;
  driversLicenseRevoked: string;
  convictionCounty: string;
  courtName: string;
  allDockets: string;
  offenseType: string[];
  supervisionType: string;
  sentenceStartDate: string;
  sentenceLengthDays: string;
  expirationDate: string;
  supervisionFeeAssessed: string;
  supervisionFeeArrearaged: "1" | "0";
  supervisionFeeExemption: string;
  supervisionFeeExemptionType: string;
  supervisionFeeExemptionExpirDate: string;
  supervisionFeeWaived: string;
  courtCostsPaid: "1" | "0";
  courtCostsBalance: string;
  courtCostsMonthlyAmt1: string;
  courtCostsMonthlyAmt2: string;
  restitutionAmt: string;
  restitutionMonthlyPayment: string;
  restitutionMonthlyPaymentTo: string;
  specialConditionsAlcDrugScreen: "1" | "0";
  specialConditionsAlcDrugScreenDate: string;
  specialConditionsAlcDrugAssessment: string;
  specialConditionsAlcDrugAssessmentComplete: "1" | "0";
  specialConditionsAlcDrugAssessmentCompleteDate: string;
  specialConditionsAlcDrugTreatment: "1" | "0";
  specialConditionsAlcDrugTreatmentInOut: "INPATIENT" | "OUTPATIENT";
  specialConditionsAlcDrugTreatmentCurrent: "1" | "0";
  specialConditionsAlcDrugTreatmentCompleteDate: string;
  specialConditionsCounseling: "1" | "0";
  specialConditionsCounselingType: ("ANGER_MANAGEMENT" | "MENTAL_HEALTH")[];
  specialConditionsCounselingCurrent: "1" | "0";
  specialConditionsCounselingCompleteDate: string;
  specialConditionsCommunityService: "1" | "0";
  specialConditionsCommunityServiceHours: string;
  specialConditionsCommunityServiceCurrent: "1" | "0";
  specialConditionsCommunityServiceCompletionDate: string;
  specialConditionsProgramming: "1" | "0";
  specialConditionsProgrammingCognitiveBehavior: "1" | "0";
  specialConditionsProgrammingCognitiveBehaviorCurrent: "1" | "0";
  specialConditionsProgrammingCognitiveBehaviorCompletionDate: string;
  specialConditionsProgrammingSafe: "1" | "0";
  specialConditionsProgrammingSafeCurrent: "1" | "0";
  specialConditionsProgrammingSafeCompletionDate: string;
  specialConditionsProgrammingVictimImpact: "1" | "0";
  specialConditionsProgrammingVictimImpactCurrent: "1" | "0";
  specialConditionsProgrammingVictimImpactCompletionDate: string;
  specialConditionsProgrammingFsw: "1" | "0";
  specialConditionsProgrammingFswCurrent: "1" | "0";
  specialConditionsProgrammingFswCompletionDate: string;
};

type CompliantReportingExportedCase = {
  personExternalId: string;
  personName: string;
  officerId: string;
  supervisionType: string;
  judicialDistrict: string;
  supervisionLevel: string;
  supervisionLevelStart: Timestamp;
  offenseType: string[];
  lastDrun: Timestamp[];
  lastSanction: string | null;
};

type CompliantReportingStatusCode = "ELIGIBLE" | "DENIED";

export type CompliantReportingStatusRecord = {
  personExternalId: string;
  status: CompliantReportingStatusCode;
  deniedReasons: string[];
  statusUpdated: {
    date: Timestamp;
    by: string;
  };
};

export type CompliantReportingCase = CompliantReportingExportedCase &
  Partial<CompliantReportingStatusRecord> & {
    officerName: string;
    updateCount: number;
  };

export type CompliantReportingUpdateContents = {
  creator: string;
  text: string;
  personExternalId: string;
};

export type CompliantReportingUpdate = CompliantReportingUpdateContents & {
  createdAt: Date;
};

export type UpcomingDischargeExportedCase = {
  personExternalId: string;
  personName: string;
  officerId: string;
  supervisionType: string;
  judicialDistrict: string;
  expectedDischargeDate: Timestamp | null;
};

export type UpcomingDischargeCase = UpcomingDischargeExportedCase & {
  officerName: string;
  updateCount: number;
};

type OfficerInfo = { name: string };

type OfficerMapping = Record<string, OfficerInfo>;

export type Client = {
  personExternalId: string;
  personName: string;
  officerId: string;
  officerName: string;
  supervisionType: string;
  compliantReportingCase: CompliantReportingCase | undefined;
  upcomingDischargeCase: UpcomingDischargeCase | undefined;
};

export default class CaseStore {
  readonly rootStore: RootStore;

  compliantReportingExportedCases: CompliantReportingExportedCase[] = [];

  compliantReportingStatuses: CompliantReportingStatusRecord[] = [];

  compliantReportingUpdates: CompliantReportingUpdate[] = [];

  compliantReportingReferrals: CompliantReportingExportedReferral[] = [];

  upcomingDischargeExportedCases: UpcomingDischargeExportedCase[] = [];

  officers: OfficerMapping = {};

  activeClientId?: string;

  constructor({ rootStore }: ConstructorProps) {
    makeAutoObservable(this, { rootStore: false });

    this.rootStore = rootStore;

    this.subscribe();
  }

  subscribe(): void {
    when(
      // our firestore client will not be ready until we are authorized
      () => this.rootStore.userStore.isAuthorized,
      () => {
        // subscribe to Firestore data sources
        onSnapshot(compliantReportingStatusQuery, (snapshot) =>
          this.updateCompliantReportingStatuses(snapshot)
        );

        onSnapshot(compliantReportingUpdatesQuery, (snapshot) =>
          this.updateCompliantReportingUpdates(snapshot)
        );

        onSnapshot(compliantReportingCasesQuery, (snapshot) =>
          this.updateCompliantReportingExportedCases(snapshot)
        );

        onSnapshot(compliantReportingReferralsQuery, (snapshot) =>
          this.updateCompliantReportingReferrals(snapshot)
        );

        onSnapshot(upcomingDischargeCasesQuery, (snapshot) =>
          this.updateUpcomingDischargeExportedCases(snapshot)
        );

        onSnapshot(officersQuery, (snapshot) => this.updateOfficers(snapshot));
      }
    );
  }

  setActiveClient(clientId?: string): void {
    this.activeClientId = clientId;
  }

  get activeClient(): Client | undefined {
    if (!this.activeClientId) {
      return undefined;
    }
    const compliantReportingCase = this.compliantReportingCases.find(
      (record) => record.personExternalId === this.activeClientId
    );
    const upcomingDischargeCase = this.upcomingDischargeCases.find(
      (record) => record.personExternalId === this.activeClientId
    );
    if (compliantReportingCase || upcomingDischargeCase) {
      return {
        personExternalId: this.activeClientId,
        personName:
          compliantReportingCase?.personName ??
          upcomingDischargeCase?.personName ??
          "Unknown",
        officerId:
          compliantReportingCase?.officerId ??
          upcomingDischargeCase?.officerId ??
          "Unknown",
        officerName:
          compliantReportingCase?.officerName ??
          upcomingDischargeCase?.officerName ??
          "Unknown",
        supervisionType:
          compliantReportingCase?.supervisionType ??
          upcomingDischargeCase?.supervisionType ??
          "Unknown",
        compliantReportingCase,
        upcomingDischargeCase,
      };
    }
  }

  get activeClientUpdates(): CompliantReportingUpdate[] {
    return this.compliantReportingUpdates.filter(
      (r) => r.personExternalId === this.activeClientId
    );
  }

  get compliantReportingCases(): CompliantReportingCase[] {
    return this.compliantReportingExportedCases
      .filter((caseData) =>
        this.rootStore.userStore.includedOfficers.includes(caseData.officerId)
      )
      .map((caseData) => {
        const status = this.compliantReportingStatuses.find(
          (s) => s.personExternalId === caseData.personExternalId
        );
        const updateCount = this.compliantReportingUpdates.filter(
          (u) => u.personExternalId === caseData.personExternalId
        ).length;
        const officerName =
          this.officers[caseData.officerId]?.name || caseData.officerId;
        return assign({}, caseData, status, { updateCount, officerName });
      });
  }

  get upcomingDischargeCases(): UpcomingDischargeCase[] {
    return this.upcomingDischargeExportedCases
      .filter((caseData) =>
        this.rootStore.userStore.includedOfficers.includes(caseData.officerId)
      )
      .map((caseData) => {
        const updateCount = this.compliantReportingUpdates.filter(
          (u) => u.personExternalId === caseData.personExternalId
        ).length;
        const officerName =
          this.officers[caseData.officerId]?.name || caseData.officerId;
        return assign({}, caseData, { updateCount, officerName });
      });
  }

  private updateCompliantReportingExportedCases(
    querySnapshot: QuerySnapshot<DocumentData>
  ): void {
    const cases: CompliantReportingExportedCase[] = [];
    querySnapshot.forEach((doc) => {
      cases.push(doc.data() as CompliantReportingExportedCase);
    });
    this.compliantReportingExportedCases = cases;
  }

  private updateCompliantReportingStatuses(
    querySnapshot: QuerySnapshot<DocumentData>
  ): void {
    const statuses: CompliantReportingStatusRecord[] = [];
    querySnapshot.forEach((doc) => {
      const statusData = doc.data() as CompliantReportingStatusRecord;

      // date in record may be null while a server timestamp is pending
      const statusUpdated = {
        date: statusData.statusUpdated.date ?? Timestamp.now(),
        by: statusData.statusUpdated.by,
      };
      statuses.push({ ...statusData, statusUpdated });
    });
    this.compliantReportingStatuses = statuses;
  }

  private updateCompliantReportingUpdates(
    querySnapshot: QuerySnapshot<DocumentData>
  ): void {
    const updateRecords: CompliantReportingUpdate[] = [];
    querySnapshot.forEach((doc) => {
      const updateData = doc.data();

      // date in record may be null while a server timestamp is pending
      const createdAt = updateData.createdAt?.toDate() ?? new Date();

      updateRecords.push({
        ...updateData,
        createdAt,
      } as CompliantReportingUpdate);
    });
    this.compliantReportingUpdates = updateRecords;
  }

  private updateCompliantReportingReferrals(
    querySnapshot: QuerySnapshot<DocumentData>
  ): void {
    const updateRecords: CompliantReportingExportedReferral[] = [];
    querySnapshot.forEach((doc) => {
      const updateData = doc.data();

      updateRecords.push({
        ...updateData,
      } as CompliantReportingExportedReferral);
    });
    this.compliantReportingReferrals = updateRecords;
  }

  private updateUpcomingDischargeExportedCases(
    querySnapshot: QuerySnapshot<DocumentData>
  ): void {
    const cases: UpcomingDischargeExportedCase[] = [];
    querySnapshot.forEach((doc) => {
      cases.push(doc.data() as UpcomingDischargeExportedCase);
    });
    this.upcomingDischargeExportedCases = cases;
  }

  updateOfficers(querySnapshot: QuerySnapshot<DocumentData>): void {
    querySnapshot.forEach((doc) => {
      this.officers[doc.id] = doc.data() as OfficerInfo;
    });
  }

  setCompliantReportingStatus(
    newStatus: CompliantReportingStatusCode,
    toggleDeniedReason?: string
  ): void {
    if (
      !this.activeClient?.compliantReportingCase ||
      !this.rootStore.userStore.userEmail
    )
      return;

    const { personExternalId, deniedReasons } =
      this.activeClient.compliantReportingCase;

    let newDeniedReasons: string[] = [];
    if (toggleDeniedReason) {
      newDeniedReasons = xor(deniedReasons || [], [toggleDeniedReason]);
    }

    const updatedRecord = {
      personExternalId,
      status: newStatus,
      deniedReasons: newDeniedReasons,
    };

    saveCompliantReportingStatus(
      updatedRecord,
      this.rootStore.userStore.userEmail
    );
  }

  async sendCompliantReportingUpdate(text: string): Promise<void> {
    if (!this.activeClientId || !this.rootStore.userStore.userEmail) return;

    await saveCompliantReportingNote({
      text,
      personExternalId: this.activeClientId,
      creator: this.rootStore.userStore.userEmail,
    });
  }
}
