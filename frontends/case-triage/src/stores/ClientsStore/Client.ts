// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
import parsePhoneNumber from "libphonenumber-js";
import { makeAutoObservable, runInAction, set } from "mobx";
import moment from "moment";
import { caseInsensitiveIncludes, titleCase } from "../../utils";
import type API from "../API";
import {
  CaseUpdate,
  CaseUpdateActionType,
  CaseUpdateStatus,
} from "../CaseUpdatesStore";
import { CASE_UPDATE_OPPORTUNITY_ASSOCIATION } from "../CaseUpdatesStore/CaseUpdates";
import ErrorMessageStore from "../ErrorMessageStore";
import type OpportunityStore from "../OpportunityStore";
import { Opportunity } from "../OpportunityStore/Opportunity";
import type PolicyStore from "../PolicyStore";
import { ScoreMinMax } from "../PolicyStore";
import type ClientsStore from "./ClientsStore";
import { Note, NoteData } from "./Note";

/* eslint-disable camelcase */
interface ClientFullName {
  given_names: string;
  middle_name: string;
  surname: string;
}
/* eslint-enable camelcase */

export type Gender = "FEMALE" | "MALE" | "TRANS_FEMALE" | "TRANS_MALE";

export type CaseType = "GENERAL" | "SEX_OFFENDER";

export const SupervisionLevels = <const>["HIGH", "MEDIUM", "MINIMUM"];
export type SupervisionLevel = typeof SupervisionLevels[number];

type APIDate = string | moment.Moment | null;

const POSSESSIVE_PRONOUNS: Record<Gender, string> = {
  FEMALE: "her",
  MALE: "his",
  TRANS_FEMALE: "her",
  TRANS_MALE: "his",
};

export interface NeedsMet {
  assessment: boolean;
  employment: boolean;
  faceToFaceContact: boolean;
  homeVisitContact: boolean;
}

export enum PreferredContactMethod {
  Email = "EMAIL",
  Call = "CALL",
  Text = "TEXT",
}

export const PENDING_ID = "PENDING";

type CaseUpdates = { [key in CaseUpdateActionType]?: CaseUpdate };

export interface ClientData {
  assessmentScore: number | null;
  birthdate: APIDate;
  caseType: CaseType;
  caseUpdates: CaseUpdates;
  currentAddress: string;
  fullName: ClientFullName;
  employer?: string;
  gender: Gender;
  supervisionStartDate: APIDate;
  projectedEndDate: APIDate;
  supervisionType: string;
  supervisionLevel: SupervisionLevel;
  personExternalId: string;
  mostRecentFaceToFaceDate: APIDate;
  mostRecentHomeVisitDate: APIDate;
  mostRecentAssessmentDate: APIDate;
  emailAddress?: string;
  phoneNumber: null | string;
  needsMet: NeedsMet;
  nextAssessmentDate: APIDate;
  nextFaceToFaceDate: APIDate;
  nextHomeVisitDate: APIDate;
  preferredName?: string;
  preferredContactMethod?: PreferredContactMethod;
  receivingSSIOrDisabilityIncome: boolean;
  notes?: NoteData[];
}

const parseDate = (date?: APIDate) => {
  if (!date) {
    return null;
  }

  return moment(date);
};

const formatCutoffs = ([min, max]: ScoreMinMax) => {
  if (max === null) {
    return `${min}+`;
  }

  return `${min}-${max}`;
};

/**
 * Data model representing a single client. To instantiate, it is recommended
 * to use the static `build` method.
 */
export class Client {
  assessmentScore: number | null;

  birthdate: moment.Moment | null;

  caseType: CaseType;

  caseUpdates: CaseUpdates;

  currentAddress: string;

  emailAddress?: string;

  phoneNumber?: string;

  employer?: string;

  formalName: string;

  fullName: ClientFullName;

  gender: Gender;

  mostRecentAssessmentDate: moment.Moment | null;

  mostRecentFaceToFaceDate: moment.Moment | null;

  mostRecentHomeVisitDate: moment.Moment | null;

  name: string;

  needsMet: NeedsMet;

  nextAssessmentDate: moment.Moment | null;

  nextFaceToFaceDate: moment.Moment | null;

  nextHomeVisitDate: moment.Moment | null;

  notes: Note[];

  personExternalId: string;

  possessivePronoun: string;

  preferredContactMethod?: PreferredContactMethod;

  preferredName?: string;

  projectedEndDate: moment.Moment | null;

  receivingSSIOrDisabilityIncome: boolean;

  supervisionLevel: SupervisionLevel;

  supervisionStartDate: moment.Moment | null;

  supervisionType: string;

  constructor(
    clientData: ClientData,
    private api: API,
    private clientsStore: ClientsStore,
    private opportunityStore: OpportunityStore,
    private policyStore: PolicyStore,
    private errorMessageStore: ErrorMessageStore
  ) {
    makeAutoObservable<
      Client,
      "api" | "clientStore" | "opportunityStore" | "policyStore"
    >(this, {
      api: false,
      clientStore: false,
      opportunityStore: false,
      policyStore: false,
    });

    // fields that come directly from the input
    this.assessmentScore = clientData.assessmentScore;
    this.caseType = clientData.caseType;
    this.caseUpdates = clientData.caseUpdates;
    this.currentAddress = clientData.currentAddress;
    this.fullName = clientData.fullName;
    this.employer = clientData.employer;
    this.gender = clientData.gender;
    this.supervisionType = clientData.supervisionType;
    this.supervisionLevel = clientData.supervisionLevel;
    this.personExternalId = clientData.personExternalId;
    this.emailAddress = clientData.emailAddress;

    this.phoneNumber =
      typeof clientData.phoneNumber === "string"
        ? parsePhoneNumber(clientData.phoneNumber, "US")?.formatNational()
        : undefined;
    this.needsMet = clientData.needsMet;
    this.preferredName = clientData.preferredName;
    this.preferredContactMethod = clientData.preferredContactMethod;
    this.receivingSSIOrDisabilityIncome =
      clientData.receivingSSIOrDisabilityIncome;

    // fields that require some processing
    this.notes = (clientData.notes || [])
      .map((noteData) => new Note(noteData, api))
      .sort(
        (a, b) => a.createdDatetime.valueOf() - b.createdDatetime.valueOf()
      );
    const { given_names: given, surname } = clientData.fullName;

    this.name = `${titleCase(given)} ${titleCase(surname)}`;

    this.formalName = titleCase(surname);
    if (given) {
      this.formalName += `, ${titleCase(given)}`;
    }

    this.possessivePronoun = POSSESSIVE_PRONOUNS[clientData.gender] || "their";

    this.birthdate = parseDate(clientData.birthdate);
    this.supervisionStartDate = parseDate(clientData.supervisionStartDate);
    this.projectedEndDate = parseDate(clientData.projectedEndDate);
    this.mostRecentFaceToFaceDate = parseDate(
      clientData.mostRecentFaceToFaceDate
    );
    this.mostRecentHomeVisitDate = parseDate(
      clientData.mostRecentHomeVisitDate
    );
    this.mostRecentAssessmentDate = parseDate(
      clientData.mostRecentAssessmentDate
    );
    this.nextFaceToFaceDate = parseDate(clientData.nextFaceToFaceDate);
    this.nextHomeVisitDate = parseDate(clientData.nextHomeVisitDate);
    this.nextAssessmentDate = parseDate(clientData.nextAssessmentDate);
  }

  static build({
    api,
    client,
    clientsStore,
    opportunityStore,
    policyStore,
    errorMessageStore,
  }: {
    api: API;
    client: ClientData;
    clientsStore: ClientsStore;
    opportunityStore: OpportunityStore;
    policyStore: PolicyStore;
    errorMessageStore: ErrorMessageStore;
  }): Client {
    return new Client(
      { ...client },
      api,
      clientsStore,
      opportunityStore,
      policyStore,
      errorMessageStore
    );
  }

  get supervisionLevelText(): string {
    return this.policyStore.getSupervisionLevelNameForClient(this);
  }

  get supervisionLevelCutoffs(): ReturnType<
    PolicyStore["getSupervisionLevelCutoffsForClient"]
  > {
    return this.policyStore.getSupervisionLevelCutoffsForClient(this);
  }

  hasCaseUpdateInStatus(
    action: CaseUpdateActionType,
    status: CaseUpdateStatus
  ): boolean {
    return this.caseUpdates[action]?.status === status;
  }

  hasInProgressUpdate(action: CaseUpdateActionType): boolean {
    return this.hasCaseUpdateInStatus(action, CaseUpdateStatus.IN_PROGRESS);
  }

  get riskLevel(): SupervisionLevel | undefined {
    return this.policyStore.findSupervisionLevelForScore(
      this.gender,
      this.assessmentScore
    );
  }

  get riskLevelLabel(): string {
    const { riskLevel } = this;
    return riskLevel ? this.policyStore.getSupervisionLevelName(riskLevel) : "";
  }

  get assessmentScoreDetails(): string | null {
    const cutoff =
      this.riskLevel && this.supervisionLevelCutoffs?.[this.riskLevel];

    if (this.assessmentScore === null || cutoff === undefined) {
      return null;
    }
    return `${titleCase(this.riskLevelLabel)} (${titleCase(
      this.gender
    )} ${formatCutoffs(cutoff)})`;
  }

  findInProgressUpdate(
    actions: CaseUpdateActionType[]
  ): CaseUpdate | undefined {
    const found = actions.find((action) => this.hasInProgressUpdate(action));
    if (!found) {
      return;
    }

    return this.caseUpdates[found];
  }

  /**
   * Counts in-progress (non-deprecated) actions
   */
  get inProgressUpdates(): CaseUpdateActionType[] {
    return Object.values(CaseUpdateActionType)
      .map((actionType) => {
        if (this.hasInProgressUpdate(actionType)) {
          return actionType;
        }
        return undefined;
      })
      .filter(
        (actionType): actionType is CaseUpdateActionType =>
          actionType !== undefined
      );
  }

  get pendingCaseloadRemoval(): CaseUpdate | undefined {
    return (
      this.caseUpdates[CaseUpdateActionType.NOT_ON_CASELOAD] ||
      this.caseUpdates[CaseUpdateActionType.CURRENTLY_IN_CUSTODY]
    );
  }

  get isVisible(): boolean {
    return caseInsensitiveIncludes(
      this.name,
      this.clientsStore.clientSearchString
    );
  }

  get isActive(): boolean {
    return (
      this.clientsStore.clientPendingView?.personExternalId ===
        this.personExternalId ||
      this.clientsStore.activeClient?.personExternalId === this.personExternalId
    );
  }

  get hasUpcomingBirthday(): boolean {
    if (this.birthdate === null) {
      return false;
    }

    const today = moment();
    const endPeriod = today.clone().add(31, "days");
    const birthdayThisYear = moment(this.birthdate).year(today.year());
    if (
      today.isSameOrBefore(birthdayThisYear) &&
      endPeriod.isSameOrAfter(birthdayThisYear)
    ) {
      return true;
    }

    // Handle wrap-around case
    const birthdayNextYear = moment(this.birthdate).year(today.year() + 1);
    if (
      today.isSameOrBefore(birthdayNextYear) &&
      endPeriod.isSameOrAfter(birthdayNextYear)
    ) {
      return true;
    }

    return false;
  }

  get nextContactDate(): moment.Moment | null {
    // TODO(#7320): Our current `nextHomeVisitDate` determines when the next home visit
    // should be assuming that home visits must be F2F visits and not collateral visits.
    // As a result, until we do more investigation into what the appropriate application
    // of state policy is, we're not showing home visits as the next contact dates.
    return this.nextFaceToFaceDate;
  }

  get opportunities(): Opportunity[] {
    return (
      this.opportunityStore.opportunitiesByPerson?.[this.personExternalId] || []
    );
  }

  get activeOpportunities(): Opportunity[] {
    return this.opportunities.filter(
      (opp) =>
        !opp.isDeferred &&
        // not overridden by a case update
        CASE_UPDATE_OPPORTUNITY_ASSOCIATION[opp.opportunityType].every(
          (updateKey) => !this.hasInProgressUpdate(updateKey)
        )
    );
  }

  /**
   * Creates the specified note via API request. If the request succeeds,
   * the resulting `Note` object will be appended to `this.notes`.
   */
  async createNote({ text }: { text: string }): Promise<void> {
    const { api, notes } = this;

    const newNote = new Note(
      {
        createdDatetime: "",
        noteId: PENDING_ID,
        resolved: false,
        text,
        updatedDatetime: "",
      },
      api
    );

    notes.push(newNote);

    try {
      const response = await this.api.post<NoteData>("/api/create_note", {
        personExternalId: this.personExternalId,
        text,
      });

      runInAction(() => set(newNote, response));
    } catch (e) {
      runInAction(() => {
        notes.splice(notes.indexOf(newNote), 1);
      });
    }
  }

  get activeNotes(): Note[] {
    return this.notes.filter((note) => !note.resolved);
  }

  async updateReceivingSSIOrDisabilityIncome(
    markReceiving: boolean
  ): Promise<void> {
    const {
      receivingSSIOrDisabilityIncome: previousExternalIncomeStatus,
      personExternalId,
    } = this;
    runInAction(() =>
      set(this, "receivingSSIOrDisabilityIncome", markReceiving)
    );

    return this.api
      .post<void>("/api/set_receiving_ssi_or_disability_income", {
        personExternalId,
        markReceiving,
      })
      .then(() => {
        // We check to see if their opportunities have changed
        // now that we've changed SSI/Disability income information.
        this.opportunityStore.fetchOpportunities();
      })
      .catch((error) => {
        runInAction(() =>
          set(
            this,
            "receivingSSIOrDisabilityIncome",
            previousExternalIncomeStatus
          )
        );

        this.errorMessageStore.pushErrorMessage(
          "Failed to update SSI/Disability income"
        );
      });
  }
}
