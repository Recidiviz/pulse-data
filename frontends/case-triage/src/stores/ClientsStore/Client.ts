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
import assertNever from "assert-never";
import { makeAutoObservable, runInAction, set } from "mobx";
import moment from "moment";
import { caseInsensitiveIncludes, titleCase } from "../../utils";
import type API from "../API";
import {
  CaseUpdate,
  CaseUpdateActionType,
  CaseUpdateStatus,
} from "../CaseUpdatesStore";
import type OpportunityStore from "../OpportunityStore";
import { Opportunity, OpportunityType } from "../OpportunityStore/Opportunity";
import type ClientsStore from "./ClientsStore";
import { NoteData, Note } from "./Note";

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

export interface ClientData {
  assessmentScore: number | null;
  caseType: CaseType;
  caseUpdates: Record<CaseUpdateActionType, CaseUpdate>;
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
  needsMet: NeedsMet;
  nextAssessmentDate: APIDate;
  nextFaceToFaceDate: APIDate;
  nextHomeVisitDate: APIDate;
  preferredName?: string;
  preferredContactMethod?: PreferredContactMethod;
  notes?: NoteData[];
}

const parseDate = (date?: APIDate) => {
  if (!date) {
    return null;
  }

  return moment(date);
};

export type DueDateStatus = "OVERDUE" | "UPCOMING" | null;

function getDueDateStatus({
  dueDate,
  upcomingWindowDays,
}: {
  dueDate: moment.Moment | null;
  upcomingWindowDays: number;
}) {
  const currentTime = moment(Date.now());
  if (dueDate) {
    const differenceInDays = dueDate.diff(currentTime, "days");

    if (differenceInDays < 0) {
      return "OVERDUE";
    }

    if (differenceInDays <= upcomingWindowDays) {
      return "UPCOMING";
    }
  }

  return null;
}

export const AlertKindList = [
  OpportunityType.OVERDUE_DOWNGRADE,
  "EMPLOYMENT",
  "ASSESSMENT_OVERDUE",
  "ASSESSMENT_UPCOMING",
  "CONTACT_OVERDUE",
  "CONTACT_UPCOMING",
] as const;
type AlertKind = typeof AlertKindList[number];
const ALERT_PRIORITIES = AlertKindList.reduce((memo, kind, index) => {
  return { ...memo, [kind]: index };
}, {} as Record<AlertKind, number>);

type AlertBase = { priority: number };

type OpportunityAlert = AlertBase & {
  kind: Extract<AlertKind, OpportunityType>;
  opportunity: Opportunity;
};
type DateAlert = AlertBase & {
  kind: Extract<
    AlertKind,
    | "ASSESSMENT_OVERDUE"
    | "ASSESSMENT_UPCOMING"
    | "CONTACT_OVERDUE"
    | "CONTACT_UPCOMING"
  >;
  status: NonNullable<DueDateStatus>;
  date: moment.Moment;
};
type PlainAlert = AlertBase & {
  kind: Exclude<AlertKind, OpportunityAlert["kind"] | DateAlert["kind"]>;
};

type Alert = PlainAlert | OpportunityAlert | DateAlert;

/**
 * Data model representing a single client. To instantiate, it is recommended
 * to use the static `build` method.
 */
export class Client {
  assessmentScore: number | null;

  caseType: CaseType;

  caseUpdates: Record<CaseUpdateActionType, CaseUpdate>;

  currentAddress: string;

  emailAddress?: string;

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

  supervisionLevel: SupervisionLevel;

  supervisionLevelText: string;

  supervisionStartDate: moment.Moment | null;

  supervisionType: string;

  constructor(
    clientData: ClientData & { supervisionLevelText: string },
    private api: API,
    private clientsStore: ClientsStore,
    private opportunityStore: OpportunityStore
  ) {
    makeAutoObservable<Client, "api" | "clientStore" | "opportunityStore">(
      this,
      {
        api: false,
        clientStore: false,
        opportunityStore: false,
      }
    );

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
    this.needsMet = clientData.needsMet;
    this.preferredName = clientData.preferredName;
    this.preferredContactMethod = clientData.preferredContactMethod;
    this.supervisionLevelText = clientData.supervisionLevelText;

    // fields that require some processing
    this.notes = (clientData.notes || []).map(
      (noteData) => new Note(noteData, api)
    );
    const { given_names: given, surname } = clientData.fullName;

    this.name = `${titleCase(given)} ${titleCase(surname)}`;

    this.formalName = titleCase(surname);
    if (given) {
      this.formalName += `, ${titleCase(given)}`;
    }

    this.possessivePronoun = POSSESSIVE_PRONOUNS[clientData.gender] || "their";

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
  }: {
    api: API;
    client: ClientData;
    clientsStore: ClientsStore;
    opportunityStore: OpportunityStore;
  }): Client {
    const supervisionLevelText =
      clientsStore.policyStore.getSupervisionLevelNameForClient(client);
    return new Client(
      { ...client, supervisionLevelText },
      api,
      clientsStore,
      opportunityStore
    );
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

  findInProgressUpdate(
    actions: CaseUpdateActionType[]
  ): CaseUpdate | undefined {
    const found = actions.find((action) => this.hasInProgressUpdate(action));
    if (!found) {
      return;
    }

    return this.caseUpdates[found];
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

  get nextContactDate(): moment.Moment | null {
    // TODO(#7320): Our current `nextHomeVisitDate` determines when the next home visit
    // should be assuming that home visits must be F2F visits and not collateral visits.
    // As a result, until we do more investigation into what the appropriate application
    // of state policy is, we're not showing home visits as the next contact dates.
    return this.nextFaceToFaceDate;
  }

  get contactStatus(): DueDateStatus {
    return getDueDateStatus({
      dueDate: this.nextContactDate,
      upcomingWindowDays: 7,
    });
  }

  get riskAssessmentStatus(): DueDateStatus {
    return getDueDateStatus({
      dueDate: this.nextAssessmentDate,
      upcomingWindowDays: 30,
    });
  }

  get opportunities(): Opportunity[] {
    return (
      this.opportunityStore.opportunitiesByPerson?.[this.personExternalId] || []
    );
  }

  get alerts(): Alert[] {
    const alerts: (Alert | undefined)[] = AlertKindList.map((kind) => {
      switch (kind) {
        case OpportunityType.OVERDUE_DOWNGRADE: {
          const opportunity = this.opportunities.find(
            (opp) => opp.opportunityType === kind
          );
          if (opportunity && !opportunity.deferredUntil) {
            return { kind, opportunity, priority: ALERT_PRIORITIES[kind] };
          }
          break;
        }
        case "EMPLOYMENT":
          if (!this.needsMet.employment) {
            return { kind, priority: ALERT_PRIORITIES[kind] };
          }
          break;
        case "ASSESSMENT_OVERDUE":
          if (
            this.riskAssessmentStatus === "OVERDUE" &&
            this.nextAssessmentDate
          ) {
            return {
              kind,
              status: this.riskAssessmentStatus,
              date: this.nextAssessmentDate,
              priority: ALERT_PRIORITIES[kind],
            };
          }
          break;
        case "ASSESSMENT_UPCOMING":
          if (
            this.riskAssessmentStatus === "UPCOMING" &&
            this.nextAssessmentDate
          ) {
            return {
              kind,
              status: this.riskAssessmentStatus,
              date: this.nextAssessmentDate,
              priority: ALERT_PRIORITIES[kind],
            };
          }
          break;
        case "CONTACT_OVERDUE":
          if (this.contactStatus === "OVERDUE" && this.nextContactDate) {
            return {
              kind,
              status: this.contactStatus,
              date: this.nextContactDate,
              priority: ALERT_PRIORITIES[kind],
            };
          }
          break;
        case "CONTACT_UPCOMING":
          if (this.contactStatus === "UPCOMING" && this.nextContactDate) {
            return {
              kind,
              status: this.contactStatus,
              date: this.nextContactDate,
              priority: ALERT_PRIORITIES[kind],
            };
          }
          break;
        default:
          assertNever(kind);
      }

      return undefined;
    });

    return alerts.filter((alert): alert is Alert => alert !== undefined);
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
}
