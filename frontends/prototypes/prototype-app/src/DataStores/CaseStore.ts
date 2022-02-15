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
import { parseISO } from "date-fns";
import { DocumentData, onSnapshot, QuerySnapshot } from "firebase/firestore";
import { assign, camelCase, mapKeys, xor } from "lodash";
import { makeAutoObservable, when } from "mobx";

import {
  compliantReportingStatusQuery,
  compliantReportingUpdatesQuery,
  saveCompliantReportingNote,
  saveCompliantReportingStatus,
} from "../firebase";
import { titleCase } from "../utils";
import fixture from "./__fixtures__/cases.json";
import type { RootStore } from "./RootStore";

type ConstructorProps = {
  rootStore: RootStore;
};

type ClientFullName = {
  givenNames?: string;
  middleName?: string;
  surname?: string;
};

type CompliantReportingExportedCase = {
  personExternalId: string;
  nameParts: ClientFullName;
  personName: string;
  officerId: string;
  supervisionType: string;
  judicialDistrict: string;
  supervisionLevel: string;
  supervisionLevelStart: Date;
  offenseType: string;
  lastDRUN: Date[];
  sanctionsPast1Yr: string[];
};

type CompliantReportingStatusCode = "ELIGIBLE" | "DENIED";

export type CompliantReportingStatusRecord = {
  personExternalId: string;
  status?: CompliantReportingStatusCode;
  deniedReasons?: string[];
};

export type CompliantReportingCase = CompliantReportingExportedCase &
  CompliantReportingStatusRecord;

export type CompliantReportingUpdateContents = {
  creator: string;
  text: string;
  personExternalId: string;
};

export type CompliantReportingUpdate = CompliantReportingUpdateContents & {
  createdAt: Date;
};

function camelCaseKeys(obj: Record<string, unknown>) {
  return mapKeys(obj, (v, k) => camelCase(k));
}

// possibly not all of this casting will be necessary with real data
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const createCaseRecords = (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rawData: Record<string, any>[]
): CompliantReportingCase[] => {
  return rawData.map((rawRecord) => {
    const camelCaseRecord = camelCaseKeys(rawRecord);
    const nameParts = camelCaseKeys(JSON.parse(rawRecord.person_name));
    return {
      ...camelCaseRecord,
      nameParts,
      personName: `${titleCase(nameParts.givenNames as string)} ${titleCase(
        nameParts.surname as string
      )}`,
      supervisionLevelStart: parseISO(rawRecord.supervision_level_start),
      lastDRUN: rawRecord.last_DRUN.map(parseISO),
    } as unknown as CompliantReportingCase;
  });
};

export default class CaseStore {
  readonly rootStore: RootStore;

  compliantReportingExportedCases: CompliantReportingExportedCase[] = [];

  compliantReportingStatuses: CompliantReportingStatusRecord[] = [];

  compliantReportingUpdates: CompliantReportingUpdate[] = [];

  activeClientId?: string;

  constructor({ rootStore }: ConstructorProps) {
    makeAutoObservable(this, { rootStore: false });

    this.rootStore = rootStore;

    this.compliantReportingExportedCases = createCaseRecords(fixture);

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
      }
    );
  }

  setActiveClient(clientId?: string): void {
    this.activeClientId = clientId;
  }

  get activeClient(): CompliantReportingCase | undefined {
    return this.compliantReportingCases.find(
      (record) => record.personExternalId === this.activeClientId
    );
  }

  get activeClientUpdates(): CompliantReportingUpdate[] {
    return this.compliantReportingUpdates.filter(
      (r) => r.personExternalId === this.activeClientId
    );
  }

  get compliantReportingCases(): CompliantReportingCase[] {
    return this.compliantReportingExportedCases.map((caseData) => {
      const status = this.compliantReportingStatuses.find(
        (s) => s.personExternalId === caseData.personExternalId
      );
      return assign({}, caseData, status);
    });
  }

  private updateCompliantReportingStatuses(
    querySnapshot: QuerySnapshot<DocumentData>
  ): void {
    const statuses: CompliantReportingStatusRecord[] = [];
    querySnapshot.forEach((doc) => {
      statuses.push(doc.data() as CompliantReportingStatusRecord);
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

  setCompliantReportingStatus(
    newStatus: CompliantReportingStatusCode,
    toggleDeniedReason?: string
  ): void {
    if (!this.activeClient) return;

    const { personExternalId, deniedReasons } = this.activeClient;

    let newDeniedReasons: string[] = [];
    if (toggleDeniedReason) {
      newDeniedReasons = xor(deniedReasons || [], [toggleDeniedReason]);
    }

    const updatedRecord = {
      personExternalId,
      status: newStatus,
      deniedReasons: newDeniedReasons,
    };

    saveCompliantReportingStatus(updatedRecord);
  }

  async sendCompliantReportingUpdate(text: string): Promise<void> {
    if (!this.activeClientId) return;

    await saveCompliantReportingNote({
      text,
      personExternalId: this.activeClientId,
      creator: this.rootStore.userStore.userName,
    });
  }
}
