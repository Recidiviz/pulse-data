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
import { Client } from "./stores/ClientsStore/Client";
import { CaseUpdateActionType } from "./stores/CaseUpdatesStore/CaseUpdates";

export const identify = (
  userId: string,
  metadata?: Record<string, unknown>
): void => {
  if (process.env.NODE_ENV !== "development") {
    window.analytics.identify(userId, metadata);
  } else {
    // eslint-disable-next-line
    console.log(
      `[Analytics] Identifying user id: ${userId}, with metadata: ${JSON.stringify(
        metadata
      )}`
    );
  }
};

const track = (eventName: string, metadata?: Record<string, unknown>): void => {
  if (process.env.NODE_ENV !== "development") {
    window.analytics.track(eventName, metadata);
  } else {
    // eslint-disable-next-line
    console.log(
      `[Analytics] Tracking event name: ${eventName}, with metadata: ${JSON.stringify(
        metadata
      )}`
    );
  }
};

type ListSubsection = "ACTIVE" | "IN_PROGRESS" | "TOP_OPPORTUNITIES";

const subsectionForClient = (client: Client): ListSubsection => {
  // TODO(#5808): when top opportunities is available, update subsection calculation.
  return client.inProgressSubmissionDate ? "IN_PROGRESS" : "ACTIVE";
};

export const trackPersonCaseUpdated = (
  client: Client,
  previousActionsTaken: CaseUpdateActionType[] | undefined,
  addedActions: CaseUpdateActionType[]
): void => {
  const prevActions = previousActionsTaken || [];
  track("frontend.person_case_updated", {
    subsection: subsectionForClient(client),
    personExternalId: client.personExternalId,
    previousActionSet: prevActions,
    newActionSet: [...prevActions, ...addedActions],
  });
};

export const trackPersonSelected = (client: Client): void => {
  track("frontend.person_selected", {
    subsection: subsectionForClient(client),
    personExternalId: client.personExternalId,
  });
};

export const trackScrolledToBottom = (): void => {
  track("frontend.scrolled_to_bottom");
};
