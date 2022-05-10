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
import { v4 as uuidv4 } from "uuid";
import { Client } from "./stores/ClientsStore";
import { CaseUpdateActionType } from "./stores/CaseUpdatesStore/CaseUpdates";
import { isIE11 } from "./utils";

const sessionId = uuidv4();

export const identify = (
  userId: string,
  userHash: string,
  metadata?: Record<string, unknown>
): void => {
  const fullMetadata = metadata || {};
  fullMetadata.sessionId = sessionId;
  if (process.env.NODE_ENV !== "development") {
    window.analytics.identify(userId, fullMetadata, {
      integrations: {
        Intercom: {
          hideDefaultLauncher: isIE11(),
          userHash,
        },
      },
    });
  } else {
    // eslint-disable-next-line
    console.log(
      `[Analytics] Identifying user id: ${userId}, with metadata: ${JSON.stringify(
        fullMetadata
      )}`
    );
  }
};

const track = (eventName: string, metadata?: Record<string, unknown>): void => {
  const fullMetadata = metadata || {};
  fullMetadata.sessionId = sessionId;

  if (process.env.NODE_ENV !== "development") {
    window.analytics.track(eventName, fullMetadata);
  } else {
    // eslint-disable-next-line
    console.log(
      `[Analytics] Tracking event name: ${eventName}, with metadata: ${JSON.stringify(
        fullMetadata
      )}`
    );
  }
};

export const trackPersonSelected = (client: Client): void => {
  track("frontend.person_selected", {
    personExternalId: client.personExternalId,
  });
};

export const trackPersonActionRemoved = (
  client: Client,
  updateId: string,
  actionRemoved: CaseUpdateActionType
): void => {
  track("frontend.person_action_removed", {
    personExternalId: client.personExternalId,
    updateId,
    actionRemoved,
  });
};

export const trackPersonActionTaken = (
  client: Client,
  actionTaken: CaseUpdateActionType
): void => {
  track("frontend.person_action_taken", {
    personExternalId: client.personExternalId,
    actionTaken,
  });
};

export const trackScrolledToBottom = (): void => {
  track("frontend.scrolled_to_bottom");
};

export const trackSearchBarEnterPressed = (searchTerm: string): void => {
  track("frontend.search_bar_enter_pressed", { searchTerm });
};

export const trackSearchBarFocused = (): void => {
  track("frontend.search_bar_focused");
};
