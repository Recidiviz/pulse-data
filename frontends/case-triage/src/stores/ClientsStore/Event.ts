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

import moment, { Moment } from "moment";
import { titleCase } from "../../utils";

/**
 * Types of data objects we expect in the timeline API response
 */
export type ClientEventData = ContactEventData | AssessmentEventData;

// see recidiviz/common/constants/state/state_supervision_contact.py
type ContactEventType =
  | "EXTERNAL_UNKNOWN"
  | "INTERNAL_UNKNOWN"
  | "FACE_TO_FACE"
  | "TELEPHONE"
  | "WRITTEN_MESSAGE"
  | "VIRTUAL";

const contactTypeLabels: Record<ContactEventType, string> = {
  EXTERNAL_UNKNOWN: "",
  INTERNAL_UNKNOWN: "",
  FACE_TO_FACE: "Face to Face",
  TELEPHONE: "Telephone",
  VIRTUAL: "Virtual",
  WRITTEN_MESSAGE: "Written",
};

/**
 * Maps raw enum strings to display values. Returns an empty string for "unknown"
 * as we don't actually want to display that value anywhere.
 */
export const getContactTypeLabel = (contactType: ContactEventType): string => {
  const label = contactTypeLabels[contactType];
  // fallback in case the API has provided an unsupported type
  if (label === undefined) return titleCase(contactType);

  return label;
};

type ContactEventData = {
  eventType: "CONTACT";
  eventDate: string;
  eventMetadata: {
    contactType: ContactEventType;
  };
};

type AssessmentEventData = {
  eventType: "ASSESSMENT";
  eventDate: string;
  eventMetadata: {
    scoreChange: number | null;
    score: number;
  };
};

/**
 * Timeline domain objects constructed from API response
 */
export type ClientEvent = ContactEvent | AssessmentEvent;

export type ContactEvent = Omit<ContactEventData, "eventDate"> & {
  eventDate: Moment;
};

export type AssessmentEvent = Omit<AssessmentEventData, "eventDate"> & {
  eventDate: Moment;
};

export function buildClientEvent(data: ClientEventData): ClientEvent {
  const eventDate = moment(data.eventDate);

  return { ...data, eventDate };
}
