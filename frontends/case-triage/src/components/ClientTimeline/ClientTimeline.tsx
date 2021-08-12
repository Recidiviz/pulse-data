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

import { observer } from "mobx-react-lite";
import React from "react";
import { useRootStore } from "../../stores";
import Loading from "../Loading";
import { AssessmentEventDetails } from "./AssessmentEventDetails";
import {
  TimelineEntry,
  TimelineEntryContents,
  TimelineEntryDate,
  TimelineWrapper,
} from "./ClientTimeline.styles";
import { ContactEventDetails } from "./ContactEventDetails";

export const ClientTimeline = observer((): JSX.Element | null => {
  const {
    clientsStore: { activeClient },
  } = useRootStore();
  if (!activeClient) return null;

  if (!activeClient.isTimelineLoaded) {
    return <Loading />;
  }

  return (
    <TimelineWrapper>
      {activeClient.timeline.map((event, index) => (
        // eslint-disable-next-line react/no-array-index-key
        <TimelineEntry key={index}>
          <TimelineEntryDate>
            {event.eventDate.format("M/D/YY")}
          </TimelineEntryDate>
          <TimelineEntryContents>
            {event.eventType === "CONTACT" ? (
              <ContactEventDetails {...event} />
            ) : (
              <AssessmentEventDetails {...event} />
            )}
          </TimelineEntryContents>
        </TimelineEntry>
      ))}
    </TimelineWrapper>
  );
});
