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
import { IconSVG, Need, NeedState } from "@recidiviz/design-system";
import moment from "moment";
import * as React from "react";
import { observer } from "mobx-react-lite";
import { Caption, CaseCardBody, CaseCardInfo } from "./CaseCard.styles";
import { Client } from "../../stores/ClientsStore/Client";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import { NeedsActionFlow } from "../NeedsActionFlow/NeedsActionFlow";
import TEST_IDS from "../TestIDs";
import { LONG_DATE_FORMAT } from "../../utils";

interface NeedsRiskAssessmentProps {
  className: string;
  client: Client;
}

const getAssessmentScoreText = (client: Client) =>
  client.assessmentScore !== null ? `Score: ${client.assessmentScore}` : null;

const NeedsRiskAssessment: React.FC<NeedsRiskAssessmentProps> = ({
  className,
  client,
}: NeedsRiskAssessmentProps) => {
  const {
    needsMet: { assessment: met },
    mostRecentAssessmentDate,
  } = client;

  const title = met ? `Risk Assessment: Up to Date` : `Risk Assessment Needed`;

  let caption;
  if (mostRecentAssessmentDate) {
    caption = (
      <>
        <div>
          {getAssessmentScoreText(client)}
          {client.assessmentScoreDetails &&
            `, ${client.assessmentScoreDetails}`}
        </div>
        <div>
          Last assessed on {mostRecentAssessmentDate.format(LONG_DATE_FORMAT)}
        </div>
      </>
    );
  } else {
    caption = `A risk assessment has never been completed`;
  }

  let nextAssessmentText;
  if (client.nextAssessmentDate) {
    const beginningOfDay = moment().startOf("day");

    // Upcoming assessments are set to day boundaries. Use the beginning of today when calculating distance
    // Thus, showing a minimum unit of "In a day" rather than "In X hours"
    const timeAgo = client.nextAssessmentDate.from(beginningOfDay, true);

    if (client.nextAssessmentDate.isSame(beginningOfDay, "day")) {
      nextAssessmentText = <div>Next assessment recommended today</div>;
    } else if (client.nextAssessmentDate.isAfter(beginningOfDay, "day")) {
      nextAssessmentText = <div>Next assessment recommended in {timeAgo}</div>;
    } else {
      nextAssessmentText = <div>Risk assessment recommended {timeAgo} ago</div>;
    }
  } else {
    nextAssessmentText = (
      <div>No regular assessment is needed, unless prompted by an event</div>
    );
  }

  return (
    <CaseCardBody
      className={className}
      data-testid={TEST_IDS.NEEDS_RISK_ASSESSMENT}
    >
      <Need
        kind={IconSVG.NeedsRiskAssessment}
        state={client.needsMet.assessment ? NeedState.MET : NeedState.NOT_MET}
      />
      <CaseCardInfo>
        <strong>{title}</strong>
        <br />
        <Caption>
          {caption}
          {nextAssessmentText}
        </Caption>

        <NeedsActionFlow
          actionable={!met}
          client={client}
          resolve={CaseUpdateActionType.COMPLETED_ASSESSMENT}
          dismiss={CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA}
        />
      </CaseCardInfo>
    </CaseCardBody>
  );
};

export default observer(NeedsRiskAssessment);
