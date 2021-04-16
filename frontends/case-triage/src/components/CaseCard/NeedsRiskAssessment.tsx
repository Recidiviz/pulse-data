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
import {
  Caption,
  CaseCardBody,
  CaseCardInfo,
  CheckboxButtonContainer,
} from "./CaseCard.styles";
import { NeedsCheckboxButton } from "./NeedsCheckboxButton";
import { DecoratedClient } from "../../stores/ClientsStore/Client";
import { useRootStore } from "../../stores";
import {
  ScoreMinMax,
  ScoreMinMaxBySupervisionLevel,
} from "../../stores/PolicyStore";
import { titleCase } from "../../utils";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";

interface NeedsRiskAssessmentProps {
  className: string;
  client: DecoratedClient;

  onStatusChanged: (helped: boolean) => void;
}

const getAssessmentLevelText = (
  client: DecoratedClient,
  cutoffs?: ScoreMinMaxBySupervisionLevel
) => {
  if (cutoffs) {
    const cutoff = cutoffs[client.supervisionLevel];
    if (!cutoff) {
      return null;
    }

    return (
      <>
        , {titleCase(client.supervisionLevelText)}, ({titleCase(client.gender)}{" "}
        {getCutoffsText(cutoff)})
      </>
    );
  }

  return null;
};

const getAssessmentScoreText = (client: DecoratedClient) =>
  client.assessmentScore !== null ? `Score: ${client.assessmentScore}` : null;

const getCutoffsText = ([min, max]: ScoreMinMax) => {
  if (max === null) {
    return `${min}+`;
  }

  return `${min}-${max}`;
};

const NeedsRiskAssessment: React.FC<NeedsRiskAssessmentProps> = ({
  className,
  client,
  onStatusChanged,
}: NeedsRiskAssessmentProps) => {
  const [needChecked, setNeedChecked] = React.useState(false);
  React.useEffect(() => {
    setNeedChecked(false);
  }, [client]);

  const { policyStore } = useRootStore();
  const supervisionLevelCutoffs = policyStore.getSupervisionLevelCutoffsForClient(
    client
  );

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
          {getAssessmentLevelText(client, supervisionLevelCutoffs)}
        </div>
        <div>
          Last assessed on {mostRecentAssessmentDate.format("MMMM Do, YYYY")}
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

  const onToggleCheck = (checked: boolean) => {
    setNeedChecked(checked);
    onStatusChanged(checked);
  };

  return (
    <CaseCardBody className={className}>
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
        {!client.needsMet.assessment ? (
          <CheckboxButtonContainer>
            <NeedsCheckboxButton
              checked={needChecked}
              inProgress={client.inProgressActions?.includes(
                CaseUpdateActionType.COMPLETED_ASSESSMENT
              )}
              onToggleCheck={onToggleCheck}
            >
              I completed their risk assessment
            </NeedsCheckboxButton>
          </CheckboxButtonContainer>
        ) : null}
      </CaseCardInfo>
    </CaseCardBody>
  );
};

export default NeedsRiskAssessment;
