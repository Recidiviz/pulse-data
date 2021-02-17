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
import { IconSVG, Need } from "@recidiviz/case-triage-components";
import * as React from "react";
import { Caption, CaseCardBody, CaseCardInfo } from "./CaseCard.styles";
import { DecoratedClient } from "../../stores/ClientsStore/Client";
import { useRootStore } from "../../stores";
import { SupervisionLevel } from "../../stores/ClientsStore";
import {
  ScoreMinMaxBySupervisionLevel,
  ScoreMinMax,
} from "../../stores/PolicyStore";
import { titleCase } from "../../utils";

interface NeedsRiskAssessmentProps {
  className: string;
  client: DecoratedClient;
}

const getSupervisionLevelText = (
  client: DecoratedClient,
  cutoffs?: ScoreMinMaxBySupervisionLevel,
  supervisionLevel?: SupervisionLevel
) => {
  if (cutoffs && supervisionLevel) {
    const cutoff: ScoreMinMax = cutoffs[supervisionLevel];

    return (
      <>
        , {titleCase(supervisionLevel)}, ({titleCase(client.gender)}{" "}
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
}: NeedsRiskAssessmentProps) => {
  const { policyStore } = useRootStore();
  const supervisionLevelCutoffs = policyStore.getSupervisionLevelCutoffsForClient(
    client
  );
  const assessmentSupervisionLevel = policyStore.findAssessmentSupervisionLevelForClient(
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
          {getSupervisionLevelText(
            client,
            supervisionLevelCutoffs,
            assessmentSupervisionLevel
          )}
        </div>
        Last assessed on {mostRecentAssessmentDate.format("MMMM Do, YYYY")}
      </>
    );
  } else {
    caption = `A risk assessment has never been completed`;
  }

  return (
    <CaseCardBody className={className}>
      <Need
        kind={IconSVG.NeedsRiskAssessment}
        met={client.needsMet.assessment}
      />
      <CaseCardInfo>
        <strong>{title}</strong>
        <br />
        <Caption>{caption}</Caption>
      </CaseCardInfo>
    </CaseCardBody>
  );
};

export default NeedsRiskAssessment;
