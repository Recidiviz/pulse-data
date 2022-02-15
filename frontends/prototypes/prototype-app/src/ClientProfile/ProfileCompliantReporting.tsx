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

import { Button, H4, Icon, IconSVG, palette } from "@recidiviz/design-system";
import { format, formatDistanceToNowStrict, formatISO } from "date-fns";
import { observer } from "mobx-react-lite";
import React from "react";
import styled from "styled-components/macro";

import { useDataStore } from "../StoreProvider";
import CompliantReportingDenial from "./CompliantReportingDenial";
import { IconPad } from "./styles";
import UpdatesInput from "./UpdatesInput";

const ProfileContents = styled.div`
  display: grid;
  grid-template-columns: 1fr;
  grid-template-rows: 1fr auto;
  height: 100%;
  width: 100%;
  grid-area: 2 / 1;
`;

const ContentsWrapper = styled.div`
  grid-area: 1/1;
  padding: 32px;
  overflow: auto;
`;

const CriteriaList = styled.ul`
  margin: 16px 0;
`;

const Criterion = styled.li`
  margin: 8px 0;
`;

const Checkbox: React.FC = () => {
  return (
    <IconPad>
      <Icon kind={IconSVG.Success} color={palette.signal.highlight} size={15} />
    </IconPad>
  );
};

const StatusRow = styled.div`
  display: flex;
  margin: 32px 0;
`;

const StatusItem = styled.div`
  padding-right: 16px;
`;

const Update = styled.div`
  border-top: 1px solid ${palette.slate20};
  margin: 24px 0;
  padding-top: 24px;
`;

const UpdateMeta = styled.div`
  font-size: 14px;
  margin-bottom: 8px;
`;

const UpdateName = styled.span`
  display: inline-block;
  margin-right: 1em;
`;

const UpdateDate = styled.span`
  color: ${palette.slate80};
`;

const UpdateText = styled.div``;

const ProfileCompliantReporting: React.FC = () => {
  const { caseStore } = useDataStore();

  const client = caseStore.activeClient;
  if (!client) return null;

  return (
    <ProfileContents>
      <ContentsWrapper>
        <H4>Compliant Reporting</H4>
        <CriteriaList>
          <Criterion>
            <Checkbox />
            {client.supervisionLevel} for{" "}
            {formatDistanceToNowStrict(client.supervisionLevelStart)}
          </Criterion>
          <Criterion>
            <Checkbox />
            {client.offenseType}
          </Criterion>
          <Criterion>
            <Checkbox />
            {client.lastDRUN
              .map((d) => `DRUN on ${format(d, "M/d/yyyy")}`)
              .join(", ")}
          </Criterion>
          <Criterion>
            <Checkbox />
            {client.sanctionsPast1Yr.length
              ? client.sanctionsPast1Yr.join(", ")
              : "No previous sanctions"}
          </Criterion>
        </CriteriaList>

        <StatusRow>
          <StatusItem>
            <Button
              kind={client.status === "ELIGIBLE" ? "primary" : "secondary"}
              onClick={() => {
                caseStore.setCompliantReportingStatus("ELIGIBLE");
              }}
            >
              Eligible
            </Button>
          </StatusItem>
          <StatusItem>
            <CompliantReportingDenial />
          </StatusItem>
        </StatusRow>
        {caseStore.activeClientUpdates.map((update) => (
          <Update key={formatISO(update.createdAt)}>
            <UpdateMeta>
              <UpdateName>{update.creator}</UpdateName>{" "}
              <UpdateDate>{format(update.createdAt, "LLL d, yyyy")}</UpdateDate>
            </UpdateMeta>
            <UpdateText>{update.text}</UpdateText>
          </Update>
        ))}
      </ContentsWrapper>
      <UpdatesInput />
    </ProfileContents>
  );
};

export default observer(ProfileCompliantReporting);
