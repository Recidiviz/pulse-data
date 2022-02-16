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

import { Button, H4, palette } from "@recidiviz/design-system";
import { formatDistanceToNowStrict, formatISO } from "date-fns";
import { observer } from "mobx-react-lite";
import React from "react";
import styled from "styled-components/macro";

import DisplayDate from "../DisplayDate";
import StatusUpdated from "../LastUpdated";
import { useDataStore } from "../StoreProvider";
import UserName from "../UserName";
import CompliantReportingDenial from "./CompliantReportingDenial";
import SummaryItem from "./SummaryItem";
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
  line-height: 1.3;
`;

const StatusHeading = styled.h5`
  margin: 32px 0 8px;
`;

const StatusRow = styled.div`
  display: flex;
  margin-bottom: 16px;
`;

const StatusItem = styled.div`
  padding-right: 16px;
`;

const StatusMetadata = styled.div`
  font-size: 14px;
  color: ${palette.text.caption};
  margin-bottom: 32px;
`;

const Update = styled.div`
  border-top: 1px solid ${palette.slate20};
  margin: 24px 0;
  padding-top: 24px;
`;

const UpdateMeta = styled.div`
  font-size: 12px;
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
            <SummaryItem>
              Supervision Type: {client.supervisionType}
            </SummaryItem>
          </Criterion>
          <Criterion>
            <SummaryItem>
              Supervision Level: {client.supervisionLevel} for{" "}
              {formatDistanceToNowStrict(client.supervisionLevelStart.toDate())}
            </SummaryItem>
          </Criterion>
          <Criterion>
            <SummaryItem>Offense Type: {client.offenseType}</SummaryItem>
          </Criterion>
          <Criterion>
            <SummaryItem>
              Negative Drug Screens (past 12 mo):{" "}
              {client.lastDrun.map((d, index, arr) => (
                <>
                  <DisplayDate date={d.toDate()} />
                  {index !== arr.length - 1 && "; "}
                </>
              ))}
            </SummaryItem>
          </Criterion>
          <Criterion>
            <SummaryItem>
              Last Sanction (past 12 mo):{" "}
              {client.sanctionsPast1Yr || "No previous sanctions"}
            </SummaryItem>
          </Criterion>
          <Criterion>
            <SummaryItem>
              Judicial District: {client.judicialDistrict}
            </SummaryItem>
          </Criterion>
        </CriteriaList>

        <StatusHeading>Status</StatusHeading>
        <StatusRow>
          <StatusItem>
            <Button
              kind={client.status === "ELIGIBLE" ? "primary" : "secondary"}
              shape="block"
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
        {client.statusUpdated && (
          <StatusMetadata>
            <StatusUpdated updated={client.statusUpdated} />
          </StatusMetadata>
        )}

        {caseStore.activeClientUpdates.map((update) => (
          <Update key={formatISO(update.createdAt)}>
            <UpdateMeta>
              <UpdateName>
                <UserName email={update.creator} />
              </UpdateName>{" "}
              <UpdateDate>
                <DisplayDate date={update.createdAt} />
              </UpdateDate>
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
