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
import { formatISO } from "date-fns";
import { observer } from "mobx-react-lite";
import React from "react";
import styled from "styled-components/macro";

import DisplayDate from "../DisplayDate";
import StatusUpdated from "../LastUpdated";
import { useDataStore } from "../StoreProvider";
import UserName from "../UserName";
import { formatTimestampRelative } from "../utils";
import CompliantReportingDenial from "./CompliantReportingDenial";
import ProfileUpcomingDischarge from "./ProfileUpcomingDischarge";
import SummaryItem from "./SummaryItem";
import UpdatesInput from "./UpdatesInput";

const ProfileContents = styled.div`
  display: grid;
  grid-template-columns: 1fr;
  grid-template-rows: auto 1fr auto;
  height: 100%;
  width: 100%;

  & > *:not(:last-child) {
    border-top: 1px solid ${palette.slate20};
  }
`;

const ContentsWrapper = styled.div`
  grid-column: 1;
  padding: 32px;
  overflow: auto;
  grid-order: 2;
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

  const compliantReportingCase = caseStore.activeClient?.compliantReportingCase;

  return (
    <ProfileContents>
      <ProfileUpcomingDischarge />
      <ContentsWrapper>
        {!compliantReportingCase ? (
          <div />
        ) : (
          <>
            <H4>Compliant Reporting</H4>
            <CriteriaList>
              <Criterion>
                <SummaryItem>
                  Supervision Type: {compliantReportingCase.supervisionType}
                </SummaryItem>
              </Criterion>
              <Criterion>
                <SummaryItem>
                  Supervision Level: {compliantReportingCase.supervisionLevel}{" "}
                  for{" "}
                  {formatTimestampRelative(
                    compliantReportingCase.supervisionLevelStart
                  )}
                </SummaryItem>
              </Criterion>
              <Criterion>
                <SummaryItem>
                  Offense Type:{" "}
                  {compliantReportingCase.offenseType.map((o, index, arr) => (
                    <>
                      {o}
                      {index !== arr.length - 1 && "; "}
                    </>
                  ))}
                </SummaryItem>
              </Criterion>
              <Criterion>
                <SummaryItem>
                  Negative Drug Screens (past 12 mo):{" "}
                  {compliantReportingCase.lastDrun.map((d, index, arr) => (
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
                  {compliantReportingCase.lastSanction ||
                    "No previous sanctions"}
                </SummaryItem>
              </Criterion>
              <Criterion>
                <SummaryItem>
                  Judicial District: {compliantReportingCase.judicialDistrict}
                </SummaryItem>
              </Criterion>
            </CriteriaList>

            <StatusHeading>Status</StatusHeading>
            <StatusRow>
              <StatusItem>
                <Button
                  kind={
                    compliantReportingCase.status === "ELIGIBLE"
                      ? "primary"
                      : "secondary"
                  }
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
            {compliantReportingCase.statusUpdated && (
              <StatusMetadata>
                <StatusUpdated updated={compliantReportingCase.statusUpdated} />
              </StatusMetadata>
            )}
          </>
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
