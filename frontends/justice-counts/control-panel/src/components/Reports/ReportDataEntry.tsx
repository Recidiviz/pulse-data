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

import { when } from "mobx";
import { observer } from "mobx-react-lite";
import React, { useEffect } from "react";
import { useParams } from "react-router-dom";
import styled from "styled-components/macro";

import { useStore } from "../../stores";

const ReportDataEntry = () => {
  const { reportStore, userStore } = useStore();
  const params = useParams();
  const reportID = Number(params.id);

  useEffect(
    () =>
      // return when's disposer so it is cleaned up if it never runs
      when(
        () => userStore.userInfoLoaded,
        () => reportStore.mockGetReport(reportID)
      ),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    []
  );

  const reportOverview = reportStore.reportOverviews[reportID];
  const reportMetrics = reportStore.reportMetrics[reportID];
  const Container = styled.div`
    padding: 100px;
  `;

  if (!reportMetrics || !reportOverview) {
    return <Container>Loading...</Container>;
  }

  return (
    <Container>
      <Container>Report Overview: {JSON.stringify(reportOverview)}</Container>
      <Container>Report Metrics: {JSON.stringify(reportMetrics)}</Container>
    </Container>
  );
};

export default observer(ReportDataEntry);
