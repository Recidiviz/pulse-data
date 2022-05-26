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
import React, { useEffect, useState } from "react";
import { useParams } from "react-router-dom";

import { Report } from "../../shared/types";
import { useStore } from "../../stores";
import { PageWrapper } from "../Forms";
import DataEntryForm from "./DataEntryForm";
import PublishDataPanel from "./PublishDataPanel";
import ReportSummaryPanel from "./ReportSummaryPanel";

const ReportDataEntry = () => {
  const [activeMetric, setActiveMetric] = useState<string>("");
  const [fieldDescription, setFieldDescription] = useState<{
    title: string;
    description: string;
  }>({
    title: "",
    description: "",
  });
  const { reportStore, userStore } = useStore();
  const params = useParams();
  const reportID = Number(params.id);

  useEffect(
    () =>
      // return when's disposer so it is cleaned up if it never runs
      when(
        () => userStore.userInfoLoaded,
        () => reportStore.getReport(reportID)
      ),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    []
  );

  const reportOverview = reportStore.reportOverviews[reportID] as Report;
  const reportMetrics = reportStore.reportMetrics[reportID];

  const updateActiveMetric = (metricKey: string) => setActiveMetric(metricKey);
  const updateFieldDescription = (title: string, description: string) =>
    setFieldDescription({ title, description });

  useEffect(() => {
    if (reportMetrics) updateActiveMetric(reportMetrics[0].key); // open to the first metric by default
  }, [reportMetrics, reportID]);

  if (!reportMetrics || !reportOverview) {
    return <PageWrapper>Loading...</PageWrapper>;
  }

  return (
    <PageWrapper>
      <ReportSummaryPanel
        reportID={reportID}
        updateActiveMetric={updateActiveMetric}
        activeMetric={activeMetric}
      />
      <DataEntryForm
        reportID={reportID}
        updateFieldDescription={updateFieldDescription}
      />
      <PublishDataPanel
        reportID={reportID}
        fieldDescription={fieldDescription}
      />
    </PageWrapper>
  );
};

export default observer(ReportDataEntry);
