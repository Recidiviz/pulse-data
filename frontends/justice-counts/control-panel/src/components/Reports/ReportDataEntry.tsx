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

import { trackReportUnpublished } from "../../analytics";
import { Report } from "../../shared/types";
import { useStore } from "../../stores";
import { printReportTitle } from "../../utils";
import { PageWrapper } from "../Forms";
import { Loading } from "../Loading";
import { showToast } from "../Toast";
import DataEntryForm from "./DataEntryForm";
import PublishConfirmation from "./PublishConfirmation";
import PublishDataPanel from "./PublishDataPanel";
import { FieldDescriptionProps } from "./ReportDataEntry.styles";
import ReportSummaryPanel from "./ReportSummaryPanel";

const ReportDataEntry = () => {
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [loadingError, setLoadingError] = useState<string | undefined>(
    undefined
  );
  const [activeMetric, setActiveMetric] = useState<string>("");
  const [fieldDescription, setFieldDescription] = useState<
    FieldDescriptionProps | undefined
  >(undefined);
  const [showConfirmation, setShowConfirmation] = useState(false);
  const { reportStore, userStore } = useStore();
  const params = useParams();
  const reportID = Number(params.id);
  const reportOverview = reportStore.reportOverviews[reportID] as Report;
  const reportMetrics = reportStore.reportMetrics[reportID];
  const toggleConfirmationDialogue = async () => {
    if (reportOverview.status === "PUBLISHED") {
      const response = (await reportStore.updateReport(
        reportID,
        [],
        "DRAFT"
      )) as Response;
      if (response.status === 200) {
        showToast(
          `The ${printReportTitle(
            reportStore.reportOverviews[reportID].month,
            reportStore.reportOverviews[reportID].year,
            reportStore.reportOverviews[reportID].frequency
          )} report has been unpublished and editing is enabled.`,
          true,
          undefined,
          4000
        );
        const agencyID = reportStore.reportOverviews[reportID]?.agency_id;
        const agency = userStore.userAgencies?.find((a) => a.id === agencyID);
        trackReportUnpublished(reportID, agency);
      } else {
        showToast(
          `Something went wrong unpublishing the ${printReportTitle(
            reportStore.reportOverviews[reportID].month,
            reportStore.reportOverviews[reportID].year,
            reportStore.reportOverviews[reportID].frequency
          )} report!`,
          false
        );
      }
    } else {
      setShowConfirmation(!showConfirmation);
    }
  };

  useEffect(
    () =>
      // return when's disposer so it is cleaned up if it never runs
      when(
        () => userStore.userInfoLoaded,
        async () => {
          const result = await reportStore.getReport(reportID);
          if (result instanceof Error) {
            setLoadingError(result.message);
          }
          userStore.setCurrentAgencyId(
            reportStore.reportOverviews[reportID]?.agency_id
          );
          setIsLoading(false);
        }
      ),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    []
  );

  const updateActiveMetric = (metricKey: string) => setActiveMetric(metricKey);
  const updateFieldDescription = (title?: string, description?: string) => {
    if (title && description) {
      setFieldDescription({ title, description });
    } else {
      setFieldDescription(undefined);
    }
  };

  useEffect(() => {
    const firstEnabledMetric = reportMetrics?.find((metric) => metric.enabled);
    if (reportMetrics && firstEnabledMetric)
      updateActiveMetric(firstEnabledMetric.key); // open to the first enabled metric by default
  }, [reportMetrics, reportID]);

  if (isLoading) {
    return (
      <PageWrapper>
        <Loading />
      </PageWrapper>
    );
  }

  if (!reportMetrics || !reportOverview) {
    return <PageWrapper>Error: {loadingError}</PageWrapper>;
  }

  return (
    <>
      <ReportSummaryPanel
        reportID={reportID}
        activeMetric={activeMetric}
        fieldDescription={fieldDescription}
        toggleConfirmationDialogue={toggleConfirmationDialogue}
      />
      <DataEntryForm
        reportID={reportID}
        updateActiveMetric={updateActiveMetric}
        updateFieldDescription={updateFieldDescription}
        toggleConfirmationDialogue={toggleConfirmationDialogue}
      />
      <PublishDataPanel
        reportID={reportID}
        activeMetric={activeMetric}
        fieldDescription={fieldDescription}
        toggleConfirmationDialogue={toggleConfirmationDialogue}
        isPublished={reportOverview.status === "PUBLISHED"}
      />
      {showConfirmation && (
        <PublishConfirmation
          toggleConfirmationDialogue={toggleConfirmationDialogue}
          reportID={reportID}
        />
      )}
    </>
  );
};

export default observer(ReportDataEntry);
