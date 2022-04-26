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
import React, { Fragment, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";

import {
  ArrowDownIcon,
  Badge,
  Cell,
  FilterBar,
  FilterBy,
  FilterOptions,
  Label,
  LabelRow,
  NoReportsDisplay,
  ReportsHeader,
  ReportsPageTitle,
  Row,
  SortBy,
  Table,
} from "../components/Reports";
import { ReportOverview } from "../shared/types";
import { useStore } from "../stores";
import {
  normalizeString,
  printCommaSeparatedList,
  printElapsedDaysSinceDate,
  printReportTitle,
  removeSnakeCase,
} from "../utils";

enum ReportStatusFilterOption {
  AllReports = "All Reports",
  Draft = "Draft",
  Published = "Published",
  NotStarted = "Not_Started",
}

const reportListColumnTitles = [
  "Report Period",
  "Frequency",
  "Editors",
  "Last Modified",
];

const Reports: React.FC = () => {
  const { reportStore, userStore } = useStore();
  const navigate = useNavigate();

  const [reportsFilter, setReportsFilter] = useState<string>("allreports");

  const filterReportsBy = (
    e: React.MouseEvent<HTMLDivElement, MouseEvent>
  ): void => {
    const { id } = e.target as HTMLDivElement;
    const normalizedID = normalizeString(id);
    setReportsFilter(normalizedID);
  };

  const renderReportYearRow = (
    filteredReports: ReportOverview[],
    currentIndex: number,
    currentReportYear: number
  ): JSX.Element | undefined => {
    const indexIsLessThanListOfReports =
      currentIndex + 1 < filteredReports.length;
    const nextReportYear =
      indexIsLessThanListOfReports && filteredReports[currentIndex + 1].year;

    if (indexIsLessThanListOfReports && nextReportYear !== currentReportYear) {
      return <Row noHover>{nextReportYear}</Row>;
    }
  };

  useEffect(
    () =>
      // return when's disposer so it is cleaned up if it never runs
      when(
        () => userStore.userInfoLoaded,
        () => reportStore.getReportOverviews()
      ),
    [reportStore, userStore]
  );

  const filteredReportsMemoized = React.useMemo(
    () =>
      reportsFilter === "allreports"
        ? reportStore.reportOverviewList
        : reportStore.reportOverviewList.filter(
            (report) => normalizeString(report.status) === reportsFilter
          ),
    [reportStore.reportOverviewList, reportsFilter]
  );

  return (
    <>
      <ReportsHeader>
        <ReportsPageTitle>Reports</ReportsPageTitle>

        {/* Filter Reports By */}
        <FilterBar>
          <FilterOptions>
            {Object.values(ReportStatusFilterOption).map((option) => (
              <FilterBy
                key={option}
                id={option}
                selected={normalizeString(option) === reportsFilter}
                onClick={(e) => filterReportsBy(e)}
              >
                {removeSnakeCase(option)}
              </FilterBy>
            ))}
          </FilterOptions>

          {/* Sort By */}
          <SortBy>
            Sort by Reporting Period
            <ArrowDownIcon />
          </SortBy>
        </FilterBar>

        {/* Labels */}
        <LabelRow>
          {reportListColumnTitles.map((title) => (
            <Label key={title}>{title}</Label>
          ))}
        </LabelRow>
      </ReportsHeader>

      {/* Reports List Table */}
      <Table>
        {filteredReportsMemoized.length > 0 ? (
          filteredReportsMemoized.map(
            (report: ReportOverview, index: number) => (
              <Fragment key={report.id}>
                <Row
                  onClick={() => {
                    navigate(`/reports/${report.id}`);
                  }}
                >
                  {/* Report Period */}
                  <Cell id="report_period">
                    {printReportTitle(
                      report.month,
                      report.year,
                      report.frequency
                    )}
                    <Badge status={report.status}>
                      {removeSnakeCase(report.status).toLowerCase()}
                    </Badge>
                  </Cell>

                  {/* Status */}
                  <Cell capitalize>{report.frequency.toLowerCase()}</Cell>

                  {/* Editors */}
                  <Cell>
                    {report.editors.length === 0
                      ? "-"
                      : printCommaSeparatedList(report.editors)}
                  </Cell>

                  {/* Last Modified */}
                  <Cell>
                    {!report.last_modified_at
                      ? "-"
                      : printElapsedDaysSinceDate(report.last_modified_at)}
                  </Cell>
                </Row>

                {/* Report Year Marker */}
                {renderReportYearRow(
                  filteredReportsMemoized,
                  index,
                  report.year
                )}
              </Fragment>
            )
          )
        ) : (
          <NoReportsDisplay>No reports to display.</NoReportsDisplay>
        )}
        {userStore.userAgencies?.length === 0 && (
          <NoReportsDisplay>
            It looks like no agency is tied to this account. Please reach out to
            your Recidiviz contact for assistance.
          </NoReportsDisplay>
        )}
      </Table>
    </>
  );
};

export default observer(Reports);
