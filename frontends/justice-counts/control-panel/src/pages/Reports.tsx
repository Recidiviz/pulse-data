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

import { observer } from "mobx-react-lite";
import React, { Fragment, useEffect } from "react";

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
import {
  printCommaSeparatedList,
  printElapsedDaysSinceDate,
  printReportTitle,
} from "../components/utils";
import { ReportOverview, useStore } from "../stores";

const reportListColumnTitles = [
  "Report Period",
  "Last Modified",
  "Editors",
  "Status",
];

const Reports: React.FC = () => {
  const { reportStore } = useStore();

  /*
   * In a given listen of reports, we sort it in descending order (for now) and go one by one,
   * comparing the current `report.year` and the next `report.year` on the list and print a stateless
   * row of the next `report.year` on the reports list.
   * e.g. (in descending order) compare [current report year: 2022] and [next report year: 2021],
   * then print `2021` (because the following list of reports will be for the year 2021)
   */

  const renderReportYearRow = (
    currentIndex: number,
    currentReportYear: number
  ): JSX.Element | undefined => {
    const indexIsLessThanListOfReports =
      currentIndex + 1 < reportStore.filteredReports.length;
    const nextReportYear =
      indexIsLessThanListOfReports &&
      reportStore.filteredReports[currentIndex + 1].year;

    if (indexIsLessThanListOfReports && nextReportYear !== currentReportYear) {
      return <Row noHover>{nextReportYear}</Row>;
    }
  };

  useEffect(() => {
    // should only run once every render.
    reportStore.getReports();
  }, [reportStore]);

  return (
    <>
      <ReportsHeader>
        <ReportsPageTitle>All Reports</ReportsPageTitle>

        {/* Filter Reports By */}
        <FilterBar>
          <FilterOptions>
            <FilterBy selected>All Reports</FilterBy>
            <FilterBy>Drafts</FilterBy>
            <FilterBy>Published</FilterBy>
            <FilterBy>Missing</FilterBy>
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
        {reportStore.filteredReports.length > 0 ? (
          reportStore.filteredReports.map(
            (report: ReportOverview, index: number) => (
              <Fragment key={report.id}>
                <Row published={report.status === "PUBLISHED"}>
                  {/* Report Period */}
                  <Cell id="report_period">
                    {printReportTitle(
                      report.month,
                      report.year,
                      report.frequency
                    )}
                    <Badge published={report.status === "PUBLISHED"}>
                      {report.frequency}
                    </Badge>
                  </Cell>

                  {/* Last Modified */}
                  <Cell>
                    {report.last_modified_at === ""
                      ? "-"
                      : printElapsedDaysSinceDate(+report.last_modified_at)}
                  </Cell>

                  {/* Editors */}
                  <Cell>
                    {report.editors.length === 0
                      ? "-"
                      : printCommaSeparatedList(report.editors)}
                  </Cell>

                  {/* Status */}
                  <Cell capitalize>
                    {report.status.split("_").join(" ").toLowerCase()}
                  </Cell>
                </Row>

                {/* Report Year Marker */}
                {renderReportYearRow(index, report.year)}
              </Fragment>
            )
          )
        ) : (
          <NoReportsDisplay>No reports to display.</NoReportsDisplay>
        )}
      </Table>
    </>
  );
};

export default observer(Reports);
