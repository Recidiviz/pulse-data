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

import React from "react";

import { removeSnakeCase } from "../../utils";
import { ReactComponent as SpreadsheetIcon } from "../assets/microsoft-excel-icon.svg";
import {
  ButtonWrapper,
  DownloadTemplateBox,
  systemToTemplateSpreadsheetFileName,
} from ".";

export type GeneralInstructionsTemplateParams = {
  systems: string[];
};

export type SystemsInstructionsTemplateParams = {
  system: string;
};

type SystemDetails = {
  name: string;
  url: string;
  metric: string;
  metric_sheet_name: string;
  metric_disaggregation_sheet_name: string;
  metric_category: string;
  metric_disaggregation: string;
  metric_disaggregation_column_name: string;
  metric_disaggregation_values: { [name: string]: number | string }[];
};

export const systemToDetails: { [system: string]: SystemDetails } = {
  LAW_ENFORCEMENT: {
    name: "Law Enforcement",
    url: "https://justicecounts.csgjusticecenter.org/metrics/sectors/law-enforcement/",
    metric: "Reported Crime",
    metric_sheet_name: "reported_crime",
    metric_disaggregation_sheet_name: "reported_crime_by_type",
    metric_category: "Population Movements",
    metric_disaggregation: "offense type",
    metric_disaggregation_column_name: "offense_type",
    metric_disaggregation_values: [
      { name: "Person", value: 3291 },
      { name: "Property", value: 1293 },
      { name: "Drug", value: 504 },
      { name: "Other", value: 34 },
      { name: "Unknown", value: 3 },
    ],
  },
  DEFENSE: {
    name: "Defense",
    url: "https://justicecounts.csgjusticecenter.org/metrics/sectors/defense/",
    metric: "Cases Appointed Counsel",
    metric_sheet_name: "cases_appointed",
    metric_disaggregation_sheet_name: "cases_appointed_by_severity",
    metric_category: "Population Movements",
    metric_disaggregation: "case severity",
    metric_disaggregation_column_name: "case_severity",
    metric_disaggregation_values: [
      { name: "Felony", value: 3291 },
      { name: "Misdemeanor", value: 1293 },
      { name: "Infraction", value: 504 },
      { name: "Unknown", value: 3 },
    ],
  },
  PROSECUTION: {
    name: "Prosecution",
    url: "https://justicecounts.csgjusticecenter.org/metrics/sectors/prosecution/",
    metric: "Cases Declined",
    metric_sheet_name: "cases_declined",
    metric_disaggregation_sheet_name: "cases_declined_by_severity",
    metric_category: "Operations and Dynamics",
    metric_disaggregation: "case severity",
    metric_disaggregation_column_name: "case_severity",
    metric_disaggregation_values: [
      { name: "Felony", value: 3291 },
      { name: "Misdemeanor", value: 1293 },
      { name: "Infraction", value: 504 },
      { name: "Unknown", value: 3 },
    ],
  },
  COURTS_AND_PRETRIAL: {
    name: "Courts and Pretrial",
    url: "https://justicecounts.csgjusticecenter.org/metrics/sectors/courts-pretrial/",
    metric: "Pretrial Releases",
    metric_sheet_name: "pretrial_releases",
    metric_disaggregation_sheet_name: "pretrial_releases_by_type",
    metric_category: "Operations and Dynamics",
    metric_disaggregation: "release type",
    metric_disaggregation_column_name: "release_type",
    metric_disaggregation_values: [
      { name: "ROR", value: 3291 },
      { name: "Monetary bail", value: 1293 },
      { name: "Supervision", value: 504 },
      { name: "Other", value: 34 },
      { name: "Unknown", value: 3 },
    ],
  },
  JAILS: {
    name: "Jails",
    url: "https://justicecounts.csgjusticecenter.org/metrics/sectors/jails/",
    metric: "Admissions",
    metric_sheet_name: "admissions",
    metric_disaggregation_sheet_name: "admissions_by_type",
    metric_category: "Population Movements",
    metric_disaggregation: "admission type",
    metric_disaggregation_column_name: "admission_type",
    metric_disaggregation_values: [
      { name: "Pretrial", value: 3291 },
      { name: "Sentenced", value: 1293 },
      { name: "Transfer or Hold", value: 504 },
      { name: "Unknown", value: 3 },
    ],
  },
  PRISONS: {
    name: "Prisons",
    url: "https://justicecounts.csgjusticecenter.org/metrics/sectors/prisons/",
    metric: "Admissions",
    metric_sheet_name: "admissions",
    metric_disaggregation_sheet_name: "admissions_by_type",
    metric_category: "Population Movements",
    metric_disaggregation: "admission type",
    metric_disaggregation_column_name: "admission_type",
    metric_disaggregation_values: [
      { name: "New Sentence", value: 3291 },
      { name: "Transfer or Hold", value: 1293 },
      { name: "Supervision Violation or Revocation", value: 504 },
      { name: "Other", value: 34 },
      { name: "Unknown", value: 3 },
    ],
  },
  SUPERVISION: {
    name: "Supervision",
    url: "https://justicecounts.csgjusticecenter.org/metrics/sectors/community-supervision/",
    metric: "New Supervision Cases",
    metric_sheet_name: "new_cases",
    metric_disaggregation_sheet_name: "new_cases_by_type",
    metric_category: "Population Movements",
    metric_disaggregation: "supervision type",
    metric_disaggregation_column_name: "supervision_type",
    metric_disaggregation_values: [
      { name: "Active", value: 3291 },
      { name: "Passive", value: 1293 },
      { name: "Unknown", value: 504 },
    ],
  },
};

export const GeneralInstructions: React.FC<
  GeneralInstructionsTemplateParams
> = ({ systems }) => {
  return (
    <>
      <h1>How to Upload Data to Justice Counts</h1>
      <p>
        Agencies participating in Justice Counts have two options for reporting
        their assigned metrics:
      </p>
      <ol>
        <li>
          Filling out the autogenerated reports on the{" "}
          <a href="/" target="_blank" rel="noreferrer noopener">
            Reports
          </a>{" "}
          page
        </li>
        <li>Uploading an Excel spreadsheet</li>
      </ol>
      <p>
        If you choose the second option, we require that you upload the
        spreadsheet <b>in a particular format</b>, so we can build automation on
        our end to easily upload this data into our platform.
      </p>

      <h3>Templates</h3>

      <ButtonWrapper>
        {systems.map((system) => {
          const systemName = removeSnakeCase(system).toLowerCase();
          const systemFileName = systemToTemplateSpreadsheetFileName[system];

          return (
            <DownloadTemplateBox key={system}>
              <SpreadsheetIcon />

              <span>
                {systemName}
                <a
                  href={`./assets/${systemFileName}`}
                  download={systemFileName}
                >
                  Download
                </a>
              </span>
            </DownloadTemplateBox>
          );
        })}
      </ButtonWrapper>

      <p>
        Complete, downloadable spreadsheet templates can be found above. We
        suggest that you download the template and review it, and then read the
        description below for more detail.
      </p>
      <h2>High-Level Summary</h2>
      <p>
        In this section, we provide instructions applicable to all agencies. In
        the next section, we provide instructions specific to your agency.
      </p>
      <h3>Basics</h3>
      <p>
        You will need to generate one Excel workbook for each criminal justice
        system that your agency is reporting data for. Your agency is
        responsible for reporting data for:{" "}
        {/* replace last comma with "and": https://stackoverflow.com/a/41035407 */}
        {systems
          .map((system) => systemToDetails[system].name)
          .join(", ")
          .replace(/,(?!.*,)/gim, " and")}
        .
      </p>
      <p>
        Include one sheet (or tab) in the workbook for each Justice Counts
        metric defined for the system. See the{" "}
        <a
          href="https://justicecounts.csgjusticecenter.org/metrics/justice-counts-metrics/"
          target="_blank"
          rel="noreferrer noopener"
        >
          Justice Counts website
        </a>{" "}
        for a list and description of these metrics.
      </p>
      <p>
        We require that each sheet is given a standard name.{" "}
        <b>Refer to your template file for the valid sheet names.</b>
      </p>
      <p>
        All sheets will have columns for <i>year</i> and <i>value</i>. Monthly
        metrics will also have a column for <i>month</i>.
      </p>
      <p>
        To report data for a metric for a particular time period, add a new row
        to the sheet. Fill in the appropriate values for the <i>year</i> and{" "}
        <i>month</i> columns, and report the metric value in the <i>value</i>{" "}
        column.
      </p>
      <p>
        The year should be in the format 20XX. The month should either be a
        number [1 … 12] or the full month name [January … February]. The metric
        value should be numeric and contain no other characters (commas are
        allowed, e.g. 1,000).
      </p>

      <p>The sheets will look roughly like this:</p>
      <table>
        <thead>
          <tr>
            <th>year</th>
            <th>month</th>
            <th>value</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>2021</td>
            <td>7</td>
            <td>5088</td>
          </tr>

          <tr>
            <td>2021</td>
            <td>8</td>
            <td>5270</td>
          </tr>

          <tr>
            <td>2021</td>
            <td>9</td>
            <td>5113</td>
          </tr>

          <tr>
            <td>2021</td>
            <td>10</td>
            <td>5196</td>
          </tr>

          <tr>
            <td>2021</td>
            <td>11</td>
            <td>5237</td>
          </tr>

          <tr>
            <td>2021</td>
            <td>12</td>
            <td>5123</td>
          </tr>
        </tbody>
      </table>
      <h3>Disaggregations</h3>
      <p>
        If the metric asks you to break the value down into different categories
        (e.g. separating out crimes by offense type), add an additional sheet to
        the workbook that includes a column with the name of the category (e.g.{" "}
        <i>offense_type</i>). Each row should specify a different value for that
        category (e.g. <i>person</i>, <i>property</i>, <i>drug</i>).
      </p>
      <p>
        We require that each sheet and column is given a standard name.{" "}
        <b>
          Refer to your template file for the name of the new sheet, the name of
          the new column for category names, and the valid values for this
          column.
        </b>
      </p>
      <p>
        <b>
          Please only provide valid category names (as seen in your template
          file) in the new column.{" "}
        </b>
        If your agency categorizes the metric differently, group any unmatched
        data into the <i>Other</i> category.
      </p>

      <p>
        For instance, a sheet for <i>reported_crimes_by_type</i> might look like
        this:
      </p>
      <table>
        <thead>
          <tr>
            <th>year</th>
            <th>month</th>
            <th>value</th>
            <th>offense_type</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>2021</td>
            <td>7</td>
            <td>Person</td>
            <td>3291</td>
          </tr>

          <tr>
            <td>2021</td>
            <td>7</td>
            <td>Property</td>
            <td>1293</td>
          </tr>

          <tr>
            <td>2021</td>
            <td>7</td>
            <td>Drug</td>
            <td>504</td>
          </tr>

          <tr>
            <td>2021</td>
            <td>7</td>
            <td>Other</td>
            <td>372</td>
          </tr>

          <tr>
            <td>2021</td>
            <td>7</td>
            <td>Unknown</td>
            <td>53</td>
          </tr>
        </tbody>
      </table>
      <p>
        Fill in as many categories as you can. Skip any that are not applicable
        to your agency.
      </p>
      <h3>Multiple Jurisdictions</h3>
      <p>
        If you are reporting data for multiple jurisdictions, you should also
        add a column to each sheet titled <i>jurisdiction_name</i>. The value of
        this column should be the name of the jurisdiction to which that data
        point belongs. For instance:
      </p>
      <table>
        <thead>
          <tr>
            <th>year</th>
            <th>jurisdiction_name</th>
            <th>value</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>2021</td>
            <td>County X</td>
            <td>1000</td>
          </tr>

          <tr>
            <td>2021</td>
            <td>County Y</td>
            <td>2000</td>
          </tr>
        </tbody>
      </table>
    </>
  );
};

export const SystemsInstructions: React.FC<
  SystemsInstructionsTemplateParams
> = ({ system }) => {
  const systemDetails = systemToDetails[system];
  return (
    <>
      <p>
        {systemDetails.name} agencies are required to report the set of metrics
        described on the{" "}
        <a href={systemDetails.url} target="_blank" rel="noreferrer noopener">
          Justice Counts website
        </a>
        .
      </p>
      <p>
        For this example, consider the <i>{systemDetails.metric}</i> metric in
        the <i>{systemDetails.metric_category}</i> section.
      </p>
      <p>
        To report data for this metric, add two sheets to your Excel workbook:
        one called <i>{systemDetails.metric_sheet_name}</i> and one called{" "}
        <i>{systemDetails.metric_disaggregation_sheet_name}</i>.
      </p>
      <p>
        In the <i>{systemDetails.metric_sheet_name}</i> sheet, we&apos;ll report
        total values for each month:
      </p>
      <table>
        <thead>
          <tr>
            <th>year</th>
            <th>month</th>
            {system === "SUPERVISION" ? <th>system</th> : null}
            <th>value</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>2021</td>
            <td>7</td>
            {system === "SUPERVISION" ? <td>Parole</td> : null}
            <td>5088</td>
          </tr>
          <tr>
            <td>2021</td>
            <td>8</td>
            {system === "SUPERVISION" ? <td>Parole</td> : null}
            <td>5270</td>
          </tr>
          {system === "SUPERVISION" ? (
            <>
              <tr>
                <td>2021</td>
                <td>7</td>
                <td>Probation</td>
                <td>2093</td>
              </tr>
              <tr>
                <td>2021</td>
                <td>8</td>
                <td>Probation</td>
                <td>2392</td>
              </tr>
            </>
          ) : null}
        </tbody>
      </table>
      {system === "SUPERVISION" ? (
        <p>
          <b>Note:</b> If you are able to report separately for Parole and
          Probation, then you should fill out rows for both systems. If you have
          only aggregate data, leave the <i>system</i> column blank, or write
          &quot;Both&quot;.
        </p>
      ) : null}
      <p>
        In the <i>{systemDetails.metric_disaggregation_sheet_name}</i> sheet,
        break down values by {systemDetails.metric_disaggregation}:
      </p>
      <table>
        <thead>
          <tr>
            <th>year</th>
            <th>month</th>
            {system === "SUPERVISION" ? <th>system</th> : null}
            <th>{systemDetails.metric_disaggregation_column_name}</th>
            <th>value</th>
          </tr>
        </thead>
        <tbody>
          {systemDetails.metric_disaggregation_values.map((obj) => {
            return (
              <tr key={obj.name}>
                <td>2021</td>
                <td>7</td>
                {system === "SUPERVISION" ? <td>Parole</td> : null}
                <td>{obj.name}</td>
                <td>{obj.value}</td>
              </tr>
            );
          })}
          {system === "SUPERVISION"
            ? systemDetails.metric_disaggregation_values.map((obj) => {
                return (
                  <tr key={obj.name}>
                    <td>2021</td>
                    <td>7</td>
                    <td>Probation</td>
                    <td>{obj.name}</td>
                    <td>...</td>
                  </tr>
                );
              })
            : null}
        </tbody>
      </table>
      <p>
        Provide as many categories as you can, but feel free to skip the ones
        that you don&apos;t have.
      </p>
      <p>
        To add data for another month, just add more rows. Note that the order
        of the rows does not matter, though grouping rows from the same
        month/year and arranging them in ascending or descending order is
        preferable.
      </p>
    </>
  );
};
