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

export enum Permission {
  RECIDIVIZ_ADMIN = "recidiviz_admin",
  SWITCH_AGENCIES = "switch_agencies",
}

export type AgencySystems =
  | "LAW_ENFORCEMENT"
  | "PROSECUTION"
  | "DEFENSE"
  | "COURTS_AND_PRETRIAL"
  | "JAILS"
  | "PRISONS"
  | "SUPERVISION"
  | "PAROLE"
  | "PROBATION"
  | "POST_RELEASE";

export interface UserAgency {
  name: string;
  id: number;
  fips_county_code: string;
  state_code: string;
  system: AgencySystems;
  systems: AgencySystems[];
}

export type ReportFrequency = "MONTHLY" | "ANNUAL";

export type ReportStatus = "NOT_STARTED" | "DRAFT" | "PUBLISHED";

export interface ReportOverview {
  id: number;
  agency_id: number;
  month: number;
  year: number;
  frequency: ReportFrequency;
  last_modified_at: string | null;
  // TODO(#14138): Backend should only send timestamps
  last_modified_at_timestamp: number | null;
  editors: string[];
  status: ReportStatus;
}

export interface Report extends ReportOverview {
  metrics: Metric[];
}

export type MetricWithErrors = Metric & {
  error?: string;
  contexts: MetricContextWithErrors[];
  disaggregations: MetricDisaggregationsWithErrors[];
};

export type MetricContextWithErrors = MetricContext & {
  error?: string;
};

export type MetricDisaggregationsWithErrors = MetricDisaggregations & {
  dimensions: MetricDisaggregationDimensionsWithErrors[];
};

export type MetricDisaggregationDimensionsWithErrors =
  MetricDisaggregationDimensions & {
    error?: string;
  };

export interface Metric {
  key: string;
  system: AgencySystems;
  display_name: string;
  description: string;
  reporting_note: string;
  value: string | number | boolean | null | undefined;
  unit: string;
  category: string;
  label: string;
  definitions: MetricDefinition[];
  contexts: MetricContext[];
  disaggregations: MetricDisaggregations[];
  enabled?: boolean;
}

export interface MetricDefinition {
  term: string;
  definition: string;
}

export interface MetricContext {
  key: string;
  display_name: string | null | undefined;
  reporting_note: string | null | undefined;
  required: boolean;
  type: "TEXT" | "NUMBER" | "MULTIPLE_CHOICE";
  value: string | number | boolean | null | undefined;
  multiple_choice_options: string[];
}

export interface MetricDisaggregations {
  key: string;
  display_name: string;
  dimensions: MetricDisaggregationDimensions[];
  required: boolean;
  helper_text: string | null | undefined;
  enabled?: boolean;
}

export interface MetricDisaggregationDimensions {
  key: string;
  label: string;
  value: string | number | boolean | null | undefined;
  reporting_note: string;
  enabled?: boolean;
}

export interface CreateReportFormValuesType extends Record<string, unknown> {
  month: 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12;
  year: number;
  frequency: ReportFrequency;
  annualStartMonth: number;
  isRecurring: boolean;
}

export interface FormError {
  message: string;
  info?: string;
}

export interface FormContexts {
  [contextKey: string]: { value?: string; error?: FormError };
}

export interface FormDimensions {
  [dimensionKey: string]: { value?: string; error?: FormError };
}

export interface FormDisaggregations {
  [disaggregationKey: string]: FormDimensions;
}

export interface FormMetric {
  value: string | number;
  contexts: FormContexts;
  disaggregations: FormDisaggregations;
}

export interface FormReport {
  [metricKey: string]: FormMetric;
}

export interface FormStoreMetricValue {
  [metricKey: string]: { value?: string; error?: FormError };
}
export interface FormStoreMetricValues {
  [reportID: string]: FormStoreMetricValue;
}

export interface FormStoreContextValue {
  [metricKey: string]: FormContexts;
}
export interface FormStoreContextValues {
  [reportID: string]: FormStoreContextValue;
}

export interface FormStoreDisaggregationValue {
  [metricKey: string]: FormDisaggregations;
}
export interface FormStoreDisaggregationValues {
  [reportID: string]: FormStoreDisaggregationValue;
}

export interface UpdatedMetricsValues {
  key: string;
  value: Metric["value"];
  contexts: { key: string; value: MetricContext["value"] }[];
  disaggregations: {
    key: string;
    dimensions: {
      key: string;
      value: MetricDisaggregationDimensions["value"];
    }[];
  }[];
}

/**
 * Reports data that comes in from the server.
 * This closely resembles how report data is stored in our backend.
 */
export interface RawDatapoint {
  id: number;
  report_id: number | null;
  start_date: string;
  end_date: string;
  metric_definition_key: string;
  metric_display_name: string | null;
  disaggregation_display_name: string | null;
  dimension_display_name: string | null;
  value: string;
  is_published: boolean;
  frequency: ReportFrequency;
}

/**
 * A Datapoint is an object representing a piece of justice counts metrics data for rendering in Recharts.
 * Currently we only render Stacked Bar Charts.
 * Each Datapoint represents a bar on the bar chart.
 * Each Datapoint has:
 * • a unique start_date and end_date, which serve as the x-axis category,
 * • the frequency of the reporting data, either monthly or annual
 * • "dataVizMissingData" which is used to render the missing data bar if there are no metrics for the time range represented
 * • remaning keys which store the name of a piece of the stacked bar chart and its value.
 *
 * For example, raw datapoints that look like {start_date: "1/2020", disaggregation: "Gender", dimension: "Male", value: 5},
 * {start_date: "1/2020", disaggregation: "Gender", dimension: "Female", value: 3}, would be combined into
 * {start_date: "1/2020", "Male": 5, "Female": 3}
 * and keyed by "Gender".
 */
export interface Datapoint {
  start_date: string;
  end_date: string;
  frequency: ReportFrequency;
  // dataVizMissingData is used to render the missing data bar if there are no values reported for that time range
  dataVizMissingData: number;
  // the value here should really be number | null but Typescript doesn't allow for this easily
  [dimensionOrAggregatedTotal: string]: string | number | null;
}

export interface DatapointsGroupedByAggregateAndDisaggregations {
  aggregate: Datapoint[];
  disaggregations: {
    [disaggregation: string]: {
      [start_date: string]: Datapoint;
    };
  };
}

export interface DatapointsByMetric {
  [metricKey: string]: DatapointsGroupedByAggregateAndDisaggregations;
}

export type DataVizTimeRange = 0 | 6 | 12 | 60 | 120;

export const DataVizTimeRangesMap: { [key: string]: DataVizTimeRange } = {
  All: 0,
  "6 Months Ago": 6,
  "1 Year Ago": 12,
  "5 Years Ago": 60,
  "10 Years Ago": 120,
};

export type DatapointsViewSetting = "Count" | "Percentage";

export interface DimensionNamesByMetricAndDisaggregation {
  [metric: string]: {
    [disaggregation: string]: string[];
  };
}

export const DataVizAggregateName = "Total";
