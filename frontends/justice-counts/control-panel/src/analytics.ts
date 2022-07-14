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

import { UpdatedMetricsValues, UserAgency } from "./shared/types";

const TEST_SENDING_ANALYTICS = false; // used for testing sending analytics in development
const LOG_ANALYTICS = false; // used for logging analytics being sent

export const identify = (
  userId: string,
  metadata?: Record<string, unknown>
): void => {
  const fullMetadata = metadata || {};
  if (LOG_ANALYTICS) {
    // eslint-disable-next-line
    console.log(
      `[Analytics] Identifying user id: ${userId}, with metadata: ${JSON.stringify(
        fullMetadata
      )}`
    );
  }
  if (
    (process.env.NODE_ENV !== "development" &&
      process.env.NODE_ENV !== "test") ||
    TEST_SENDING_ANALYTICS
  ) {
    window.analytics.identify(userId, fullMetadata);
  }
};

const track = (eventName: string, metadata?: Record<string, unknown>): void => {
  const fullMetadata = metadata || {};
  if (LOG_ANALYTICS) {
    // eslint-disable-next-line
    console.log(
      `[Analytics] Tracking event name: ${eventName}, with metadata: ${JSON.stringify(
        fullMetadata
      )}`
    );
  }
  if (
    (process.env.NODE_ENV !== "development" &&
      process.env.NODE_ENV !== "test") ||
    TEST_SENDING_ANALYTICS
  ) {
    window.analytics.track(eventName, fullMetadata);
  }
};

export const trackReportCreated = (
  reportId: number,
  agency?: UserAgency
): void => {
  track("frontend_report_created", {
    reportId,
    agencyId: agency?.id,
    agencyName: agency?.name,
    agencyFips: agency?.fips_county_code,
    agencyState: agency?.state_code,
    agencySystem: agency?.system,
    agencySystems: agency?.systems,
  });
};

export const trackReportNotStartedToDraft = (
  reportId: number,
  agency?: UserAgency
): void => {
  track("frontend_report_not_started_to_draft", {
    reportId,
    agencyId: agency?.id,
    agencyName: agency?.name,
    agencyFips: agency?.fips_county_code,
    agencyState: agency?.state_code,
    agencySystem: agency?.system,
    agencySystems: agency?.systems,
  });
};

export const trackReportPublished = (
  reportId: number,
  metrics: UpdatedMetricsValues[],
  agency?: UserAgency
): void => {
  const metricsReported = metrics.reduce((res: string[], metric) => {
    if (metric.value !== null) {
      res.push(metric.key);
    }
    return res;
  }, []);
  const metricsReportedCount = metricsReported.length;
  const metricsReportedWithContext = metrics.reduce((res: string[], metric) => {
    if (
      metric.contexts.find(
        (context) => context.value !== null && context.value !== undefined
      )
    ) {
      res.push(metric.key);
    }
    return res;
  }, []);
  const metricsReportedWithContextCount = metricsReportedWithContext.length;
  const metricsReportedWithDisaggregations = metrics.reduce(
    (res: string[], metric) => {
      if (
        metric.disaggregations.find((disaggregation) =>
          disaggregation.dimensions.find(
            (dimension) =>
              dimension.value !== undefined && dimension.value !== null
          )
        )
      ) {
        res.push(metric.key);
      }
      return res;
    },
    []
  );
  const metricsReportedWithDisaggregationsCount =
    metricsReportedWithDisaggregations.length;
  const totalMetricsCount = metrics.length;
  track("frontend_report_published", {
    reportId,
    metricsReportedCount,
    metricsReportedWithContextCount,
    metricsReportedWithDisaggregationsCount,
    totalMetricsCount,
    agencyId: agency?.id,
    agencyName: agency?.name,
    agencyFips: agency?.fips_county_code,
    agencyState: agency?.state_code,
    agencySystem: agency?.system,
    agencySystems: agency?.systems,
  });
};

export const trackReportUnpublished = (
  reportId: number,
  agency?: UserAgency
): void => {
  track("frontend_report_unpublished", {
    reportId,
    agencyId: agency?.id,
    agencyName: agency?.name,
    agencyFips: agency?.fips_county_code,
    agencyState: agency?.state_code,
    agencySystem: agency?.system,
    agencySystems: agency?.systems,
  });
};

export const trackNetworkError = (
  path: string,
  method: string,
  status: number,
  errorMsg: string
): void => {
  track("frontend_network_error", {
    path,
    method,
    status,
    errorMsg,
  });
};

export const trackAutosaveTriggered = (reportId: number): void => {
  track("frontend_report_autosave_triggered", {
    reportId,
  });
};

export const trackAutosaveFailed = (reportId: number): void => {
  track("frontend_report_autosave_failed", {
    reportId,
  });
};

export const trackNavigation = (screen: string): void => {
  track("frontend_navigate", {
    screen,
  });
};

export const trackLoadTime = (
  path: string,
  method: string,
  loadTime: number
): void => {
  track("load_time", {
    path,
    method,
    loadTime,
  });
};
