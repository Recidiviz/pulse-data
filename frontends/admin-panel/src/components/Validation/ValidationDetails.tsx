// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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

import { InfoCircleOutlined } from "@ant-design/icons";
import {
  Badge,
  Card,
  Checkbox,
  Descriptions,
  Divider,
  message,
  Segmented,
  Spin,
  Tooltip,
} from "antd";
import * as React from "react";
import { Link } from "react-router-dom";
import {
  fetchValidationDescription,
  fetchValidationDetails,
  fetchValidationErrorTable,
  getAllBQRefreshTimestamps,
} from "../../AdminPanelAPI";
import { getRecentIngestInstanceStatusHistory } from "../../AdminPanelAPI/IngestOperations";
import { useFetchedDataJSON, useFetchedDataProtobuf } from "../../hooks";
import {
  ValidationStatusRecord,
  ValidationStatusRecords,
} from "../../recidiviz/admin_panel/models/validation_pb";
import {
  IngestInstanceStatusInfo,
  IngestStatusRefreshInfo,
} from "../IngestOperationsView/constants";
import { gcpEnvironment } from "../Utilities/EnvironmentUtilities";
import {
  RecordStatus,
  ValidationDetailsProps,
  ValidationErrorTableData,
} from "./constants";
import SamenessPerRowDetails from "./SamenessPerRowDetails";
import SamenessPerViewDetails from "./SamenessPerViewDetails";
import {
  convertResultStatus,
  formatDatetime,
  formatStatusAmount,
  getBadgeStatusForRecordStatus,
  getDaysActive,
  getRecordStatus,
  getTextForRecordStatus,
  replaceInfinity,
} from "./utils";
import ValidationDetailsGraph from "./ValidationDetailsGraph";
import ValidationErrorTable from "./ValidationErrorTable";

const getValidationLogLink = (
  validationName: string,
  stateCode: string,
  traceId?: string
): string | undefined => {
  if (!traceId) {
    // Needs to have traceId to locate correct log
    return;
  }

  const queryEnv = gcpEnvironment.isProduction
    ? "recidiviz-123"
    : "recidiviz-staging";

  // Time range is set to 5 days which should be fine since ValidationDetails is focusing on the most recent runs.
  return `https://console.cloud.google.com/logs/query;query=logName%3D%22projects%2F${queryEnv}%2Flogs%2Fapp%22%0Atrace%3D%22projects%2F${queryEnv}%2Ftraces%2F${traceId}%22%0A%22${validationName}%22%0A%22${stateCode}%22;timeRange=P5D?project=${queryEnv}`;
};

/** Determines which ingest statuses were active during each BigQuery refresh. See
 * `IngestStatusRefreshInfo` for details on the relationship between a refresh and
 * ingest instance statuses. */
const getIngestEvents = (
  refreshTimestamps: string[] | undefined,
  ingestInstanceStatuses: IngestInstanceStatusInfo[] | undefined
): IngestStatusRefreshInfo[] => {
  if (!refreshTimestamps || !ingestInstanceStatuses) {
    return [];
  }

  let statusIdx = 0;
  const results = [];
  for (
    let refreshTimestampIdx = 0;
    refreshTimestampIdx < refreshTimestamps.length;
    refreshTimestampIdx += 1
  ) {
    // If the status happened after the refresh, move to the next status. This will only
    // happen for the first (most recent) refresh.
    while (
      statusIdx < ingestInstanceStatuses.length &&
      ingestInstanceStatuses[statusIdx].statusTimestamp >
        refreshTimestamps[refreshTimestampIdx]
    ) {
      statusIdx += 1;
    }

    const statusStartIdx = statusIdx;
    if (refreshTimestampIdx + 1 === refreshTimestamps.length) {
      // If this is the most recent refresh, include the rest of the statuses.
      statusIdx = ingestInstanceStatuses.length;
    } else {
      // Otherwise, just get the statuses until we find one that is before the prior
      // refresh.
      while (
        statusIdx < ingestInstanceStatuses.length &&
        ingestInstanceStatuses[statusIdx].statusTimestamp >
          refreshTimestamps[refreshTimestampIdx + 1]
      ) {
        statusIdx += 1;
      }
    }

    results.push({
      refreshTimestamp: refreshTimestamps[refreshTimestampIdx],
      ingestStatuses: ingestInstanceStatuses.slice(
        statusStartIdx,
        statusIdx + 1 // Keep the next one, as it is still active during this refresh.
      ),
    });
  }
  return results;
};

const getAllVersionChanges = (
  records: ValidationStatusRecord[]
): { [systemVersion: string]: string } => {
  return records.reduce((acc, record) => {
    const systemVersion = record.getSystemVersion() || "";
    const runDatetime = record.getRunDatetime().toDate();
    return {
      ...acc,
      [systemVersion]:
        systemVersion in acc && acc[systemVersion] < runDatetime
          ? acc[systemVersion]
          : runDatetime,
    };
  }, {} as { [systemVersion: string]: string });
};

const ValidationDetails: React.FC<ValidationDetailsProps> = ({
  validationName,
  stateCode,
}) => {
  const [validationDescription, setValidationDescription] =
    React.useState<string | undefined>(undefined);

  const fetchErrorTable = React.useCallback(() => {
    return fetchValidationErrorTable(validationName, stateCode);
  }, [validationName, stateCode]);

  const { loading: errorTableLoading, data: errorTable } =
    useFetchedDataJSON<ValidationErrorTableData>(fetchErrorTable);

  const [lookbackDays, setLookbackDays] = React.useState(30);

  const fetchResults = React.useCallback(async () => {
    // TODO(#19328): Why are we doing these together? I think it means we wait to fetch
    // results until after we have the description.
    const r = await fetchValidationDescription(validationName);
    const text = await r.text();
    if (r.status >= 400) {
      message.error(`Fetching validation description... failed: ${text}`);
    } else {
      setValidationDescription(text);
    }

    return fetchValidationDetails(validationName, stateCode, lookbackDays);
  }, [validationName, stateCode, lookbackDays]);

  const validationFetched = useFetchedDataProtobuf<ValidationStatusRecords>(
    fetchResults,
    ValidationStatusRecords.deserializeBinary
  );

  const records = validationFetched.data?.getRecordsList() || [];
  const latestRecord = records && records[0];

  // `validationLoading` is true whenever we are reloading the records, including when
  // the lookback was changed. `validationInitialLoading` will only be true when we
  // first load the page for this state and validation, not when the lookback is
  // changed. Components of the page that only use the latest record only need to
  // display a loading state when `validationInitialLoading` is true, as that won't
  // change based on the lookback.
  const validationLoading = validationFetched.loading;
  const validationInitialLoading = validationLoading && !records.length;

  const latestResultStatus =
    (latestRecord && getRecordStatus(latestRecord)) || RecordStatus.UNKNOWN;
  const validationLogLink =
    latestRecord &&
    getValidationLogLink(validationName, stateCode, latestRecord.getTraceId());

  // Get the ingest status changes and group them by which refresh they were included in
  const [includeIngestEvents, setIncludeIngestEvents] = React.useState(true);

  const fetchIngestInstanceStatuses = React.useCallback(() => {
    return getRecentIngestInstanceStatusHistory(stateCode);
  }, [stateCode]);

  const {
    loading: ingestInstanceStatusesLoading,
    data: ingestInstanceStatuses,
  } = useFetchedDataJSON<IngestInstanceStatusInfo[]>(
    fetchIngestInstanceStatuses
  );

  const fetchRefreshTimestamps = React.useCallback(() => {
    return getAllBQRefreshTimestamps(stateCode);
  }, [stateCode]);
  const { loading: refreshTimestampsLoading, data: refreshTimestamps } =
    useFetchedDataJSON<string[]>(fetchRefreshTimestamps);

  // Do the actual grouping by refresh (both lists are in descending order)
  const ingestEventsLoading =
    ingestInstanceStatusesLoading || refreshTimestampsLoading;
  const ingestEvents = includeIngestEvents
    ? getIngestEvents(refreshTimestamps, ingestInstanceStatuses).filter(
        // Only include
        ({ refreshTimestamp }) => {
          const refreshDate = new Date(refreshTimestamp);
          return (
            records.length > 0 &&
            refreshDate >= records.slice(-1)[0].getRunDatetime().toDate() &&
            refreshDate < latestRecord.getRunDatetime().toDate()
          );
        }
      )
    : [];

  // Note, this will still show a version change even if the view update failed and so
  // not all the changes from that version had taken effect.
  const [includeVersionChanges, setIncludeVersionChanges] =
    React.useState(false);
  const versionChanges = includeVersionChanges
    ? getAllVersionChanges(records)
    : {};

  return (
    <>
      <Divider orientation="left">Recent Run Details</Divider>
      {/* TODO(#9480): could let the user pick a run but just default to the most recent */}
      <Card title="Summary">
        {validationInitialLoading ? (
          <Spin />
        ) : (
          <Descriptions bordered>
            <Descriptions.Item label="Run ID">
              {latestRecord?.getRunId()}
            </Descriptions.Item>
            <Descriptions.Item label="Run Datetime">
              {formatDatetime(latestRecord?.getRunDatetime()?.toDate())}
            </Descriptions.Item>
            <Descriptions.Item label="System Version">
              {latestRecord?.getSystemVersion()}
            </Descriptions.Item>
            <Descriptions.Item label="Status" span={3}>
              <Badge
                status={getBadgeStatusForRecordStatus(latestResultStatus)}
                text={getTextForRecordStatus(latestResultStatus)}
              />
            </Descriptions.Item>
            <Descriptions.Item label="Error Amount">
              {formatStatusAmount(
                latestRecord?.getErrorAmount(),
                latestRecord?.getIsPercentage()
              )}
            </Descriptions.Item>
            <Descriptions.Item label="Soft Threshold">
              {formatStatusAmount(
                latestRecord?.getSoftFailureAmount(),
                latestRecord?.getIsPercentage()
              )}
            </Descriptions.Item>
            <Descriptions.Item label="Hard Threshold">
              {formatStatusAmount(
                latestRecord?.getHardFailureAmount(),
                latestRecord?.getIsPercentage()
              )}
            </Descriptions.Item>
            <Descriptions.Item label="Message" span={3}>
              {latestRecord?.getFailureDescription() || "None"}
            </Descriptions.Item>
            <Descriptions.Item label="Description" span={3}>
              {validationDescription}
            </Descriptions.Item>
            {validationLogLink && (
              <Descriptions.Item label="Log" span={3}>
                <Link to={{ pathname: validationLogLink }} target="_blank">
                  Log Explorer
                </Link>
              </Descriptions.Item>
            )}
            {latestRecord?.getErrorLog() && (
              <Descriptions.Item label="Error Details" span={3}>
                {latestRecord.getErrorLog()}
              </Descriptions.Item>
            )}
          </Descriptions>
        )}
      </Card>
      <br />
      <div
        className="site-card-wrapper"
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-evenly",
        }}
      >
        {validationInitialLoading && <Spin />}
        {latestRecord?.getResultDetailsCase() ===
          ValidationStatusRecord.ResultDetailsCase.SAMENESS_PER_VIEW && (
          <SamenessPerViewDetails
            samenessPerView={latestRecord?.getSamenessPerView()}
          />
        )}
        {latestRecord?.getResultDetailsCase() ===
          ValidationStatusRecord.ResultDetailsCase.SAMENESS_PER_ROW && (
          <SamenessPerRowDetails
            samenessPerRow={latestRecord?.getSamenessPerRow()}
          />
        )}
      </div>
      <Divider orientation="left">History</Divider>
      <div
        className="site-card-wrapper"
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-evenly",
        }}
      >
        <Card
          title="Graph"
          extra={
            <Segmented
              options={[
                { label: "1 wk", value: 7 },
                { label: "2 wk", value: 14 },
                { label: "1 mo", value: 30 },
                { label: "3 mo", value: 90 },
                { label: "6 mo", value: 180 },
              ]}
              defaultValue={lookbackDays}
              onChange={(value) => setLookbackDays(value as number)}
            />
          }
          actions={[
            <>
              {includeIngestEvents && ingestEventsLoading && <Spin />}
              <Checkbox
                defaultChecked={includeIngestEvents}
                indeterminate={includeIngestEvents && ingestEventsLoading}
                onChange={(event) =>
                  setIncludeIngestEvents(event.target.checked)
                }
              >
                Show Ingest Events
              </Checkbox>
            </>,
            <>
              {includeVersionChanges && validationLoading && <Spin />}
              <Checkbox
                defaultChecked={includeVersionChanges}
                indeterminate={includeVersionChanges && validationLoading}
                onChange={(event) =>
                  setIncludeVersionChanges(event.target.checked)
                }
              >
                Show Version Changes
              </Checkbox>
            </>,
          ]}
        >
          <ValidationDetailsGraph
            // Use slice() to copy before reversing.
            records={records.slice().reverse()}
            isPercent={latestRecord?.getIsPercentage()}
            loading={validationLoading}
            ingestEvents={ingestEvents}
            versionChanges={versionChanges}
          />
        </Card>

        {validationInitialLoading ? (
          <Spin />
        ) : (
          <>
            {
              // Only show the "Last Better Run" card if it is not this run.
              latestRecord?.getLastBetterStatusRunId() !==
                latestRecord?.getRunId() && (
                <Card
                  title={`Last ${getTextForRecordStatus(
                    convertResultStatus(
                      latestRecord?.getLastBetterStatusRunResultStatus()
                    )
                  )} Run`}
                  extra={
                    <Tooltip title="Information about the most recent run that had a better status.">
                      <InfoCircleOutlined />
                    </Tooltip>
                  }
                >
                  {!latestRecord?.hasLastBetterStatusRunDatetime() ? (
                    "No prior better run exists."
                  ) : (
                    <Descriptions bordered column={1}>
                      <Descriptions.Item label="Days Ago">
                        {latestRecord === undefined
                          ? "Unknown"
                          : replaceInfinity(
                              getDaysActive(latestRecord),
                              "Always"
                            )}
                      </Descriptions.Item>
                      <Descriptions.Item label="Run ID">
                        {latestRecord?.getLastBetterStatusRunId()}
                      </Descriptions.Item>
                      <Descriptions.Item label="Run Datetime">
                        {formatDatetime(
                          latestRecord
                            ?.getLastBetterStatusRunDatetime()
                            ?.toDate()
                        )}
                      </Descriptions.Item>
                      <Descriptions.Item label="Status">
                        <Badge
                          status={getBadgeStatusForRecordStatus(
                            convertResultStatus(
                              latestRecord?.getLastBetterStatusRunResultStatus()
                            )
                          )}
                          text={getTextForRecordStatus(
                            convertResultStatus(
                              latestRecord?.getLastBetterStatusRunResultStatus()
                            )
                          )}
                        />
                      </Descriptions.Item>
                    </Descriptions>
                  )}
                </Card>
              )
            }
          </>
        )}
      </div>
      <Divider orientation="left">Failing Rows</Divider>
      <div className="site-card-wrapper">
        {errorTableLoading && (
          <Spin
            style={{
              marginLeft: "49.5%",
            }}
          />
        )}
        {errorTable && !errorTableLoading && (
          <>
            <ValidationErrorTable tableData={errorTable} />
          </>
        )}
        {!errorTable && !errorTableLoading && (
          <span>Unable to load any rows.</span>
        )}
      </div>
    </>
  );
};

export default ValidationDetails;
