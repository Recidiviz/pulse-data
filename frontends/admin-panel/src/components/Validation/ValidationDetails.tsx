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
  Descriptions,
  Divider,
  message,
  Spin,
  Tooltip,
} from "antd";
import React from "react";
import { Link } from "react-router-dom";
import {
  fetchValidationDetails,
  fetchValidationDescription,
  fetchValidationErrorTable,
} from "../../AdminPanelAPI";
import { useFetchedDataJSON, useFetchedDataProtobuf } from "../../hooks";
import {
  ValidationStatusRecord,
  ValidationStatusRecords,
} from "../../recidiviz/admin_panel/models/validation_pb";
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

const ValidationDetails: React.FC<ValidationDetailsProps> = ({
  validationName,
  stateInfo,
}) => {
  const [validationDescription, setValidationDescription] =
    React.useState<string | undefined>(undefined);

  const fetchErrorTable = React.useCallback(() => {
    return fetchValidationErrorTable(validationName, stateInfo.code);
  }, [validationName, stateInfo]);

  const { loading: errorTableLoading, data: errorTable } =
    useFetchedDataJSON<ValidationErrorTableData>(fetchErrorTable);

  const getValidationLogLink = (traceId?: string) => {
    if (!traceId) {
      // Needs to have traceId to locate correct log
      return;
    }

    const queryEnv = gcpEnvironment.isProduction
      ? "recidiviz-123"
      : "recidiviz-staging";

    // Time range is set to 5 days which should be fine since ValidationDetails is focusing on the most recent runs.
    return `https://console.cloud.google.com/logs/query;query=logName%3D%22projects%2F${queryEnv}%2Flogs%2Fapp%22%0Atrace%3D%22projects%2F${queryEnv}%2Ftraces%2F${traceId}%22%0A%22${validationName}%22%0A%22${stateInfo.code}%22;timeRange=P5D?project=${queryEnv}`;
  };

  // TODO(#9480): Allow user to see more than last 14 days.
  const fetchResults = React.useCallback(async () => {
    const r = await fetchValidationDescription(validationName);
    const text = await r.text();
    if (r.status >= 400) {
      message.error(`Fetching validation description... failed: ${text}`);
    } else {
      setValidationDescription(text);
    }

    return fetchValidationDetails(validationName, stateInfo.code);
  }, [validationName, stateInfo]);

  const validationFetched = useFetchedDataProtobuf<ValidationStatusRecords>(
    fetchResults,
    ValidationStatusRecords.deserializeBinary
  );

  const validationLoading = validationFetched.loading;
  const validationData = validationFetched.data;
  const records = validationData?.getRecordsList();
  const latestRecord = records && records[0];
  const latestResultStatus =
    (latestRecord && getRecordStatus(latestRecord)) || RecordStatus.UNKNOWN;
  const validationLogLink =
    latestRecord && getValidationLogLink(latestRecord.getTraceId());

  return (
    <>
      <Divider orientation="left">Recent Run Details</Divider>
      {/* TODO(#9480): could let the user pick a run but just default to the most recent */}
      <Card title="Summary">
        {validationLoading ? (
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
        {validationLoading && <Spin />}
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
        {validationLoading && <Spin />}
        {records && (
          <>
            <Card title="Graph">
              <ValidationDetailsGraph
                // Use slice() to copy before reversing.
                records={records.slice().reverse()}
                isPercent={latestRecord?.getIsPercentage()}
              />
            </Card>

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
