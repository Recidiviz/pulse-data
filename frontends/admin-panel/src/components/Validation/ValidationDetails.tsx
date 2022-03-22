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
import {
  fetchValidationDetails,
  fetchValidationDescription,
} from "../../AdminPanelAPI";
import { useFetchedDataProtobuf } from "../../hooks";
import {
  ValidationStatusRecord,
  ValidationStatusRecords,
} from "../../recidiviz/admin_panel/models/validation_pb";
import { RecordStatus, ValidationDetailsProps } from "./constants";
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

const ValidationDetails: React.FC<ValidationDetailsProps> = ({
  validationName,
  stateInfo,
}) => {
  const [validationDescription, setValidationDescription] =
    React.useState<string | undefined>(undefined);

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
    </>
  );
};

export default ValidationDetails;
