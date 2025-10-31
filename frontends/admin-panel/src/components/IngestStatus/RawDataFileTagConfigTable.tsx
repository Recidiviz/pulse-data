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
import {
  AlignLeftOutlined,
  BookOutlined,
  GoogleOutlined,
} from "@ant-design/icons";
import { Card, Divider } from "antd";
import Descriptions from "antd/lib/descriptions";
import Spin from "antd/lib/spin";

import NewTabLink from "../NewTabLink";
import { RawFileConfigSummary } from "./constants";

interface RawFileConfigTableProps {
  rawFileConfigSummary: RawFileConfigSummary | undefined;
  stateCode: string;
  loading: boolean;
}

const RawFileConfigTable: React.FC<RawFileConfigTableProps> = ({
  rawFileConfigSummary,
  stateCode,
  loading,
}) => {
  if (loading) {
    return <Spin />;
  }

  const env =
    window.RUNTIME_GCP_ENVIRONMENT === "production" ? "prod" : "staging";

  return (
    <>
      {rawFileConfigSummary ? (
        <>
          <Divider orientation="left"> Useful Links </Divider>
          <Card>
            <Descriptions
              bordered
              column={{ xs: 1, sm: 1, md: 1, lg: 1, xl: 10 }}
            >
              <Descriptions.Item>
                <NewTabLink
                  href={`http://go/raw-data-gitbook/${stateCode.toLowerCase()}/${rawFileConfigSummary.fileTag.toLowerCase()}`}
                >
                  <BookOutlined /> GitBook Raw Data Page
                </NewTabLink>
              </Descriptions.Item>
              <Descriptions.Item>
                <NewTabLink
                  href={`http://go/${env}-raw-data-file/${stateCode.toLowerCase()}/${
                    rawFileConfigSummary.fileTag
                  }`}
                >
                  <GoogleOutlined /> Big Query Raw Data Table
                </NewTabLink>
              </Descriptions.Item>
              <Descriptions.Item>
                <NewTabLink
                  href={`http://go/${env}-raw-data-latest/${stateCode.toLowerCase()}/${
                    rawFileConfigSummary.fileTag
                  }`}
                >
                  <GoogleOutlined /> Big Query Latest Table
                </NewTabLink>
              </Descriptions.Item>
              <Descriptions.Item>
                <NewTabLink
                  href={`http://go/raw-data-config/${stateCode.toLowerCase()}/${stateCode.toLowerCase()}/${
                    rawFileConfigSummary.fileTag
                  }`}
                >
                  <AlignLeftOutlined /> Raw File Config Yaml
                </NewTabLink>
              </Descriptions.Item>
              <Descriptions.Item>
                <NewTabLink
                  href={`http://go/raw-data-state-gitbook/${stateCode.toLowerCase()}`}
                >
                  <BookOutlined /> Gitbook Downstream Usages
                </NewTabLink>
              </Descriptions.Item>
            </Descriptions>
          </Card>
          <Divider orientation="left"> Raw File Config Summary </Divider>
          <Card>
            <Descriptions bordered>
              <Descriptions.Item label="Description" span={10}>
                {rawFileConfigSummary.fileDescription}
              </Descriptions.Item>
              <Descriptions.Item label="Update Cadence">
                {rawFileConfigSummary.updateCadence}
              </Descriptions.Item>
              <Descriptions.Item label="File Encoding">
                {rawFileConfigSummary.encoding}
              </Descriptions.Item>
              <Descriptions.Item label="Column Separator">
                {rawFileConfigSummary.separator}
              </Descriptions.Item>
              <Descriptions.Item label="Line Terminator">
                {rawFileConfigSummary.lineTerminator.replace("\n", "\\n")}
              </Descriptions.Item>
              <Descriptions.Item label="Export Lookback Window">
                {rawFileConfigSummary.exportLookbackWindow}
              </Descriptions.Item>
              <Descriptions.Item label="Code File?">
                {rawFileConfigSummary.isCodeFile ? "Yes" : "No"}
              </Descriptions.Item>
              <Descriptions.Item label="Chunked File?">
                {rawFileConfigSummary.isChunkedFile ? "Yes" : "No"}
              </Descriptions.Item>
              <Descriptions.Item label="Manually, Loss-ily Pruned?">
                {rawFileConfigSummary.manuallyPruned ? "Yes" : "No"}
              </Descriptions.Item>
              <Descriptions.Item label="Has Headers?">
                {rawFileConfigSummary.inferColumns ? "No" : "Yes"}
              </Descriptions.Item>
            </Descriptions>
          </Card>
        </>
      ) : (
        <>
          <Divider orientation="left"> Raw File Config Summary </Divider>
          <Card> No raw file config found </Card>
        </>
      )}
    </>
  );
};

export default RawFileConfigTable;
