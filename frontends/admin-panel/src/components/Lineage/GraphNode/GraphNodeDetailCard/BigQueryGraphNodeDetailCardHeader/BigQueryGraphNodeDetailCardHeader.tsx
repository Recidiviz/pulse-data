// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2025 Recidiviz, Inc.
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

import "./BigQueryGraphNodeDetailCardHeader.css";

import {
  DatabaseOutlined,
  FileSearchOutlined,
  FolderOpenOutlined,
  ReadOutlined,
} from "@ant-design/icons";
import { Spin } from "antd";
import { AutoTextSize } from "auto-text-size";
import { observer } from "mobx-react-lite";

import { useUiStore } from "../../../../../LineageStore/LineageRootContext";
import { BigQueryNodeType } from "../../../../../LineageStore/types";
import NewTabLink from "../../../../NewTabLink";
import { gcpEnvironment } from "../../../../Utilities/EnvironmentUtilities";

type BigQueryGraphNodeDetailCardHeaderProps = {
  viewType: string;
};

const BigQueryGraphNodeDetailCardHeader: React.FC<BigQueryGraphNodeDetailCardHeaderProps> =
  observer(({ viewType }) => {
    const { nodeDetailDrawerNode } = useUiStore();

    if (!nodeDetailDrawerNode) {
      return <Spin />;
    }

    const { viewId, datasetId, type } = nodeDetailDrawerNode.data;
    const env = gcpEnvironment.isProduction ? "prod" : "staging";
    const projectId = gcpEnvironment.isProduction
      ? "recidiviz-123"
      : "recidiviz-staging";

    const projectUrl = `https://go/bq-${env}`;
    const datasetUrl = `https://go/bq-${env}-dataset/${datasetId}`;
    const viewUrl = `https://go/bq-${env}/${datasetId}/${viewId}`;

    const icon =
      type === BigQueryNodeType.VIEW ? (
        <FileSearchOutlined style={{ display: "block", margin: "0 auto" }} />
      ) : (
        <ReadOutlined />
      );

    const typeText = type === BigQueryNodeType.VIEW ? viewType : "Source Table";

    return (
      <div className={`graph-detail-title graph-detail-title-${type}`}>
        <div className={`graph-detail-icon graph-detail-icon-${type}`}>
          {icon}
        </div>
        <div className="graph-detail-spacer" />
        <div className="group-detail-info">
          <div className="graph-detail-view-label">
            <AutoTextSize
              minFontSizePx={12}
              maxFontSizePx={20}
              fontSizePrecisionPx={1}
            >
              <NewTabLink
                href={viewUrl}
                className={`no-underline-hyperlink no-underline-hyperlink-${type}`}
              >
                {viewId}
              </NewTabLink>
            </AutoTextSize>
          </div>
          <div className="graph-detail-dataset-label">
            {typeText} |{" "}
            <NewTabLink
              href={projectUrl}
              className={`no-underline-hyperlink no-underline-hyperlink-${type}`}
            >
              {" "}
              <FolderOpenOutlined />
              {projectId}
            </NewTabLink>{" "}
            |{" "}
            <NewTabLink
              href={datasetUrl}
              className={`no-underline-hyperlink no-underline-hyperlink-${type}`}
            >
              <DatabaseOutlined /> {datasetId}
            </NewTabLink>
          </div>
        </div>
      </div>
    );
  });

export default BigQueryGraphNodeDetailCardHeader;
