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

import { Collapse, message, Spin } from "antd";
import { observer } from "mobx-react-lite";
import { useCallback, useEffect, useState } from "react";

import { fetchSourceMetadata } from "../../../../../AdminPanelAPI/LineageAPI";
import { useUiStore } from "../../../../../LineageStore/LineageRootContext";
import {
  BigQueryNodeType,
  BigQuerySourceLineageDetail,
} from "../../../../../LineageStore/types";
import { getErrorMessage } from "../../../../../LineageStore/Utils";
import BigQueryGraphNodeDetailCardHeader from "../BigQueryGraphNodeDetailCardHeader/BigQueryGraphNodeDetailCardHeader";

const BigQuerySourceTableDetailCard: React.FC = observer(() => {
  const { nodeDetailDrawerUrn } = useUiStore();

  const [detailPageLoading, setDetailPageLoading] = useState<boolean>(true);
  const [sourceMetadata, setSourceMetadata] = useState<
    BigQuerySourceLineageDetail | undefined
  >(undefined);
  const getUrnDetail = useCallback(async () => {
    if (nodeDetailDrawerUrn) {
      setDetailPageLoading(true);
      try {
        const newSourceMetadata = await fetchSourceMetadata(
          nodeDetailDrawerUrn
        );
        setSourceMetadata(newSourceMetadata);
      } catch (e) {
        message.error(getErrorMessage(e));
      }
      setDetailPageLoading(false);
    }
  }, [nodeDetailDrawerUrn]);

  useEffect(() => {
    getUrnDetail();
  }, [getUrnDetail]);

  if (detailPageLoading || !sourceMetadata) {
    return <Spin />;
  }

  return (
    <>
      <BigQueryGraphNodeDetailCardHeader
        viewType={BigQueryNodeType.SOURCE_TABLE}
      />
      <Collapse
        bordered={false}
        defaultActiveKey={["description", "view-query"]}
        style={{ borderLeft: "3px solid var(--source-bbox)" }}
      >
        <Collapse.Panel
          header={
            <span className="node-detail-card-collapsable-header">
              Description
            </span>
          }
          key="description"
        >
          {sourceMetadata.description}
        </Collapse.Panel>
      </Collapse>
    </>
  );
});

export default BigQuerySourceTableDetailCard;
