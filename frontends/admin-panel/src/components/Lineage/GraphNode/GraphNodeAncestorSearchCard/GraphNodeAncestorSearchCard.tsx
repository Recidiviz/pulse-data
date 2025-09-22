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
import "./GraphNodeAncestorSearchCard.css";

import { Input, message, Spin, Table } from "antd";
import { ColumnsType } from "antd/lib/table";
import { observer } from "mobx-react-lite";
import { useCallback, useEffect, useMemo, useState } from "react";

import { fetchAncestorUrns } from "../../../../AdminPanelAPI/LineageAPI";
import { useLineageRootStore } from "../../../../LineageStore/LineageRootContext";
import {
  BigQueryLineageNode,
  GraphDirection,
  NodeDetailDrawerTab,
} from "../../../../LineageStore/types";
import { getErrorMessage } from "../../../../LineageStore/Utils";

type GraphNodeAncestorSearchCardProps = {
  direction: GraphDirection;
};

type GraphNodeAncestorSearchTableOption = {
  datasetId: string;
  viewId: string;
  urn: string;
};

function buildAncestorSearchTableItems(
  node: BigQueryLineageNode
): GraphNodeAncestorSearchTableOption {
  const { datasetId, viewId, urn } = node;
  return { datasetId, viewId, urn };
}

const GraphNodeAncestorSearchCard: React.FC<GraphNodeAncestorSearchCardProps> =
  observer(({ direction }) => {
    const {
      uiStore: {
        nodeDetailDrawerUrn,
        setActiveNodeDetailDrawerTab,
        setNodeDetailDrawerUrn,
      },
      graphStore: { resetToSubGraph },
      lineageStore: { nodeForUrn },
    } = useLineageRootStore();

    const [searchValue, setSearchValue] = useState("");
    const [optionsLoading, setOptionLoading] = useState<boolean>(true);
    const [searchLoading, setSearchLoading] = useState<boolean>(false);
    const [options, setOptions] = useState<
      GraphNodeAncestorSearchTableOption[] | undefined
    >(undefined);
    const getOptions = useCallback(async () => {
      if (nodeDetailDrawerUrn) {
        setOptionLoading(true);
        try {
          const ancestorUrns = await fetchAncestorUrns(
            direction,
            nodeDetailDrawerUrn
          );
          setOptions(
            ancestorUrns.urns.map(nodeForUrn).map(buildAncestorSearchTableItems)
          );
        } catch (e) {
          message.error(getErrorMessage(e));
        }
        setOptionLoading(false);
      }
    }, [nodeDetailDrawerUrn, direction, nodeForUrn]);

    useEffect(() => {
      getOptions();
    }, [getOptions]);

    const activeOptions = useMemo(() => {
      if (options !== undefined) {
        return options.filter(
          (option) =>
            option.datasetId.includes(searchValue) ||
            option.viewId.includes(searchValue) ||
            option.urn.includes(searchValue)
        );
      }
      return [];
    }, [options, searchValue]);
    const tableScroll = useMemo(() => {
      if (options !== undefined && options.length !== 0) {
        return { x: "max-content" };
      }
      return {};
    }, [options]);

    if (
      optionsLoading ||
      options === undefined ||
      nodeDetailDrawerUrn === undefined
    ) {
      return <Spin />;
    }

    const columns: ColumnsType<GraphNodeAncestorSearchTableOption> = [
      {
        title: "Dataset",
        dataIndex: ["datasetId"],
        render: (text) => (
          <div style={{ whiteSpace: "normal", wordWrap: "break-word" }}>
            {text}
          </div>
        ),
        width: "10%",
      },
      {
        title: "View",
        dataIndex: ["viewId"],
      },
    ];

    return (
      <>
        <Input.Search
          className="node-search"
          placeholder="Search by view name..."
          enterButton="Show Path On Graph"
          allowClear
          onChange={(s) => setSearchValue(s.target.value)}
          onSearch={(value) => {
            setSearchLoading(true);
            // validate that node exists in graph
            nodeForUrn(value.trim());
            // expand sub graph from current node to end node
            resetToSubGraph(direction, nodeDetailDrawerUrn, value.trim()).then(
              () => {
                setActiveNodeDetailDrawerTab(NodeDetailDrawerTab.DETAILS);
                setNodeDetailDrawerUrn(undefined);
                setSearchLoading(false);
              },
              (e) => {
                message.error(getErrorMessage(e));
                setSearchLoading(false);
              }
            );
          }}
          size="large"
          value={searchValue}
          loading={searchLoading}
        />
        <Table
          className="graph-node-ancestor-search"
          scroll={tableScroll}
          columns={columns}
          dataSource={activeOptions}
          loading={optionsLoading}
          onRow={(record) => {
            return {
              onClick: () => setSearchValue(record.urn),
            };
          }}
          rowKey={(record) => record.urn}
        />
      </>
    );
  });

export default GraphNodeAncestorSearchCard;
