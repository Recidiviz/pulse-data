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
import "./GraphNodeDetailDrawer.css";

import { Drawer, Tabs, TabsProps } from "antd";
import { observer } from "mobx-react-lite";

import { useUiStore } from "../../../../LineageStore/LineageRootContext";
import {
  BigQueryNodeType,
  GraphDirection,
  NodeDetailDrawerTab,
} from "../../../../LineageStore/types";
import GraphNodeAncestorSearchCard from "../GraphNodeAncestorSearchCard/GraphNodeAncestorSearchCard";
import BigQuerySourceTableDetailCard from "../GraphNodeDetailCard/BigQuerySourceTableDetailCard/BigQuerySourceTableDetailCard";
import BigQueryViewDetailCard from "../GraphNodeDetailCard/BigQueryViewDetailCard/BigQueryViewDetailCard";

export const GraphNodeDetailDrawer: React.FC = observer(() => {
  const {
    isNodeDetailDrawerOpen,
    setNodeDetailDrawerUrn,
    activeNodeDetailDrawerTab,
    setActiveNodeDetailDrawerTab,
    nodeDetailDrawerNode,
  } = useUiStore();

  const items: TabsProps["items"] = [
    {
      key: NodeDetailDrawerTab.DETAILS,
      label: "Details",
    },
    {
      key: NodeDetailDrawerTab.UPSTREAM_SEARCH,
      label: "Upstream Views",
    },
    {
      key: NodeDetailDrawerTab.DOWNSTREAM_SEARCH,
      label: "Downstream Views",
    },
  ];

  const detailComponent =
    nodeDetailDrawerNode &&
    nodeDetailDrawerNode.data.type === BigQueryNodeType.VIEW ? (
      <BigQueryViewDetailCard />
    ) : (
      <BigQuerySourceTableDetailCard />
    );

  return (
    <Drawer
      open={isNodeDetailDrawerOpen}
      onClose={() => {
        setActiveNodeDetailDrawerTab(NodeDetailDrawerTab.DETAILS);
        setNodeDetailDrawerUrn(undefined);
      }}
      closable
      keyboard
      maskClosable
      className="graph-node-detail-drawer"
      size="large"
      headerStyle={{
        borderBottom: "0",
        paddingBottom: "0",
      }}
      title={
        <Tabs
          size="small"
          type="card"
          tabBarStyle={{ margin: "0 0 0 0" }}
          items={items}
          activeKey={activeNodeDetailDrawerTab}
          onChange={(key) =>
            setActiveNodeDetailDrawerTab(
              NodeDetailDrawerTab[key as keyof typeof NodeDetailDrawerTab]
            )
          }
        />
      }
    >
      {activeNodeDetailDrawerTab === NodeDetailDrawerTab.DETAILS &&
        detailComponent}
      {activeNodeDetailDrawerTab === NodeDetailDrawerTab.DOWNSTREAM_SEARCH && (
        <GraphNodeAncestorSearchCard direction={GraphDirection.DOWNSTREAM} />
      )}
      {activeNodeDetailDrawerTab === NodeDetailDrawerTab.UPSTREAM_SEARCH && (
        <GraphNodeAncestorSearchCard direction={GraphDirection.UPSTREAM} />
      )}
    </Drawer>
  );
});
