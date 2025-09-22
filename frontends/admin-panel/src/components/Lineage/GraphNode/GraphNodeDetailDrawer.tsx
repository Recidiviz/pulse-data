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

import { Drawer } from "antd";
import { observer } from "mobx-react-lite";

import { useUiStore } from "../../../LineageStore/LineageRootContext";
import { BigQueryNodeType } from "../../../LineageStore/types";
import BigQuerySourceTableDetailCard from "./GraphNodeDetailCard/BigQuerySourceTableDetailCard/BigQuerySourceTableDetailCard";
import BigQueryViewDetailCard from "./GraphNodeDetailCard/BigQueryViewDetailCard/BigQueryViewDetailCard";

export const GraphNodeDetailDrawer: React.FC = observer(() => {
  const { nodeDetailDrawerOpen, setNodeDetailDrawerUrn, nodeDetailDrawerNode } =
    useUiStore();

  const detailCard =
    nodeDetailDrawerNode &&
    nodeDetailDrawerNode.data.type === BigQueryNodeType.VIEW ? (
      <BigQueryViewDetailCard />
    ) : (
      <BigQuerySourceTableDetailCard />
    );

  return (
    <Drawer
      open={nodeDetailDrawerOpen}
      onClose={() => setNodeDetailDrawerUrn(undefined)}
      closable
      keyboard
      maskClosable
      size="large"
    >
      {detailCard}
    </Drawer>
  );
});
