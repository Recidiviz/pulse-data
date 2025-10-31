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

import { useReactFlow } from "@xyflow/react";
import { message, Modal, Space } from "antd";
import Paragraph from "antd/lib/typography/Paragraph";
import Title from "antd/lib/typography/Title";
import { observer } from "mobx-react-lite";
import { useState } from "react";

import { useUiStore } from "../../../../LineageStore/LineageRootContext";
import { NodeFilter } from "../../../../LineageStore/NodeFilter/NodeFilter";
import { NodeFilterKey, NodeFilterType } from "../../../../LineageStore/types";
import { getErrorMessage } from "../../../../LineageStore/Utils";
import { NodeFilterSelect } from "./NodeFilterSelect";

export const NodeFilterModal: React.FC = observer(() => {
  const {
    nodeFilters,
    updateFilters,
    filterModalOpen,
    setFilterModalState,
    allDatasetFilterOptions,
    allStateCodeFilterOptions,
  } = useUiStore();

  const { fitView } = useReactFlow();
  const [applyButtonLoading, setApplyButtonLoading] = useState(false);

  // list of filters that are displayed on the front end while the modal is open --
  // at the time when the user clicks "Apply", we use these filters to set nodeFilters
  // in the uiStore
  const [candidateFilters, setCandidateFilters] = useState<Array<NodeFilter>>(
    []
  );

  const addCandidateFilter = (filter: NodeFilter) => {
    setCandidateFilters([filter, ...candidateFilters]);
  };

  const removeCandidateFilter = (
    key: NodeFilterKey,
    type?: NodeFilterType,
    value?: string
  ) => {
    if (value !== undefined) {
      // remove this specific filter
      setCandidateFilters(
        candidateFilters.filter(
          (f) => !(f.type === type && f.value === value && f.key === key)
        )
      );
    } else {
      // remove this specific type
      setCandidateFilters(candidateFilters.filter((f) => f.key !== key));
    }
  };

  return (
    <Modal
      open={filterModalOpen}
      onOk={() => {
        try {
          setApplyButtonLoading(true);
          updateFilters(candidateFilters).then(
            () => {
              setApplyButtonLoading(false);
              setFilterModalState(false);
              fitView(); // reset viewport to see the whole graph
            },
            (e) => {
              message.error(getErrorMessage(e));
              setApplyButtonLoading(false);
            }
          );
        } catch (e) {
          message.error(getErrorMessage(e));
          setApplyButtonLoading(false);
        }
      }}
      onCancel={() => {
        setCandidateFilters(nodeFilters);
        setFilterModalState(false);
      }}
      destroyOnClose
      okText="Apply Filters"
      okButtonProps={{
        loading: applyButtonLoading,
      }}
    >
      <Title level={3}> Filter Select </Title>
      <Paragraph>
        Add or remove items from different filters to include or exclude nodes
        from the view graph. The options below are filters that will not filter
        out the &quot;selected&quot; node
      </Paragraph>
      <Space direction="vertical" size="middle" style={{ display: "flex" }}>
        <NodeFilterSelect
          mode="multiple"
          title="State Codes to Include"
          placeholder="e.g. US_XX"
          options={allStateCodeFilterOptions}
          type={NodeFilterType.INCLUDE}
          filterKey={NodeFilterKey.STATE_CODE_FILTER}
          addCandidateFilter={addCandidateFilter}
          removeCandidateFilter={removeCandidateFilter}
          initialFilters={nodeFilters}
        />
        <NodeFilterSelect
          mode="multiple"
          title="Datasets to Exclude"
          placeholder="e.g. fake_dataset"
          options={allDatasetFilterOptions}
          type={NodeFilterType.EXCLUDE}
          filterKey={NodeFilterKey.DATASET_ID_FILTER}
          addCandidateFilter={addCandidateFilter}
          removeCandidateFilter={removeCandidateFilter}
          initialFilters={nodeFilters}
        />
        <NodeFilterSelect
          mode="tags"
          title="Dataset Prefixes to Exclude"
          placeholder="(type and hit enter to add new options) e.g. fake_"
          options={[]}
          type={NodeFilterType.EXCLUDE}
          filterKey={NodeFilterKey.DATASET_ID_STARTS_WITH_FILTER}
          addCandidateFilter={addCandidateFilter}
          removeCandidateFilter={removeCandidateFilter}
          initialFilters={nodeFilters}
        />
      </Space>
    </Modal>
  );
});
