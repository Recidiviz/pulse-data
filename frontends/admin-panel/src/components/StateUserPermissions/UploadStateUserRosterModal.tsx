// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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
import { Modal, PageHeader } from "antd";
import { useState } from "react";
import Draggable from "react-draggable";
import UploadRoster from "../UploadRostersView/UploadRoster";
import { DraggableModal } from "./utils";

export const UploadStateUserRosterModal = ({
  visible,
  onCancel,
}: {
  visible: boolean;
  onCancel: () => void;
}): JSX.Element => {
  const [stateCode, setStateCode] = useState<string | undefined>();
  const [role, setRole] = useState<string | undefined>();
  const draggableModal = DraggableModal();

  const handleCancel = () => {
    onCancel();
  };

  const roleParam = role ? `&role=${role}` : "";

  return (
    <Modal
      visible={visible}
      modalRender={(modal) => (
        <Draggable
          disabled={draggableModal.disabled}
          bounds={draggableModal.bounds}
          onStart={(event, uiData) => draggableModal.onStart(event, uiData)}
        >
          <div ref={draggableModal.dragRef}>{modal}</div>
        </Draggable>
      )}
      bodyStyle={{
        overflowY: "scroll",
        maxHeight: "calc(100vh - 200px)",
      }}
      width={700}
      onCancel={handleCancel}
      onOk={handleCancel}
      footer={null}
    >
      <PageHeader title="Upload user roster" />
      <UploadRoster
        action={`/auth/users?state_code=${stateCode}${roleParam}`}
        method="PUT"
        columns={[
          "email_address",
          "role",
          "district",
          "external_id",
          "first_name",
          "last_name",
        ]}
        setStateCode={setStateCode}
        stateCode={stateCode}
        setRole={setRole}
        enableRoleSelector
      />
    </Modal>
  );
};
