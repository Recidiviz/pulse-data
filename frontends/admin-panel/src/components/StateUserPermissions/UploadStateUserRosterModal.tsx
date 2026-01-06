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
import { useState } from "react";

import { StateRolePermissionsResponse } from "../../types";
import { DraggableModal } from "../Utilities/DraggableModal";
import UploadRoster from "./UploadRoster";

export const UploadStateUserRosterModal = ({
  visible,
  onCancel,
  stateRoleData,
}: {
  visible: boolean;
  onCancel: () => void;
  stateRoleData: StateRolePermissionsResponse[];
}): JSX.Element => {
  const [stateCode, setStateCode] = useState<string | undefined>();
  const [reason, setReason] = useState<string | undefined>();

  const handleCancel = () => {
    onCancel();
  };

  return (
    <DraggableModal
      visible={visible}
      title="Upload user roster"
      width={700}
      onCancel={handleCancel}
      onOk={handleCancel}
      footer={null}
    >
      <UploadRoster
        action={`/auth/users?state_code=${stateCode}`}
        method="PUT"
        columns={[
          "email_address",
          "roles",
          "district",
          "external_id",
          "first_name",
          "last_name",
        ]}
        setStateCode={setStateCode}
        stateCode={stateCode}
        setReason={setReason}
        reason={reason}
        stateRoleData={stateRoleData}
        warningMessage="Add or update roster users via CSV upload. Matches existing users by email_address + state_code. Empty external_id preserves the existing value; other empty fields will clear existing values. All roles must have a State Role Default Permission entry."
      />
    </DraggableModal>
  );
};
