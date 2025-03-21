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
import { Alert, Modal, PageHeader, Spin } from "antd";
import * as React from "react";
import { useHistory } from "react-router-dom";

import { fetchIngestStateCodes } from "../../../AdminPanelAPI";
import { StateCodeInfo } from "../../general/constants";
import StateSelector from "../../Utilities/StateSelector";
import { FlashChecklistContext } from "./FlashChecklistStore";
import { NewFlashChecklistStore } from "./NewFlashChecklistStore";
import NewFlashDatabaseChecklistActiveComponent from "./NewFlashDatabaseChecklistActiveComponent";

const FlashDatabaseChecklist = (): JSX.Element => {
  const [stateInfo, setStateInfo] = React.useState<StateCodeInfo | undefined>(
    undefined
  );
  const [modalOpen, setModalOpen] = React.useState(true);

  const history = useHistory();

  // we only want to re-generate the FlashChecklistStore each time we re-select a state
  const flashProvider = React.useMemo(() => {
    const newStore: NewFlashChecklistStore = new NewFlashChecklistStore(
      stateInfo
    );
    return newStore;
  }, [stateInfo]);

  let activeComponent;
  if (stateInfo === undefined) {
    activeComponent = (
      <Alert
        message="Select a state"
        description="Once you pick a state, this form will display the set of instructions required to flash a secondary database to primary."
        type="info"
        showIcon
      />
    );
  } else if (!flashProvider) {
    activeComponent = <Spin />;
  } else {
    activeComponent = <NewFlashDatabaseChecklistActiveComponent />;
  }

  return (
    <>
      <PageHeader
        title="Flash Primary Database"
        extra={
          <StateSelector
            fetchStateList={fetchIngestStateCodes}
            onChange={(state) => {
              setStateInfo(state);
            }}
            initialValue={undefined}
          />
        }
      />
      <br />
      <Modal
        title="Confirm Role"
        open={modalOpen}
        maskClosable={false}
        closable={false}
        keyboard={false}
        onOk={() => setModalOpen(false)}
        onCancel={() => history.push("/admin")}
      >
        If you are not a full-time Recidiviz engineer, please navigate away from
        this page. By clicking OK, you attest that you are a full-time engineer
        who should be accessing this page.
      </Modal>
      <FlashChecklistContext.Provider value={flashProvider}>
        {activeComponent}
      </FlashChecklistContext.Provider>
    </>
  );
};
export default FlashDatabaseChecklist;
