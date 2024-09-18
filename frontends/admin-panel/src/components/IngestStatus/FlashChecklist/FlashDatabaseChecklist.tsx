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
import { Alert, message, Modal, PageHeader, Spin } from "antd";
import * as React from "react";
import { useHistory } from "react-router";

import { fetchIngestStateCodes } from "../../../AdminPanelAPI";
import { isRawDataImportDagEnabled } from "../../../AdminPanelAPI/IngestOperations";
import { StateCodeInfo } from "../../general/constants";
import StateSelector from "../../Utilities/StateSelector";
import { DirectIngestInstance, RawDataDagEnabled } from "../constants";
import { FlashChecklistContext, FlashStore } from "./FlashChecklistStore";
import { getValueIfResolved } from "./FlashUtils";
import { LegacyFlashChecklistStore } from "./LegacyFlashChecklistStore";
import LegacyFlashDatabaseChecklistActiveComponent from "./LegacyFlashDatabaseChecklistActiveComponent";
import { NewFlashChecklistStore } from "./NewFlashChecklistStore";
import NewFlashDatabaseChecklistActiveComponent from "./NewFlashDatabaseChecklistActiveComponent";

const FlashDatabaseChecklist = (): JSX.Element => {
  const [stateInfo, setStateInfo] = React.useState<StateCodeInfo | undefined>(
    undefined
  );
  const [modalOpen, setModalOpen] = React.useState(true);

  // TODO(#28239) remove once raw data DAG is enabled for all states
  const [rawDataImportDagEnabled, setRawDataImportDagEnabled] = React.useState<
    RawDataDagEnabled | undefined
  >(undefined);

  const history = useHistory();

  // we only want to re-generate the FlashChecklistStore each time we re-select a state
  const flashProvider = React.useMemo(() => {
    const newStore: FlashStore = {
      legacyChecklistStore: new LegacyFlashChecklistStore(stateInfo),
      newChecklistStore: new NewFlashChecklistStore(stateInfo),
    };
    return newStore;
  }, [stateInfo]);

  const getRawDataEnabled = React.useCallback(async () => {
    if (stateInfo) {
      try {
        const enabledResults = await Promise.allSettled([
          isRawDataImportDagEnabled(
            stateInfo.code,
            DirectIngestInstance.PRIMARY
          ),
          isRawDataImportDagEnabled(
            stateInfo.code,
            DirectIngestInstance.SECONDARY
          ),
        ]);
        setRawDataImportDagEnabled({
          primary: await getValueIfResolved(enabledResults[0])?.json(),
          secondary: await getValueIfResolved(enabledResults[1])?.json(),
        });
      } catch (err) {
        message.error(`An error occurred: ${err}`);
      }
    }
  }, [stateInfo]);

  React.useEffect(() => {
    getRawDataEnabled();
  }, [getRawDataEnabled]);

  let activeComponent;
  if (!stateInfo) {
    activeComponent = (
      <Alert
        message="Select a state"
        description="Once you pick a state, this form will display the set of instructions required to flash a secondary database to primary."
        type="info"
        showIcon
      />
    );
  } else if (
    !flashProvider ||
    // there will be a moment after we have defined our state code (or are switching)
    // but before rawDataImportDagEnabled is properly set that we want to spin instead
    // of rendering the flashing checklist
    (stateInfo !== undefined && rawDataImportDagEnabled === undefined)
  ) {
    activeComponent = <Spin />;
  } else if (
    rawDataImportDagEnabled !== undefined &&
    rawDataImportDagEnabled.primary &&
    rawDataImportDagEnabled.secondary
  ) {
    activeComponent = <NewFlashDatabaseChecklistActiveComponent />;
  } else if (
    rawDataImportDagEnabled &&
    rawDataImportDagEnabled.primary !== rawDataImportDagEnabled.secondary
  ) {
    activeComponent = (
      <div>
        <Alert
          message="Cannot proceed with flash, as we cannot convert between new raw data metadata and legacy tables"
          description={`PRIMARY: ${
            rawDataImportDagEnabled.primary ? "enabled" : "not enabled"
          } // SECONDARY: ${
            rawDataImportDagEnabled.secondary ? "enabled" : "not enabled"
          }`}
        />
        <br />
        To proceed, please ensure both raw data instances are using the same
        infrastructure
      </div>
    );
  } else {
    activeComponent = <LegacyFlashDatabaseChecklistActiveComponent />;
  }

  return (
    <>
      <PageHeader
        title="Flash Primary Database"
        extra={
          <StateSelector
            fetchStateList={fetchIngestStateCodes}
            onChange={(state) => {
              setRawDataImportDagEnabled(undefined);
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
