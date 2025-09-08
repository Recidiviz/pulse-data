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
import { Button } from "antd";
import { useState } from "react";

import { triggerStateSpecificRawDataImportDAG } from "../../AdminPanelAPI/IngestOperations";
import ActionRegionConfirmationForm, {
  RegionAction,
  RegionActionContext,
  regionActionNames,
} from "../Utilities/ActionRegionConfirmationForm";
import { DirectIngestInstance } from "./constants";

interface IngestActionButtonProps {
  action: RegionAction;
  stateCode: string;
  instance?: DirectIngestInstance;
  buttonText?: string;
  block?: boolean;
  style?: React.CSSProperties;
  type?: "link" | "text" | "primary" | "default" | "ghost" | "dashed";
  onActionLoadingStateChanged?: (loading: boolean) => void;
  onActionConfirmed?: () => void;
}

const IngestActionButton: React.FC<IngestActionButtonProps> = ({
  action,
  stateCode,
  instance,
  buttonText,
  block,
  style,
  type,
  onActionLoadingStateChanged,
  onActionConfirmed,
}) => {
  const [isConfirmationModalOpen, setIsConfirmationModalOpen] = useState(false);

  const setActionLoadingState = (loading: boolean) => {
    if (onActionLoadingStateChanged) {
      onActionLoadingStateChanged(loading);
    }
  };

  const setActionConfirmed = () => {
    if (onActionConfirmed) {
      onActionConfirmed();
    }
  };

  const onIngestActionConfirmation = async (context: RegionActionContext) => {
    setIsConfirmationModalOpen(false);

    setActionLoadingState(true);
    const unsupportedIngestAction = "Unsupported ingest action";
    switch (context.ingestAction) {
      case RegionAction.TriggerStateSpecificRawDataDAG:
        if (instance) {
          await triggerStateSpecificRawDataImportDAG(stateCode, instance);
          setActionConfirmed();
        }
        break;
      default:
        throw unsupportedIngestAction;
    }
    setActionLoadingState(false);
  };

  return (
    <>
      <Button
        style={style}
        block={block}
        type={type}
        onClick={() => {
          setIsConfirmationModalOpen(true);
        }}
        key={action}
      >
        {buttonText}
      </Button>
      <ActionRegionConfirmationForm
        open={isConfirmationModalOpen}
        onConfirm={onIngestActionConfirmation}
        onCancel={() => {
          setIsConfirmationModalOpen(false);
        }}
        action={action}
        actionName={regionActionNames[action]}
        regionCode={stateCode}
        ingestInstance={instance}
      />
    </>
  );
};

export default IngestActionButton;
