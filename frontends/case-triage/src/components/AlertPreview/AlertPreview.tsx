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

import { TooltipTrigger } from "@recidiviz/design-system";
import React from "react";
import { Pill, PillKind } from "../Pill";

type AlertPreviewProps = {
  kind: PillKind;
  tooltip?: React.ReactNode;
};

const AlertPreview: React.FC<AlertPreviewProps> = ({
  children,
  kind,
  tooltip,
}) => {
  const AlertPill = (
    <Pill kind={kind} filled={false}>
      {children}
    </Pill>
  );

  return <TooltipTrigger contents={tooltip}>{AlertPill}</TooltipTrigger>;
};

export default AlertPreview;
