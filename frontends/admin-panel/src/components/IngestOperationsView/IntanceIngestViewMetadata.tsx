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
import * as React from "react";
import { IngestInstanceSummary } from "./constants";

interface InstanceIngestViewMetadataProps {
  data: IngestInstanceSummary;
}

const InstanceIngestViewMetadata: React.FC<InstanceIngestViewMetadataProps> = ({
  data,
}) => {
  return (
    <div>
      TODO(#9717): Hydrate this view with metadata about ingest view
      materialization jobs / contents.
    </div>
  );
};

export default InstanceIngestViewMetadata;
