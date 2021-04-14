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
import { action, makeAutoObservable } from "mobx";
import { fetchRegionCodeFiles } from "../../AdminPanelAPI";

interface DirectIngestFileConfig {
  // eslint-disable-next-line camelcase
  file_tag: string;
  columns: string[];
}

interface DirectIngestFileConfigMap {
  raw: Record<string, DirectIngestFileConfig>;

  // eslint-disable-next-line camelcase
  ingest_view: Record<string, DirectIngestFileConfig>;
}

class DataDiscoveryStore {
  isLoading?: boolean;

  files?: DirectIngestFileConfigMap;

  constructor() {
    makeAutoObservable(this, {
      loadDirectIngestFileConfigs: action,
    });
  }

  async loadDirectIngestFileConfigs(regionCode: string): Promise<void> {
    this.isLoading = true;
    try {
      const response = await fetchRegionCodeFiles(regionCode);
      this.files = await response.json();
    } finally {
      this.isLoading = false;
    }
  }

  columns(raw?: string[], ingest?: string[]): string[] {
    const found: Set<string> = new Set();
    if (!this.files) {
      return [];
    }
    for (let x = 0; x < Object.keys(this.files.raw).length; x += 1) {
      const { columns } = this.files.raw[x];
      columns.map((column) => found.add(column));
    }

    for (let x = 0; x < Object.keys(this.files.ingest_view).length; x += 1) {
      const { columns } = this.files.ingest_view[x];
      columns.map((column) => found.add(column));
    }

    return Array.from(found).sort();
  }
}
export default DataDiscoveryStore;
export type { DirectIngestFileConfigMap, DirectIngestFileConfig };
