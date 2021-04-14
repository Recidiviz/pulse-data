/*
Recidiviz - a data platform for criminal justice reform
Copyright (C) 2021 Recidiviz, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
=============================================================================
*/
import * as React from "react";
import { useEffect, useState } from "react";
import { Button, Checkbox, Divider, Form, Input, Space } from "antd";
import { InfoCircleOutlined } from "@ant-design/icons";
import { observer } from "mobx-react-lite";
import { useRootStore } from "../../stores";
import { DirectIngestFileConfig } from "../../stores/DataDiscoveryStore/DataDiscoveryStore";

const { Search } = Input;

interface FileCheckboxListProps {
  label: string;
  name: string;
  options: string[];
}

const FileCheckboxList = ({ label, name, options }: FileCheckboxListProps) => {
  return (
    <Form.Item label={label} name={name}>
      <Checkbox.Group>
        <Space direction="vertical">
          <>
            {options.map((option) => (
              <Checkbox value={option} key={option}>
                {option}
              </Checkbox>
            ))}
          </>
        </Space>
      </Checkbox.Group>
    </Form.Item>
  );
};

const getFileOptions = (
  fileEntries: Record<string, DirectIngestFileConfig>,
  nameFilter: string,
  columnFilter: string
) => {
  return Object.entries(fileEntries)
    .filter(
      ([file, config]) =>
        !nameFilter || config.file_tag.indexOf(nameFilter) !== -1
    )
    .filter(
      ([file, config]) =>
        !columnFilter ||
        config.columns.some(
          (column: string) => column.indexOf(columnFilter) !== -1
        )
    )
    .map(([file]) => file);
};
interface DataDiscoverySelectFilesViewProps {
  regionCode: string;
}

const DataDiscoverySelectFilesView = ({
  regionCode,
}: DataDiscoverySelectFilesViewProps): JSX.Element => {
  const { dataDiscoveryStore: store } = useRootStore();

  useEffect(() => {
    store.loadDirectIngestFileConfigs(regionCode);
  }, [store, regionCode]);

  const [nameFilter, setNameFilter] = useState("");
  const [columnFilter, setColumnFilter] = useState("");

  return store.isLoading ? (
    <span>Loading</span>
  ) : (
    <>
      <Space>
        <Search
          placeholder="Search by File Name"
          allowClear
          onSearch={(query) => setNameFilter(query.toLowerCase())}
          onChange={(event) => setNameFilter(event.target.value.toLowerCase())}
          style={{ width: 250 }}
        />
        <Search
          placeholder="Search by columns"
          onSearch={(query) => setColumnFilter(query.toLowerCase())}
          onChange={(event) =>
            setColumnFilter(event.target.value.toLowerCase())
          }
          allowClear
          style={{ width: 250 }}
        />
        <Button
          icon={<InfoCircleOutlined />}
          href={`https://app.gitbook.com/@recidiviz/s/recidiviz/state-ingest-catalog/${regionCode}/schema_mappings`}
          target="_blank"
        >
          View Gitbook Documentation
        </Button>
      </Space>
      <Divider />

      <Space align="start">
        <FileCheckboxList
          label="Raw Data Files"
          name="raw_files"
          options={
            store.files
              ? getFileOptions(store.files.raw, nameFilter, columnFilter)
              : []
          }
        />
        <FileCheckboxList
          label="Ingest Views"
          name="ingest_views"
          options={
            store.files
              ? getFileOptions(
                  store.files.ingest_view,
                  nameFilter,
                  columnFilter
                )
              : []
          }
        />
      </Space>
    </>
  );
};

export default observer(DataDiscoverySelectFilesView);
