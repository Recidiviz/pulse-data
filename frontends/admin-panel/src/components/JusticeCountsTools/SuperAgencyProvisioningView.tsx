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
import { SearchOutlined } from "@ant-design/icons";
import {
  Button,
  Form,
  Input,
  PageHeader,
  Select,
  Space,
  Spin,
  Table,
  Typography,
  message,
} from "antd";
import { FilterDropdownProps } from "antd/lib/table/interface";
import { useState } from "react";
import { getAgencies } from "../../AdminPanelAPI";
import {
  addChildAgencyToSuperAgency,
  removeChildAgenciesFromSuperAgency,
  updateAgency,
} from "../../AdminPanelAPI/JusticeCountsTools";
import { useFetchedDataJSON } from "../../hooks";
import {
  AgenciesResponse,
  Agency,
  AgencyResponse,
  ErrorResponse,
  FipsCountyCode,
  FipsCountyCodeKey,
  StateCode,
  StateCodeKey,
} from "./constants";
import { formLayout, formTailLayout } from "../constants";

type SuperAgencyRecord = {
  id: number;
  name: string;
  state: string;
  county?: string;
  superAgencyId: number;
};

const SuperAgencyProvisioningView = (): JSX.Element => {
  const { data, setData } = useFetchedDataJSON<AgenciesResponse>(getAgencies);
  const [form] = Form.useForm();
  const [showSpinner, setShowSpinner] = useState(false);

  const superagencies =
    data?.agencies.filter((agency) => agency.is_superagency) || [];

  /* eslint-disable no-param-reassign */
  const agencyIdToChildAgencies =
    data?.agencies.reduce(function (obj, agency) {
      if (agency.super_agency_id !== undefined) {
        if (!obj[agency.super_agency_id]) {
          obj[agency.super_agency_id] = [agency.id];
        } else {
          obj[agency.super_agency_id].push(agency.id);
        }
      }
      return obj;
    }, {} as { [index: number]: number[] }) ||
    ({} as { [index: number]: number[] });

  const getColumnSearchProps = (dataIndex: keyof SuperAgencyRecord) => ({
    filterDropdown: ({
      setSelectedKeys,
      selectedKeys,
      confirm,
      clearFilters,
    }: FilterDropdownProps) => (
      <div style={{ padding: 8 }}>
        <Input
          placeholder={`Search ${dataIndex}`}
          value={selectedKeys[0]}
          onChange={(e) =>
            setSelectedKeys(e.target.value ? [e.target.value] : [])
          }
          onPressEnter={() => confirm()}
          style={{ marginBottom: 8, display: "block" }}
        />
        <Space>
          <Button
            type="primary"
            onClick={() => confirm()}
            icon={<SearchOutlined />}
            size="small"
            style={{ width: 90 }}
          >
            Search
          </Button>
          <Button onClick={clearFilters} size="small" style={{ width: 90 }}>
            Reset
          </Button>
        </Space>
      </div>
    ),
    filterIcon: (filtered: boolean) => (
      <SearchOutlined style={{ color: filtered ? "#1890ff" : undefined }} />
    ),
    onFilter: (value: string | number | boolean, record: SuperAgencyRecord) => {
      const result = record[dataIndex];
      return result
        ? result
            .toString()
            .toLowerCase()
            .includes(value.toString().toLowerCase())
        : false;
    },
  });

  const onUpdateChildAgencies = async (
    newChildAgencyIds: number[],
    currentChildAgencyIds: number[],
    currentAgency: Agency
  ) => {
    // This function supports the UI functionality where child agencies can be added or removed
    // to super agencies. Child agencies can be removed from a super agency individually or all
    // child agencies can be cleared (via an X button). Child agencies can only be added one at a time.
    if (newChildAgencyIds.length < currentChildAgencyIds.length) {
      // Remove child agency
      const removedChildAgenciesList = currentChildAgencyIds.filter(
        (id) => !newChildAgencyIds.includes(id)
      );
      try {
        const response = await removeChildAgenciesFromSuperAgency(
          currentAgency.id,
          removedChildAgenciesList
        );
        if (!response.ok) {
          const { error } = (await response.json()) as ErrorResponse;
          message.error(`An error occured: ${error}`);
          return;
        }
        const { agencies } = (await response.json()) as AgenciesResponse;
        const updatedAgencies = data?.agencies || [];
        agencies.forEach((currAgency) => {
          const agencyIndex = data?.agencies
            .map((a) => a.id)
            .indexOf(currAgency.id);
          if (agencyIndex !== undefined && data !== undefined) {
            updatedAgencies[agencyIndex] = currAgency;
          } else {
            message.error(`${currAgency.name} cannot be found.`);
          }
        });
        setData({
          agencies: updatedAgencies,
          systems: data?.systems || [],
        });
        const msg =
          removedChildAgenciesList.length === 1
            ? `${
                data?.agencies
                  .filter((a) => a.id === removedChildAgenciesList[0])
                  .map((a) => a.name)[0]
              } was removed as a child agency from ${currentAgency.name}!`
            : `All child agencies were removed from ${currentAgency.name}!`;
        message.success(msg);
      } catch (err) {
        message.error(`An error occured: ${err}`);
      }
    } else {
      // Add child agency
      const newChildAgencyIdList = newChildAgencyIds.filter(
        (id) => !currentChildAgencyIds.includes(id)
      );
      try {
        const response = await addChildAgencyToSuperAgency(
          currentAgency.id /** superAgencyId */,
          newChildAgencyIdList[0] /** childAgencyId */
        );
        if (!response.ok) {
          const { error } = (await response.json()) as ErrorResponse;
          message.error(`An error occured: ${error}`);
          return;
        }
        const { agency } = (await response.json()) as AgencyResponse;
        const agencyIndex = data?.agencies.map((a) => a.id).indexOf(agency.id);
        if (agencyIndex !== undefined && data !== undefined) {
          const updatedAgencies = data.agencies;
          updatedAgencies[agencyIndex] = agency;
          setData({
            agencies: updatedAgencies,
            systems: data?.systems || [],
          });
          message.success(
            `${agency.name} was added to as a child agency to ${currentAgency.name}!`
          );
        } else {
          message.error("Agency cannot be found.");
        }
      } catch (err) {
        message.error(`An error occured: ${err}`);
      }
    }
  };

  const onCreateSuperagency = async ({ agencyId }: { agencyId: number }) => {
    setShowSpinner(true);
    try {
      const response = await updateAgency(null, null, agencyId, true);
      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        message.error(`An error occured: ${error}`);
        return;
      }
      const { agency } = (await response.json()) as AgencyResponse;
      const agencyIndex = data?.agencies.map((a) => a.id).indexOf(agency.id);
      if (agencyIndex !== undefined && data !== undefined) {
        const updatedAgencies = data.agencies;
        updatedAgencies[agencyIndex] = agency;
        setData({
          agencies: updatedAgencies,
          systems: data?.systems || [],
        });
      }
      form.resetFields();
      setShowSpinner(false);
      message.success(`"Superagency added!`);
    } catch (err) {
      setShowSpinner(false);
      message.error(`An error occured: ${err}`);
    }
  };

  const columns = [
    {
      title: "ID",
      dataIndex: "id",
      key: "id",
      ...getColumnSearchProps("id"),
    },
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
      ...getColumnSearchProps("name"),
    },
    {
      title: "State",
      dataIndex: "state",
      key: "stateCode",
      ...getColumnSearchProps("state"),
    },
    {
      title: "County",
      dataIndex: "county",
      key: "fipsCountyCode",
      ...getColumnSearchProps("county"),
    },
    {
      title: "Child Agencies",
      key: "fipsCountyCode",
      render: (agency: Agency) => {
        const { id } = agency;
        const currentChildAgencyIds = !agencyIdToChildAgencies[id]
          ? []
          : agencyIdToChildAgencies[id];
        return (
          <Select
            mode="multiple"
            allowClear
            defaultValue={currentChildAgencyIds}
            showSearch
            optionFilterProp="children"
            filterOption={(input, option) =>
              (option?.children as unknown as string)
                .toLowerCase()
                .indexOf(input.toLowerCase()) >= 0
            }
            onChange={(newChildAgencyIds: number[]) => {
              onUpdateChildAgencies(
                newChildAgencyIds,
                currentChildAgencyIds,
                agency
              );
            }}
            style={{ minWidth: 500 }}
          >
            {/* #TODO(#12091): Replace with debounced search bar */}
            {data?.agencies
              .filter((currAgency) => currAgency.id !== agency.id)
              .map((currAgency) => (
                <Select.Option key={currAgency.id} value={currAgency.id}>
                  {currAgency.name}
                </Select.Option>
              ))}
          </Select>
        );
      },
    },
  ];
  return (
    <>
      <PageHeader title="Super Agency Provisioning" />
      <Table
        columns={columns}
        dataSource={superagencies.map((agency) => ({
          ...agency,
          state:
            StateCode[agency.state_code?.toLocaleLowerCase() as StateCodeKey],
          county: FipsCountyCode[agency.fips_county_code as FipsCountyCodeKey],
          superAgencyId: agency.super_agency_id,
        }))}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          size: "small",
        }}
        rowKey={(agency) => agency.id}
      />
      <Form
        {...formLayout}
        form={form}
        onFinish={onCreateSuperagency}
        requiredMark={false}
      >
        <Typography.Title
          level={4}
          style={{ paddingTop: 16, paddingBottom: 8 }}
        >
          Add Superagency
        </Typography.Title>
        <Form.Item label="Name" name="agencyId" rules={[{ required: true }]}>
          <Select
            showSearch
            optionFilterProp="children"
            disabled={showSpinner}
            filterOption={(input, option) =>
              (option?.children as unknown as string)
                .toLowerCase()
                .indexOf(input.toLowerCase()) >= 0
            }
          >
            {data?.agencies
              .filter((agency) => !agency.is_superagency)
              .map((agency) => (
                <Select.Option key={agency.id} value={agency.id}>
                  {agency.name}
                </Select.Option>
              ))}
          </Select>
        </Form.Item>
        <Form.Item {...formTailLayout}>
          <Button type="primary" htmlType="submit" disabled={showSpinner}>
            Submit
          </Button>
          {showSpinner && <Spin style={{ marginLeft: 16 }} />}
        </Form.Item>
      </Form>
    </>
  );
};

export default SuperAgencyProvisioningView;
