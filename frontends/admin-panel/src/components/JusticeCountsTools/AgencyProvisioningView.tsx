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
import { Link } from "react-router-dom";
import { createAgency, getAgencies } from "../../AdminPanelAPI";
import {
  deleteAgencyUsers,
  getUsers,
  updateAgency,
  updateAgencyUsers,
} from "../../AdminPanelAPI/JusticeCountsTools";
import { useFetchedDataJSON } from "../../hooks";
import { formLayout, formTailLayout } from "../constants";
import {
  AgenciesResponse,
  Agency,
  AgencyTeamMember,
  CreateAgencyRequest,
  CreateAgencyResponse,
  ErrorResponse,
  FipsCountyCode,
  FipsCountyCodeKey,
  StateCode,
  StateCodeKey,
  System,
  UsersResponse,
} from "./constants";

const AgencyProvisioningView = (): JSX.Element => {
  const [showSpinner, setShowSpinner] = useState(false);
  const [selectedStateCode, setSelectedStateCode] = useState<string>("");
  const { data, setData } = useFetchedDataJSON<AgenciesResponse>(getAgencies);
  const { data: usersData } = useFetchedDataJSON<UsersResponse>(getUsers);
  const [form] = Form.useForm();
  const onFinish = async ({
    name,
    systems,
    stateCode,
    fipsCountyCode,
  }: CreateAgencyRequest) => {
    const nameTrimmed = name.trim();
    const systemsTrimmed = systems.map((system) => system.trim());
    const stateCodeTrimmed = stateCode.trim().toLocaleLowerCase();
    const fipsCountyCodeTrimmed =
      fipsCountyCode !== undefined
        ? fipsCountyCode.trim().toLocaleLowerCase()
        : undefined;
    setShowSpinner(true);
    try {
      const response = await createAgency(
        nameTrimmed,
        systemsTrimmed,
        stateCodeTrimmed,
        fipsCountyCodeTrimmed
      );
      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        setShowSpinner(false);
        message.error(`An error occured: ${error}`);
        return;
      }
      const { agency } = (await response.json()) as CreateAgencyResponse;
      setData({
        agencies: data?.agencies ? [...data.agencies, agency] : [agency],
        systems: data?.systems || [],
      });
      form.resetFields();
      setShowSpinner(false);
      message.success(`"${nameTrimmed}" added!`);
    } catch (err) {
      setShowSpinner(false);
      message.error(`An error occured: ${err}`);
    }
  };

  type AgencyRecord = {
    id: number;
    name: string;

    state: string;
    county?: string;
    team: AgencyTeamMember[];
  };

  const getColumnSearchProps = (dataIndex: keyof AgencyRecord) => ({
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
    onFilter: (value: string | number | boolean, record: AgencyRecord) => {
      const result = record[dataIndex];
      return result
        ? result
            .toString()
            .toLowerCase()
            .includes(value.toString().toLowerCase())
        : false;
    },
  });

  const onNameChange = async (agency: Agency, name: string) => {
    try {
      const response = await updateAgency(
        /** name */ name,
        /** systems */ null,
        /** agency_id */ agency.id,
        /** isSuperagency */ null,
        /** childAgencyIds */ null
      );
      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        message.error(`An error occured: ${error}`);
        return;
      }
      message.success(`${agency.name}'s name changed to ${name}!`);
    } catch (err) {
      message.error(`An error occured: ${err}`);
    }
  };

  const onSystemsChange = async (systems: string[], agency: Agency) => {
    try {
      const response = await updateAgency(
        /** name */ null,
        /** systems */ systems,
        /** agency_id */ agency.id,
        /** isSuperagency */ null,
        /** childAgencyIds */ null
      );
      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        message.error(`An error occured: ${error}`);
        return;
      }
      message.success(`${agency.name}'s systems were successfully updated.`);
    } catch (err) {
      message.error(`An error occured: ${err}`);
    }
  };

  const onUpdateTeamMember = async (
    newTeamMemberEmails: string[],
    currentTeamMemberEmails: string[],
    currentAgency: Agency
  ) => {
    try {
      const response = await Promise.resolve(
        newTeamMemberEmails.length > currentTeamMemberEmails.length
          ? updateAgencyUsers(
              /** agencyId */ currentAgency.id.toString(),
              /** emails */ newTeamMemberEmails.filter(
                (x) => !currentTeamMemberEmails.includes(x)
              ),
              /** role */ null
            )
          : deleteAgencyUsers(
              /** agencyId */ currentAgency.id.toString(),
              /** emails */ currentTeamMemberEmails.filter(
                (x) => !newTeamMemberEmails.includes(x)
              )
            )
      );
      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        message.error(`An error occured: ${error}`);
        return;
      }
      const { agencies, systems } = (await response.json()) as AgenciesResponse;
      setData({
        agencies,
        systems,
      });
      const successMessage =
        currentAgency.is_superagency === true
          ? `${currentAgency.name} and child agencies were successfully updated!`
          : `${currentAgency.name} was successfully updated!`;
      message.success(successMessage);
    } catch (err) {
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
      width: "15%",
      render: (_: string, agency: Agency) => {
        return (
          <Input
            defaultValue={agency.name}
            onBlur={(e) => onNameChange(agency, e.target.value)}
          />
        );
      },
    },
    {
      title: "Systems",
      dataIndex: "systems",
      key: "system",
      render: (systems: string[], agency: Agency) => {
        return (
          <Select
            mode="multiple"
            allowClear
            defaultValue={systems}
            showSearch
            optionFilterProp="children"
            disabled={showSpinner}
            filterOption={(input, option) =>
              (option?.children as unknown as string)
                .toLowerCase()
                .indexOf(input.toLowerCase()) >= 0
            }
            onChange={(updatedSystems: string[]) =>
              onSystemsChange(updatedSystems, agency)
            }
            style={{ minWidth: 250 }}
          >
            {/* #TODO(#12091): Replace with debounced search bar */}
            {Object.keys(System).map((system) => (
              <Select.Option key={system} value={system}>
                {system}
              </Select.Option>
            ))}
          </Select>
        );
      },
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
      title: "Team Members",
      key: "fipsCountyCode",
      render: (agency: Agency) => {
        const { team } = agency;
        const currentTeamMemberEmails = team.map((user) => user.email);
        return (
          <Select
            mode="multiple"
            allowClear
            defaultValue={currentTeamMemberEmails}
            showSearch
            optionFilterProp="children"
            disabled={showSpinner}
            filterOption={(input, option) =>
              (option?.children as unknown as string)
                .toLowerCase()
                .indexOf(input.toLowerCase()) >= 0
            }
            onChange={(newTeamMemberEmails: string[]) => {
              onUpdateTeamMember(
                newTeamMemberEmails,
                currentTeamMemberEmails,
                agency
              );
            }}
            style={{ minWidth: 250 }}
          >
            {/* #TODO(#12091): Replace with debounced search bar */}
            {usersData?.users.map((user) => (
              <Select.Option key={user.email} value={user.email}>
                {user.name}
              </Select.Option>
            ))}
          </Select>
        );
      },
    },
    {
      title: "Roles",
      dataIndex: "id",
      key: "fipsCountyCode",
      render: (agencyId: number) => {
        const linkUrl = `/admin/justice_counts_tools/agency/${agencyId}/users`;
        return <Link to={linkUrl}>Team Member Roles</Link>;
      },
    },
  ];
  return (
    <>
      <PageHeader title="Agency Provisioning" />
      <Table
        columns={columns}
        dataSource={data?.agencies.map((agency) => ({
          ...agency,
          state:
            StateCode[agency.state_code?.toLocaleLowerCase() as StateCodeKey],
          county: FipsCountyCode[agency.fips_county_code as FipsCountyCodeKey],
          teamMembers: agency.team.map((u) => u.email),
        }))}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          size: "small",
        }}
        rowKey={(agency) => JSON.stringify(agency.id.toString() + agency.team)}
        // If the user is added to a Superagency, the table will re-render so that we can
        // can see the cells of the child agencies with the name of the user we added as well.
      />
      <Form
        {...formLayout}
        form={form}
        onFinish={onFinish}
        requiredMark={false}
      >
        <Typography.Title
          level={4}
          style={{ paddingTop: 16, paddingBottom: 8 }}
        >
          Add Agency
        </Typography.Title>
        <Form.Item label="Name" name="name" rules={[{ required: true }]}>
          <Input disabled={showSpinner} />
        </Form.Item>
        <Form.Item label="Systems" name="systems" rules={[{ required: true }]}>
          <Select mode="multiple" disabled={showSpinner || !data?.systems}>
            {data?.systems.map((system) => (
              <Select.Option key={system} value={system}>
                {system}
              </Select.Option>
            ))}
          </Select>
        </Form.Item>
        <Form.Item label="State" name="stateCode" rules={[{ required: true }]}>
          <Select
            showSearch
            optionFilterProp="children"
            disabled={showSpinner}
            filterOption={(input, option) =>
              (option?.children as unknown as string)
                .toLowerCase()
                .indexOf(input.toLowerCase()) >= 0
            }
            onSelect={(stateCode: string) => {
              setSelectedStateCode(stateCode);
            }}
          >
            {Object.keys(StateCode).map((stateCode) => (
              <Select.Option key={stateCode} value={stateCode}>
                {StateCode[stateCode as StateCodeKey]}
              </Select.Option>
            ))}
          </Select>
        </Form.Item>
        <Form.Item
          label="County"
          name="fipsCountyCode"
          rules={[{ required: false }]}
        >
          <Select
            showSearch
            optionFilterProp="children"
            disabled={showSpinner || !selectedStateCode}
            filterOption={(input, option) =>
              (option?.children as unknown as string)
                .toLowerCase()
                .indexOf(input.toLowerCase()) >= 0
            }
          >
            {Object.keys(FipsCountyCode)
              .filter((code) => code.startsWith(selectedStateCode))
              .map((fipsCountyCode) => (
                <Select.Option key={fipsCountyCode} value={fipsCountyCode}>
                  {FipsCountyCode[fipsCountyCode as FipsCountyCodeKey]}
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

export default AgencyProvisioningView;
