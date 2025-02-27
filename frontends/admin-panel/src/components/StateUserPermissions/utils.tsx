// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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
import { InfoCircleOutlined, SearchOutlined } from "@ant-design/icons";
import { Button, FormInstance, Input, Space, Tooltip, Typography } from "antd";
import { ColumnType, FilterDropdownProps } from "antd/lib/table/interface";
import moment from "moment";

import {
  FeatureVariants,
  Routes,
  StateRolePermissionsResponse,
  StateUserForm,
  StateUserPermissionsResponse,
} from "../../types";
import { ROUTES_PERMISSIONS_LABELS } from "../constants";

export function updatePermissionsObject(
  existing: Partial<Routes>,
  updated: Partial<Routes>,
  remove: string[]
): Partial<Routes> | undefined;

export function updatePermissionsObject(
  existing: Partial<FeatureVariants>,
  updated: Partial<FeatureVariants>,
  remove: string[]
): Partial<FeatureVariants> | undefined;

export function updatePermissionsObject(
  existing: Partial<Routes> | Partial<FeatureVariants>,
  updated: Partial<Routes> | Partial<FeatureVariants>,
  remove: string[]
): Partial<Routes> | Partial<FeatureVariants> | undefined {
  const newPermission = Object.entries(updated).reduce(
    (permissions, [permissionType, permissionValue]) => {
      if (permissionValue !== undefined) {
        return {
          ...permissions,
          [permissionType]: permissionValue,
        };
      }
      return { ...permissions };
    },
    existing
  );
  remove.forEach((key: string) => {
    newPermission[key as keyof typeof newPermission] = false;
  });
  return newPermission;
}

export const checkResponse = async (response: Response): Promise<void> => {
  if (!response.ok) {
    const error = await response.text();
    throw error;
  }
};

export function validateAndFocus<Param>(
  form: FormInstance,
  thenFunc: (values: Param) => void
): void {
  form
    .validateFields()
    .then(thenFunc)
    .catch((errorInfo) => {
      // hypothetically setting `scrollToFirstError` on the form should do this (or at least
      // scroll so the error is visible), but it doesn't seem to, so instead put the cursor in the
      // input directly.
      document.getElementById(errorInfo.errorFields?.[0].name?.[0])?.focus();
    });
}

const { Text } = Typography;
export const formatText = (
  text: string | boolean,
  record: StateUserPermissionsResponse
): string | boolean | JSX.Element => {
  if (
    record.blocked === true ||
    (!!record.blockedOn && isDateInPast(record.blockedOn))
  ) {
    return (
      <Text type="secondary" italic>
        {text}
      </Text>
    );
  }
  return text;
};

const titleWithInfoTooltip = (title: string, tooltip: string) => (
  <>
    {title}{" "}
    <Tooltip title={tooltip}>
      <InfoCircleOutlined />
    </Tooltip>
  </>
);

const getColumnSearchProps = (dataIndex: string) => ({
  filterDropdown: ({
    setSelectedKeys,
    selectedKeys,
    confirm,
    clearFilters,
  }: FilterDropdownProps) => (
    <div style={{ padding: 8 }}>
      <Input
        placeholder="Search..."
        value={selectedKeys[0]}
        onChange={(e) =>
          setSelectedKeys(e.target.value ? [e.target.value] : [])
        }
        onPressEnter={() => confirm()}
        style={{
          marginBottom: 8,
          display: "block",
        }}
      />
      <Space>
        <Button
          type="primary"
          onClick={() => confirm()}
          icon={<SearchOutlined />}
          size="small"
          style={{
            width: 90,
          }}
        >
          Search
        </Button>
        <Button
          onClick={() => {
            clearFilters?.();
            confirm();
          }}
          size="small"
          style={{
            width: 90,
          }}
        >
          Reset
        </Button>
      </Space>
    </div>
  ),
  filterIcon: (filtered: boolean) => (
    <SearchOutlined style={{ color: filtered ? "#1890ff" : undefined }} />
  ),
  onFilter: (
    value: string | number | boolean,
    record: StateUserOrRolePermissionsResponse
  ) => {
    const item = record[dataIndex as keyof StateUserOrRolePermissionsResponse];
    return typeof item !== "object"
      ? item.toString().toLowerCase().includes(value.toString().toLowerCase())
      : JSON.stringify(item)
          .toLowerCase()
          .includes(value.toString().toLowerCase());
  },
});

type StateUserOrRolePermissionsResponse =
  | StateUserPermissionsResponse
  | StateRolePermissionsResponse;

type ColumnData = { text: string; value: string | boolean };
export const filterData =
  <T extends StateUserOrRolePermissionsResponse>(colData: T[]) =>
  (formatter: (item: T) => string | string[]): ColumnData[] =>
    colData
      .map((item) => {
        const formattedItem = formatter(item);
        return Array.isArray(formattedItem) ? formattedItem : [formattedItem];
      })
      .reduce((acc, val) => acc.concat(val), [])
      .filter((v, i, a) => a.indexOf(v) === i)
      .sort()
      .map((item) => ({
        text: item,
        value: item,
      }));

export const getUserPermissionsTableColumns = (
  data: StateUserPermissionsResponse[],
  stateRoleData: StateRolePermissionsResponse[],
  setUserToEnable?: (user: StateUserPermissionsResponse) => void
): ColumnType<StateUserPermissionsResponse>[] => {
  return [
    {
      title: "State",
      dataIndex: "stateCode",
      key: "stateCode",
      width: 100,
      filters: [...filterData(data)((d) => d.stateCode)],
      onFilter: (
        value: string | number | boolean,
        record: StateUserPermissionsResponse
      ) => {
        return (
          record.stateCode?.indexOf(
            value as keyof StateUserPermissionsResponse
          ) === 0
        );
      },
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => a.stateCode.localeCompare(b.stateCode),
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "Email",
      dataIndex: "emailAddress",
      key: "emailAddress",
      width: 250,
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => a.emailAddress.localeCompare(b.emailAddress),
      ...getColumnSearchProps("emailAddress"),
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "First Name",
      dataIndex: "firstName",
      key: "firstName",
      width: 200,
      ...getColumnSearchProps("firstName"),
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "Last Name",
      dataIndex: "lastName",
      key: "lastName",
      ...getColumnSearchProps("lastName"),
      width: 200,
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "External ID",
      dataIndex: "externalId",
      width: 200,
      ...getColumnSearchProps("externalId"),
      render: (text: string, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "Roles",
      dataIndex: "roles",
      key: "roles",
      width: 150,
      filters: [...filterData(data)((d) => d.roles ?? [])],
      onFilter: (
        value: string | number | boolean,
        record: StateUserPermissionsResponse
      ) => {
        return !!record.roles && record.roles.includes(value as string);
      },
      render: (
        text: StateUserPermissionsResponse["roles"],
        record: StateUserPermissionsResponse
      ) => {
        return {
          children: (text ?? []).map((role) => formatText(`${role}\n`, record)),
        };
      },
    },
    {
      title: "District",
      dataIndex: "district",
      width: 250,
      key: "district",
      ...getColumnSearchProps("district"),
      render: (text: string, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "Routes",
      dataIndex: "routes",
      key: "routes",
      width: 300,
      ...getColumnSearchProps("routes"),
      render: (
        text: Record<string, unknown>,
        record: StateUserPermissionsResponse
      ) => {
        const role = stateRoleData.find(
          (d) =>
            d.stateCode === record.stateCode && record.roles?.includes(d.role)
        );
        return {
          props: {
            style: {
              background:
                JSON.stringify(role?.routes) === JSON.stringify(record.routes)
                  ? "none"
                  : "yellow",
              whiteSpace: "pre",
            },
          },
          children: formatRoutes(text),
        };
      },
    },
    {
      title: titleWithInfoTooltip(
        "Feature variants",
        "Note: active times are in *your* time zone"
      ),
      dataIndex: "featureVariants",
      key: "featureVariants",
      width: 350,
      ...getColumnSearchProps("featureVariants"),
      render: (
        text: Record<string, string>,
        record: StateUserPermissionsResponse
      ) => {
        const role = stateRoleData.find(
          (d) =>
            d.stateCode === record.stateCode && record.roles?.includes(d.role)
        );
        return {
          props: {
            style: {
              background:
                JSON.stringify(role?.featureVariants ?? {}) ===
                JSON.stringify(record.featureVariants ?? {})
                  ? "none"
                  : "yellow",
              whiteSpace: "pre",
            },
          },
          children: formatFeatureVariants(text),
        };
      },
    },
    {
      title: "Allowed Supervision Location IDs",
      dataIndex: "allowedSupervisionLocationIds",
      key: "allowedSupervisionLocationIds",
      filters: [...filterData(data)((d) => d.allowedSupervisionLocationIds)],
      onFilter: (
        value: string | number | boolean,
        record: StateUserPermissionsResponse
      ) => {
        return (
          record.allowedSupervisionLocationIds?.indexOf(
            value as keyof StateUserPermissionsResponse
          ) === 0
        );
      },
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) =>
        a.allowedSupervisionLocationIds?.localeCompare(
          b.allowedSupervisionLocationIds
        ),
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "Allowed Supervision Location Level",
      dataIndex: "allowedSupervisionLocationLevel",
      key: "allowedSupervisionLocationLevel",
      width: 220,
      filters: [...filterData(data)((d) => d.allowedSupervisionLocationLevel)],
      onFilter: (
        value: string | number | boolean,
        record: StateUserPermissionsResponse
      ) => {
        return (
          record.allowedSupervisionLocationLevel?.indexOf(
            value as keyof StateUserPermissionsResponse
          ) === 0
        );
      },
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) =>
        a.allowedSupervisionLocationIds?.localeCompare(
          b.allowedSupervisionLocationIds
        ),
      render: (text, record) => {
        return formatText(text, record);
      },
    },
    {
      title: "Block Status",
      dataIndex: "blockedOn",
      key: "blockedOn",
      width: 150,
      filters: [
        {
          value: "active",
          text: "Active",
        },
        {
          value: "blocked",
          text: "Blocked",
        },
        {
          value: "upcomingBlock",
          text: "Upcoming block",
        },
      ],
      onFilter: (
        value: boolean | string | number,
        record: StateUserPermissionsResponse
      ) => {
        const now = new Date();
        const blockedOn = record.blockedOn ? new Date(record.blockedOn) : null;
        switch (value) {
          case "active":
            return !blockedOn;
          case "blocked":
            return !!blockedOn && blockedOn <= now;
          case "upcomingBlock":
            return !!blockedOn && blockedOn > now;
          default:
            return false;
        }
      },
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => {
        const blockStatusA = sortOnBlockStatus(a);
        const blockStatusB = sortOnBlockStatus(b);

        // If multiple users have an upcoming block, sort within them by block date
        if (blockStatusA === 3 && blockStatusB === 3) {
          const blockedOnA = a.blockedOn
            ? new Date(a.blockedOn).getTime()
            : Infinity;
          const blockedOnB = b.blockedOn
            ? new Date(b.blockedOn).getTime()
            : Infinity;

          return blockedOnA - blockedOnB;
        }

        return blockStatusB - blockStatusA;
      },
      render: (value: string | null, record: StateUserPermissionsResponse) => {
        const now = new Date();
        const blockedOn = value ? new Date(value) : null;

        if (!blockedOn) {
          return undefined;
        }
        return (
          setUserToEnable && (
            <Button
              onClick={() => {
                setUserToEnable(record);
              }}
              style={{ height: "100%" }}
            >
              {blockedOn <= now ? (
                "Enable user"
              ) : (
                <>
                  Remove upcoming block:
                  <br />
                  {moment(blockedOn).format("lll")}
                </>
              )}
            </Button>
          )
        );
      },
    },
  ];
};

export const getRolePermissionsTableColumns = (
  stateRoleData: StateRolePermissionsResponse[]
): ColumnType<StateRolePermissionsResponse>[] => {
  return [
    {
      title: "State",
      dataIndex: "stateCode",
      key: "stateCode",
      width: 100,
      filters: [...filterData(stateRoleData)((d) => d.stateCode)],
      onFilter: (
        value: string | number | boolean,
        record: StateRolePermissionsResponse
      ) => {
        return (
          record.stateCode?.indexOf(
            value as keyof StateRolePermissionsResponse
          ) === 0
        );
      },
    },
    {
      title: "Role",
      dataIndex: "role",
      key: "role",
      width: 150,
      filters: [...filterData(stateRoleData)((d) => d.role)],
      onFilter: (
        value: string | number | boolean,
        record: StateRolePermissionsResponse
      ) => {
        return (
          record.role?.indexOf(value as keyof StateRolePermissionsResponse) ===
          0
        );
      },
      sorter: (
        a: StateRolePermissionsResponse,
        b: StateRolePermissionsResponse
      ) => {
        return a.role.localeCompare(b.role);
      },
    },
    {
      title: "Routes",
      dataIndex: "routes",
      key: "routes",
      width: 300,
      ...getColumnSearchProps("routes"),
      render: (text: Record<string, string>) => {
        return {
          props: {
            style: {
              whiteSpace: "pre",
            },
          },
          children: formatRoutes(text),
        };
      },
    },
    {
      title: titleWithInfoTooltip(
        "Feature variants",
        "Note: active times are in *your* time zone"
      ),
      dataIndex: "featureVariants",
      key: "featureVariants",
      width: 350,
      ...getColumnSearchProps("featureVariants"),
      render: (text: Record<string, string>) => {
        return {
          props: {
            style: {
              whiteSpace: "pre",
            },
          },
          children: formatFeatureVariants(text),
        };
      },
    },
  ];
};

export const aggregateFormPermissionResults = (
  formResults: Partial<StateUserForm>
): {
  routes: Partial<Routes>;
  featureVariantsToAdd: Partial<FeatureVariants>;
  featureVariantsToRemove: string[];
} => {
  const routes = Object.fromEntries(
    Object.keys(ROUTES_PERMISSIONS_LABELS)
      .filter((key) => key in formResults)
      .map((key) => [key, formResults[key as keyof StateUserForm]])
  ) as Partial<Routes>;
  const featureVariantsToAdd =
    formResults.featureVariant?.reduce<Partial<FeatureVariants>>(
      (fvsToAdd, fv) => {
        if (fv.enabled) {
          return {
            ...fvsToAdd,
            [fv.name]: {
              ...(fv.activeDate && { activeDate: new Date(fv.activeDate) }),
            },
          };
        }
        return fvsToAdd;
      },
      {}
    ) ?? {};
  const featureVariantsToRemove =
    formResults.featureVariant?.reduce<string[]>((fvsToRemove, fv) => {
      if (fv.name && fv.enabled === false) {
        fvsToRemove.push(fv.name);
      }
      return fvsToRemove;
    }, []) ?? [];
  return { routes, featureVariantsToAdd, featureVariantsToRemove };
};

export const formatRoutes = (
  text: Record<string, unknown | boolean>
): string => {
  if (!text) return "";
  return Object.keys(text)
    .sort()
    .filter((route) => !!text[route])
    .join("\n");
};

export const formatFeatureVariants = (
  text: Record<string, string | boolean>
): string => {
  if (!text) return "";
  return Object.keys(text)
    .sort()
    .filter((fvName) => !!text[fvName])
    .map((fvName) => {
      const rawVariant = text[fvName];
      const variant =
        typeof rawVariant === "string" ? JSON.parse(rawVariant) : rawVariant;
      const activeDate = variant?.activeDate && new Date(variant.activeDate);
      const now = new Date();

      return !activeDate || activeDate < now
        ? fvName
        : `${fvName}: ${moment(activeDate).format("lll")}`;
    })
    .join("\n");
};

export const getStateRoles = (
  stateCodes: string[],
  stateRoleData: StateRolePermissionsResponse[]
): string[] => {
  const permissionsForState = stateRoleData?.filter((d) =>
    stateCodes.includes(d.stateCode)
  );
  const rolesForState = permissionsForState?.map((p) => p.role);

  return rolesForState;
};

export const getStateDistricts = (
  stateCodes: string[],
  userData: StateUserPermissionsResponse[]
): string[] => {
  const usersForState = userData?.filter((d) =>
    stateCodes.includes(d.stateCode)
  );
  const districtsForState = usersForState
    ?.map((u) => u.district)
    .filter((v, i, a) => a.indexOf(v) === i && v);

  return districtsForState;
};

const sortOnBlockStatus = (record: StateUserPermissionsResponse) => {
  const now = new Date();
  const blockedOn = record.blockedOn ? new Date(record.blockedOn) : null;

  if (!blockedOn) return 1;
  if (blockedOn <= now) return 2;
  return 3;
};

export const isDateInPast = (date: string): boolean => {
  return new Date(date) < new Date();
};
