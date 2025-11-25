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
import { isFuture, isPast } from "date-fns";
import moment from "moment";

import {
  AllowedAppRecord,
  AllowedApps,
  FeatureVariantRecord,
  FeatureVariants,
  FeatureVariantValue,
  Route,
  RouteRecord,
  Routes,
  StateRolePermissionsResponse,
  StateUserForm,
  StateUserPermissionsResponse,
} from "../../types";
import { ALLOWED_APPS_LABELS, ROUTES_PERMISSIONS_LABELS } from "../constants";

// Given a record T, finds all keys of T whose values extend V
export type KeysMatching<T, V> = {
  [K in keyof T]-?: T[K] extends V ? K : never;
}[keyof T];

export function updatePermissionsObject<
  Permissions extends Routes | FeatureVariants | AllowedApps
>(
  existing: Partial<Permissions>,
  updated: Partial<Permissions>,
  remove: string[]
): Partial<Permissions> | undefined {
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
    // @ts-expect-error TS2322 we know every value in this object is a boolean
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
  if (!!record.blockedOn && isPast(record.blockedOn)) {
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
    const valueAsStr = value.toString().toLowerCase();

    if (!item) return false;

    if (typeof item !== "object") {
      return item.toString().toLowerCase().includes(valueAsStr);
    }

    return Object.entries(item).some(
      ([itemKey, itemValue]) =>
        itemKey.toLowerCase().includes(valueAsStr) && itemValue !== false
    );
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
      title: titleWithInfoTooltip(
        "Allowed Apps (In Development)",
        "Top-level access-control for each of our apps. NOTE: This feature is still in development and currently does nothing."
      ),
      dataIndex: "allowedApps",
      key: "allowedApps",
      width: 300,
      ...getColumnSearchProps("allowedApps"),
      render: (text: Record<string, string>) => {
        return {
          props: {
            style: {
              whiteSpace: "pre",
            },
          },
          children: formatPermission(text, ", "),
        };
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
        const allRoutes = stateRoleData
          .filter(
            (r) =>
              r.stateCode === record.stateCode && record.roles?.includes(r.role)
          )
          .map((r) => r.routes);
        const mergedRoutes = mergeRoutes(allRoutes);
        return {
          props: {
            style: {
              whiteSpace: "pre",
            },
          },
          children: getHighlightedPermissions(
            mergedRoutes,
            record.routes ?? {}
          ),
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
        const allFvs = stateRoleData
          .filter(
            (r) =>
              r.stateCode === record.stateCode && record.roles?.includes(r.role)
          )
          .map((r) => r.featureVariants);
        const mergedFvs = mergeFeatureVariants(allFvs);
        return {
          props: {
            style: {
              whiteSpace: "pre",
            },
          },
          children: getHighlightedPermissions(
            mergedFvs,
            record.featureVariants ?? {}
          ),
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
      title: titleWithInfoTooltip(
        "Allowed Apps (In Development)",
        "Top-level access-control for each of our apps. NOTE: This feature is still in development and currently does nothing."
      ),
      dataIndex: "allowedApps",
      key: "allowedApps",
      width: 300,
      ...getColumnSearchProps("allowedApps"),
      render: (text: Record<string, string>) => {
        return {
          props: {
            style: {
              whiteSpace: "pre",
            },
          },
          children: formatPermission(text, ", "),
        };
      },
    },
    {
      title: titleWithInfoTooltip(
        "Dashboard Routes",
        "Controls access to sections of dashboard.recidiviz.org"
      ),
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
          children: formatPermission(text),
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
          children: formatPermission(text),
        };
      },
    },
    {
      title: titleWithInfoTooltip(
        "JII Permissions",
        "Controls access to features with opportunities.app"
      ),
      dataIndex: "jiiPermissions",
      key: "jiiPermissions",
      width: 300,
      ...getColumnSearchProps("jiiPermissions"),
      render: (text: Record<string, string>) => {
        return {
          props: {
            style: {
              whiteSpace: "pre",
            },
          },
          children: formatPermission(text),
        };
      },
    },
  ];
};

function extractBooleanFields<
  Label extends KeysMatching<StateUserForm, boolean>
>(
  formResults: Partial<StateUserForm>,
  permissionLabels: Record<Label, string>
): Partial<Record<Label, boolean>> {
  // formResults contains results that aren't just booleans, but the typing should
  // ensure that this will only be called for entries with boolean outputs
  // @ts-expect-error TS2322
  return Object.fromEntries(
    Object.keys(permissionLabels)
      .filter((key) => key in formResults)
      .map((key) => [key, formResults[key as keyof StateUserForm]])
  );
}

export const aggregateFormPermissionResults = (
  formResults: Partial<StateUserForm>
): {
  routes: RouteRecord;
  featureVariantsToAdd: FeatureVariantRecord;
  featureVariantsToRemove: string[];
  allowedApps: AllowedAppRecord;
} => {
  const routes = extractBooleanFields(formResults, ROUTES_PERMISSIONS_LABELS);
  const allowedApps = extractBooleanFields(formResults, ALLOWED_APPS_LABELS);

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

  return { routes, featureVariantsToAdd, featureVariantsToRemove, allowedApps };
};

export const formatPermission = (
  text: Record<string, string>,
  joiner = "\n"
): string => {
  if (!text) return "";
  return Object.keys(text)
    .sort()
    .filter((permission) => !!text[permission])
    .map((permission) => {
      const value =
        typeof text[permission] === "string"
          ? JSON.parse(text[permission])
          : text[permission];
      const activeDate = value?.activeDate && new Date(value.activeDate);
      const now = new Date();

      return !activeDate || activeDate < now
        ? permission
        : `${permission}: ${moment(activeDate).format("lll")}`;
    })
    .join(joiner);
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

/**
 * Reconciles/prioritizes routes if user has multiple roles. For routes, if any
 * of a user's roles grant permission to a route, ignore/override when any of their
 * other roles don't grant that permission.
 */
export const mergeRoutes = (permissionsList: RouteRecord[]): RouteRecord => {
  /* eslint-disable no-param-reassign -- reduce() requires modifying the accumulator directly */
  return permissionsList.reduce((newPermissions, permission) => {
    Object.entries(permission).forEach(([key, value]) => {
      const existingValue = newPermissions[key as Route];

      if (!existingValue) {
        newPermissions[key as Route] = value;
      }
    });
    return newPermissions;
  }, {} as RouteRecord);
};

/**
 * Reconciles/prioritizes feature variants if user has multiple roles.
 * For feature variants, when comparing different permissions for the same variant,
 * order of priority is:
 *     1) always on (value of variant will not have an activeDate attribute)
 *     2) earliest active date
 *     3) false
 */
export const mergeFeatureVariants = (
  permissionsList: FeatureVariants[]
): FeatureVariantRecord => {
  /* eslint-disable no-param-reassign -- reduce() requires modifying the accumulator directly */
  return permissionsList.reduce((newPermissions, permission) => {
    Object.entries(permission).forEach(([key, value]) => {
      const existingValue = newPermissions[key];

      if (!existingValue) {
        newPermissions[key] = value;
      } else if (
        typeof existingValue === "object" &&
        "activeDate" in existingValue
      ) {
        if (typeof value === "object" && "activeDate" in value) {
          newPermissions[key] = {
            activeDate: new Date(
              Math.min(
                new Date(existingValue.activeDate as Date).getTime(),
                new Date(value.activeDate as Date).getTime()
              )
            ),
          };
        } else if (value !== false) {
          newPermissions[key] = value;
        }
      }
    });
    return newPermissions;
  }, {} as FeatureVariantRecord);
};

/**
 * Checks for equivalence between merged default permissions and merged default +
 * override permissions (i.e., user permissions). Equivalence is defined as the permission
 * being currently enabled/disabled for both default and user permissions, regardless
 * of the exact values.
 * - Routes
 *   - If `defaultPermissions` is `true`, it is always equivalent.
 *   - If `defaultPermissions` is `false`, it is equivalent if `userPermissions` is also `false`.
 *   - Otherwise, it is not equivalent.
 *
 * - Feature variants
 *   - If `defaultPermissions` is always on (empty object), it is always equivalent.
 *   - If `defaultPermissions` is a past date, it is equivalent if:
 *     - `userPermissions` is always on, or
 *     - `userPermissions` is also a past date.
 *   - If `defaultPermissions` is a future date, it is equivalent if:
 *     - `userPermissions` is also a future date.
 *   - If `defaultPermissions` is a future date it is not equivalent if:
 *     - `userPermissions` is always on, or
 *     - `userPermissions` is a past date.
 */
export const checkPermissionsEquivalence = (
  defaultPermissions: FeatureVariantValue | boolean,
  userPermissions: FeatureVariantValue | boolean
): boolean => {
  if (typeof defaultPermissions === "object") {
    if (!Object.keys(defaultPermissions).length) return true;

    if ("activeDate" in defaultPermissions && defaultPermissions.activeDate) {
      if (isPast(defaultPermissions.activeDate)) {
        return true;
      }
      if (
        typeof userPermissions === "object" &&
        "activeDate" in userPermissions &&
        userPermissions.activeDate
      ) {
        return (
          isFuture(defaultPermissions.activeDate) &&
          isFuture(userPermissions.activeDate)
        );
      }
      return false;
    }
  }
  if (defaultPermissions) return true;

  return userPermissions === false;
};

const getHighlightedPermissions = (
  defaultPermissions: FeatureVariantRecord | RouteRecord,
  userPermissions: FeatureVariantRecord | RouteRecord
) => {
  const keys = Array.from(
    new Set([
      ...Object.keys(defaultPermissions),
      ...Object.keys(userPermissions),
    ])
  );

  return keys
    .filter((key) => !!userPermissions[key as keyof typeof userPermissions])
    .map((key) => {
      const defaultValue =
        defaultPermissions[key as keyof typeof defaultPermissions] ?? false;
      const userValue =
        userPermissions[key as keyof typeof userPermissions] ?? false;
      return (
        <div
          key={key}
          style={{
            background: !checkPermissionsEquivalence(defaultValue, userValue)
              ? "yellow"
              : "none",
            padding: "2px",
          }}
        >
          {formatPermission({
            [key]: JSON.stringify(userValue),
          })}
        </div>
      );
    });
};
