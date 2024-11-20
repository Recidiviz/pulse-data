// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

// import { OpportunityConfiguration } from "../../../WorkflowsStore/models/OpportunityConfiguration";

export const StaticValue = ({ value }: { value?: string }) => (
  <span className="ant-form-text">{value?.toString() ?? <i>Default</i>}</span>
);

// type FormSpec<T> = {
//   sectionHeading?: string;
//   sectionSubhead?: string;
//   fields: Partial<
//     Record<
//       keyof T,
//       {
//         label: string;
//         required?: boolean;
//         multiple?: boolean;
//         View?: React.FC;
//         Edit?: React.FC;
//       }
//     >
//   >;
// }[];

// const opportunityConfigFormSpec: FormSpec<OpportunityConfiguration> = [
//   {
//     fields: {
//       variantDescription: {
//         label: "Variant Description",
//         required: true,
//       },
//     },
//   },
// ];
