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

// According to https://github.com/microsoft/TypeScript/issues/33128#issuecomment-748937504,
// this line is needed in order to turn this from a script into a module in order
// to allow the interface definition to be extended.
export {};

declare global {
  interface Window {
    // values added from `app_public_config.js`
    APP_CONFIG: Record<string, string>;
    SEGMENT_KEY: string;

    // Segment's analytics.js
    analytics: SegmentAnalytics.AnalyticsJS;
  }
}
