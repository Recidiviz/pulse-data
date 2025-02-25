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

import styled from "styled-components";
import { describe, expect, test } from "vitest";

import {
  computeTextWidth,
  convertRemToPx,
  renderToStaticSvg,
  wrapText,
} from "./utils";

describe("convertRemToPx", () => {
  test("converts a property value", () => {
    expect(
      convertRemToPx(`<style>.abc123{font-size: 1.5rem;}</style>`)
    ).toMatchInlineSnapshot('"<style>.abc123{font-size: 24px;}</style>"');
  });
  test("converts all property values", () => {
    expect(
      convertRemToPx(
        `<style>.abc123{font-size: 1.5rem; padding: 0.4rem;}.xyz789{height:4rem;width: 8rem;}</style>`
      )
    ).toMatchInlineSnapshot(
      '"<style>.abc123{font-size: 24px; padding: 6.4px;}.xyz789{height:64px;width: 128px;}</style>"'
    );
  });
  test("converts part of a value", () => {
    expect(
      convertRemToPx(
        `<style>.abc123{font:500 1rem/1.2 "Public Sans",sans-serif;padding:5px 1rem 0.5em;}</style>`
      )
    ).toMatchInlineSnapshot(
      '"<style>.abc123{font:500 16px/1.2 \\"Public Sans\\",sans-serif;padding:5px 16px 0.5em;}</style>"'
    );
  });
});

describe("renderToStaticSvg", () => {
  test("renders the entire component tree to a string", () => {
    function TestCmp() {
      return (
        <svg>
          <rect width="10" height={20} x={5} y={5} />
          <text>chart goes here</text>
        </svg>
      );
    }

    expect(renderToStaticSvg(TestCmp)).toMatchFileSnapshot(
      "./__snapshots__/renderToStaticSvg"
    );
  });

  test("expects an SVG", () => {
    function NotAnSvg() {
      return <div>chart goes here</div>;
    }

    expect(() => renderToStaticSvg(NotAnSvg)).toThrow();
  });

  test("injects styles from Styled Components", () => {
    const StyledEl = styled.circle`
      fill: blue;
    `;
    function TestCmp() {
      return (
        <svg>
          <StyledEl />
        </svg>
      );
    }

    const rendered = renderToStaticSvg(TestCmp);

    expect(rendered).toMatch(
      // a bunch of wildcards here because SC hashes the classname
      // and also injects a bunch of irrelevant attributes and comments;
      // the s flag is necessary because that may include newlines
      /<defs><style.*?>.*{fill:blue;}.*<\/style><\/defs>/s
    );
  });
});

test("computeTextWidth", () => {
  // this snapshot was verified to match Chrome rendering in Storybook
  expect(computeTextWidth("The quick brown fox", 18)).toMatchInlineSnapshot(`
    {
      "cumulativeWidths": [
        10.431000000000001,
        20.529,
        30.320999999999998,
        34.047,
        44.46,
        54.72,
        58.905,
        68.247,
        78.08399999999999,
        82.008,
        92.44799999999998,
        99.11699999999998,
        108.82799999999996,
        122.54399999999997,
        132.65099999999995,
        136.57499999999996,
        143.24399999999997,
        152.45099999999996,
        162.36899999999997,
      ],
      "totalWidth": 162.36899999999997,
    }
  `);
});

describe("wrapText", () => {
  test("fits on one line", () => {
    expect(wrapText("Riley, Dennis F", 125, 16)).toEqual(["Riley, Dennis F"]);
  });

  test("wraps to two lines", () => {
    expect(wrapText("Holmes-Briggs, Leonard", 125, 16)).toEqual([
      "Holmes-Briggs,",
      "Leonard",
    ]);
  });

  test("wraps at hyphens", () => {
    expect(wrapText("Leonard Holmes-Briggs", 130, 16)).toEqual([
      "Leonard Holmes-",
      "Briggs",
    ]);
  });

  test("wraps to three lines", () => {
    expect(wrapText("Holmes-Briggs, Caroline Gertrude", 125, 16)).toEqual([
      "Holmes-Briggs,",
      "Caroline",
      "Gertrude",
    ]);
  });
});
