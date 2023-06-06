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

// algorithm and implementation adapted from
// https://observablehq.com/@yurivish/building-a-better-beeswarm

import { ascending } from "d3-array";

type InputPoint<D> = { value: number } & D;

type SwarmInput<D> = {
  data: InputPoint<D>[];
  valueScale: (d: number) => number;
  radius: number;
  spread?: number;
  minSpread?: number;
};

export type SwarmPoint<D> = {
  position: number;
  offset: number;
  datum: InputPoint<D>;
};

type SwarmedData<D> = {
  swarmPoints: SwarmPoint<D>[];
  swarmSpread: number;
};

/**
 * returns 0 if even, 1 if odd
 */
const oddEvenScale = (n: number) => n % 2;

const { sqrt, abs } = Math;

/**
 * Transforms input data into SwarmPoint objects and calculates positions
 */
function initializeSwarm<D>(valueScale: (n: number) => number) {
  return function swarmPointInitializer(d: InputPoint<D>) {
    return {
      datum: d,
      position: valueScale(d.value),
      // this is the default value, treated as the vertical center of the swarm;
      // it may be replaced to prevent overlapping circles
      offset: 0,
    };
  };
}

function sortSwarm<D>(a: SwarmPoint<D>, b: SwarmPoint<D>) {
  return ascending(a.position, b.position);
}

/**
 * This creates the spread placements that will actually create the swarm layout,
 * by placing circles in order and adjusting them based on what has already been placed
 */
function calculatePlacements<D>(radius: number) {
  return function placementCalculator(
    placedCircles: SwarmPoint<D>[],
    currentCircle: SwarmPoint<D>
  ) {
    const requiredDistanceBetweenCenters = radius * 2;

    // this will collect the spread intervals between which this circle will overlap with others
    const intervals: [number, number][] = [];

    // scan circles that have already been placed,
    // starting with the closest to current for efficiency
    for (let i = placedCircles.length - 1; i >= 0; i -= 1) {
      const otherCircle = placedCircles[i];
      const distanceBetweenCenters = abs(
        currentCircle.position - otherCircle.position
      );
      // stop scanning once it becomes clear that no circle can overlap the current one
      if (distanceBetweenCenters > requiredDistanceBetweenCenters) break;

      // compute the distance by which one would need to offset the circle along the spread axis
      // so that it just touches the other circle (modeled as one side of a right triangle
      // where the hypotenuse connects the centers of the two circles)
      const offset = sqrt(
        requiredDistanceBetweenCenters * requiredDistanceBetweenCenters -
          distanceBetweenCenters * distanceBetweenCenters
      );
      // use that offset to create an interval within which this circle is forbidden
      intervals.push([
        otherCircle.offset - offset,
        otherCircle.offset + offset,
      ]);
    }

    // Find an offset coordinate for this circle by finding
    // the lowest point at the edge of any interval where it can fit.
    // This is quadratic in the number of intervals, but runs fast in practice due to
    // fact that we stop once the first acceptable candidate is found.
    const offset =
      intervals
        .flat()
        // sorting by absolute values to find the one closest to zero first
        .sort((a, b) => ascending(abs(a), abs(b)))
        .find((candidate) =>
          intervals.every(([lo, hi]) => candidate <= lo || candidate >= hi)
        ) ??
      // if there weren't any overlaps, retain the default offset
      currentCircle.offset;

    return [...placedCircles, { ...currentCircle, offset }];
  };
}

/**
 * Because it's possible for circles to overflow the available spread,
 * this repositions them to overlap within that space rather than overflowing
 */
function handleOverflowingCircles<D>(swarmSpread: number, radius: number) {
  return function overflowHandler(point: SwarmPoint<D>) {
    const { offset } = point;
    // if 0 is the center, each side is half the spread (minus padding so the entire circle fits inside)
    const sideWidth = swarmSpread / 2 - radius;

    // apply a slight jitter when overlapping to limit 100% overlapping circles (which are harder to see),
    // based on how many layers of overlapping circles there are
    const overlapIndex = Math[offset > 0 ? "floor" : "ceil"](
      offset / sideWidth
    );
    const offsetMultiplier = overlapIndex ? oddEvenScale(overlapIndex) : 0;
    const overlapOffset = offsetMultiplier * radius * (offset >= 0 ? 1 : -1);

    // use the remainder to cycle through the available space as many times as needed,
    // layering on more points at each pass
    const finalOffset = (offset + overlapOffset) % sideWidth;

    return { ...point, offset: finalOffset };
  };
}

/**
 * Calculates and returns the coordinates of a swarm of circles representing the given data.
 * Coordinates are plotted along two axes:
 * - the "position" axis, which transforms the value according to the provided scale function
 * - the "offset" axis, which is centered on 0 and offsets the circles to limit overlaps
 *   without distorting the values
 *
 * These axes can be used interchangeably as x or y values.
 */
export function calculateSwarm<D = unknown>({
  data,
  valueScale,
  radius,
  spread,
  minSpread,
}: SwarmInput<D>): SwarmedData<D> {
  const swarmPoints: SwarmPoint<D>[] = data
    .map(initializeSwarm(valueScale))
    // points will be placed in this order
    .sort(sortSwarm)
    .reduce(calculatePlacements(radius), []);

  const swarmSpread = Math.max(
    spread ??
      // if fixed spread is not provided, calculate the size needed to fit all points
      2 *
        (radius +
          // extra padding so all the circles fit inside the band
          1 +
          // find the farthest distance from any circle to the center
          Math.max(
            ...swarmPoints
              // zero is a special case because many rates skew towards zero
              // and there is a higher risk of carceral antipatterns around zero rates.
              // So what we actually want is for the chart to fit the widest point where
              // the value is NOT zero; the swarm can overflow its boundaries at zero if applicable,
              // we will handle that below
              .filter((d) => d.datum.value !== 0)
              .map((d) => Math.abs(d.offset))
          )),
    minSpread ?? 0
  );

  return {
    swarmSpread,
    swarmPoints: swarmPoints.map(handleOverflowingCircles(swarmSpread, radius)),
  };
}
