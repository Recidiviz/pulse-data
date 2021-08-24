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
import React, { useContext, useEffect, useRef, useState } from "react";
import { useSpringRef, useTransition } from "@react-spring/web";

import { useDrag } from "@use-gesture/react";
import { Icon, IconSVG } from "@recidiviz/design-system";
import {
  CarouselContainer,
  CarouselControl,
  CarouselControlsContainer,
  CarouselIndicatorOff,
  CarouselIndicatorOn,
  CarouselItem,
} from "./Carousel.styles";

interface CarouselContextProps {
  count: number;
  index: number;
  next: () => void;
  previous: () => void;
}

export const CarouselContext = React.createContext<CarouselContextProps>({
  count: 0,
  index: 0,
  next: () => undefined,
  previous: () => undefined,
});

export interface CarouselProps {
  children: React.ReactNode[];
}

const CarouselBody: React.FC<CarouselProps> = ({ children }) => {
  const context = useContext(CarouselContext);
  const previousIndexRef = useRef<number>(0);

  const transRef = useSpringRef();
  const transitions = useTransition(context.index, {
    ref: transRef,
    keys: null,
    from: { opacity: 0 },
    enter: { opacity: 1 },
    leave: { opacity: 0 },
  });

  useEffect(() => {
    previousIndexRef.current = context.index;
  }, [context.index]);

  useEffect(() => {
    transRef.start();
  }, [context.index, transRef]);

  const bind = useDrag(({ swipe: [swipeX] }) => {
    if (swipeX === -1) context.next();
    if (swipeX === 1) context.previous();
  });

  return (
    <CarouselContainer {...bind()}>
      {transitions((style, i) => {
        return <CarouselItem style={style}>{children[i]}</CarouselItem>;
      })}
    </CarouselContainer>
  );
};

export const Carousel: React.FC<CarouselProps> = ({
  children,
}: CarouselProps) => {
  const [index, setIndex] = useState(0);
  const state = {
    count: children.length,
    index,
    next: () => {
      setIndex((index + 1) % children.length);
    },
    previous: () => {
      setIndex(index === 0 ? children.length - 1 : index - 1);
    },
  };

  useEffect(() => {
    function onKeyUp(event: KeyboardEvent) {
      if (event.key === "Left" || event.key === "ArrowLeft") {
        state.previous();
      }

      if (event.key === "Right" || event.key === "ArrowRight") {
        state.next();
      }
    }
    document.addEventListener("keyup", onKeyUp);
    return () => document.removeEventListener("keyup", onKeyUp);
  });

  return (
    <CarouselContext.Provider value={state}>
      <CarouselBody>{children}</CarouselBody>
    </CarouselContext.Provider>
  );
};

export const CarouselControls: React.FC = (
  props: React.HTMLAttributes<HTMLDivElement>
) => {
  const context = useContext(CarouselContext);

  return (
    <CarouselControlsContainer {...props}>
      <CarouselControl
        disabled={context.index === 0}
        onClick={() => context.previous()}
        aria-label="Previous Slide"
        style={{ transform: "rotate(90deg)" }}
      >
        <Icon kind={IconSVG.Caret} size={8} />
      </CarouselControl>
      <CarouselControl
        disabled={context.index === context.count - 1}
        onClick={() => context.next()}
        aria-label="Next Slide"
        style={{ transform: "rotate(-90deg)" }}
      >
        <Icon kind={IconSVG.Caret} size={8} />
      </CarouselControl>
    </CarouselControlsContainer>
  );
};

export const CarouselIndicators: React.FC = () => {
  const context = useContext(CarouselContext);
  const indicators = [];

  for (let i = 0; i < context.count; i += 1) {
    const Component =
      i === context.index ? CarouselIndicatorOn : CarouselIndicatorOff;
    indicators.push(<Component key={i} />);
  }

  return <div style={{ display: "flex" }}>{indicators}</div>;
};
