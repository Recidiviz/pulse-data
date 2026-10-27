# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Formats the zigquery emulator's /debug/stats response as a human-readable
summary."""
from typing import Any

_MAX_SQL_CHARS = 100


def _kib(num_bytes: int) -> str:
    return f"{num_bytes / 1024:.1f}KiB"


def _one_line_sql(sql: str | list[int]) -> str:
    if isinstance(sql, list):
        # zigquery truncates slow-query SQL by byte count, which can split a
        # multi-byte UTF-8 character, and Zig's std.json serializes the resulting
        # invalid UTF-8 as an array of byte values instead of a string.
        sql = bytes(sql).decode("utf-8", errors="replace")
    collapsed = " ".join(sql.split())
    if len(collapsed) <= _MAX_SQL_CHARS:
        return collapsed
    return collapsed[: _MAX_SQL_CHARS - 3] + "..."


def format_emulator_stats(stats: dict[str, Any]) -> str:
    """Returns a multi-line summary of the zigquery emulator's /debug/stats
    response, with routes sorted by total time spent."""
    contention = stats["contention"]
    phases = stats["phases"]
    sqlite = phases["sqlite"]
    alloc = stats["sqliteAlloc"]

    lines = [
        f"Requests: {contention['requests']} | "
        f"handle {contention['totalHandleMs']:.1f}ms total "
        f"({contention['meanHandleMs']:.3f}ms mean) | "
        f"lock wait {contention['totalWaitMs']:.1f}ms "
        f"({contention['waitFraction']:.1%}), max waiters {contention['maxWaiters']}",
        f"SQLite memory: schema {_kib(alloc['schemaBytes'])} | "
        f"statements {_kib(alloc['stmtBytes'])} | "
        f"pager cache {_kib(alloc['pagerCacheBytes'])} | "
        f"lookaside hit rate {alloc['lookaside']['hitRate']:.1%}",
        "",
        f"{'Phase':<28}{'Count':>7}{'Total ms':>10}{'% handle':>10}",
    ]
    # (name, total ms, count or None, fraction of handle time or None)
    phase_rows: list[tuple[str, float, int | None, float | None]] = [
        (
            "analyze",
            phases["analyzeMs"],
            phases["analyzeCount"],
            phases["analysisFraction"],
        ),
        ("transform", phases["transformMs"], phases["transformCount"], None),
        ("sqlite", phases["sqliteMs"], None, phases["sqliteFraction"]),
        ("  prepare", sqlite["prepareMs"], sqlite["prepareCount"], None),
        ("  step", sqlite["stepMs"], sqlite["stepCount"], None),
        ("  exec", sqlite["execMs"], sqlite["execCount"], None),
        ("  catalog", sqlite["catalogMs"], None, None),
        ("codec", phases["codecMs"], phases["codecCount"], None),
        ("view refresh", phases["viewRefreshMs"], phases["viewRefreshCount"], None),
    ]
    for name, total_ms, count, fraction in phase_rows:
        count_str = "" if count is None else str(count)
        fraction_str = "" if fraction is None else f"{fraction:.1%}"
        lines.append(
            f"{name:<28}{count_str:>7}{total_ms:>10.1f}{fraction_str:>10}".rstrip()
        )

    lines.extend(
        [
            "",
            f"{'Route':<28}{'Count':>7}{'Total ms':>10}{'Mean':>9}"
            f"{'p50':>9}{'p90':>9}{'p99':>9}{'Max':>9}",
        ]
    )
    for route in sorted(stats["routes"], key=lambda r: r["totalMs"], reverse=True):
        lines.append(
            f"{route['route']:<28}{route['count']:>7}{route['totalMs']:>10.1f}"
            f"{route['meanMs']:>9.3f}{route['p50Ms']:>9.3f}{route['p90Ms']:>9.3f}"
            f"{route['p99Ms']:>9.3f}{route['maxMs']:>9.3f}"
        )

    lines.extend(["", "Slowest queries:"])
    for i, query in enumerate(stats["slowQueries"], 1):
        lines.append(f"{i:>3}. {query['ms']:>9.3f}ms  {_one_line_sql(query['sql'])}")

    return "\n".join(lines)
