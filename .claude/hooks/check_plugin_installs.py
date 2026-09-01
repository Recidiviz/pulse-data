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
"""SessionStart hook that warns when an enabled Claude Code plugin is not installed.

The `enabledPlugins` map in `.claude/settings.json` does not install plugins; it
only activates plugins a developer already installed with
`claude plugin install <name>@<marketplace>`. When an enabled plugin has no
install record, its skills and hooks silently do not load. This hook compares
the enabled set against `~/.claude/plugins/installed_plugins.json` and prints
the install command for each missing plugin.

This script runs outside the `recidiviz` package, so it uses only the standard
library.
"""

import json
import os
import sys
from pathlib import Path


def _project_dir() -> Path:
    """Returns the repo root, from $CLAUDE_PROJECT_DIR when the harness sets it."""
    env_dir = os.environ.get("CLAUDE_PROJECT_DIR")
    if env_dir:
        return Path(env_dir)
    return Path(__file__).resolve().parent.parent.parent


def _enabled_plugins(project_dir: Path) -> set[str]:
    """Returns plugins enabled for this repo, with local overrides applied."""
    enabled: dict[str, bool] = {}
    for settings_name in ("settings.json", "settings.local.json"):
        settings_path = project_dir / ".claude" / settings_name
        if not settings_path.exists():
            continue
        settings = json.loads(settings_path.read_text())
        # `enabledPlugins` is an optional settings key.
        enabled.update(settings.get("enabledPlugins", {}))
    return {name for name, is_enabled in enabled.items() if is_enabled}


def _installed_plugins() -> set[str]:
    """Returns plugins with an install record on this machine."""
    installed_path = Path.home() / ".claude" / "plugins" / "installed_plugins.json"
    if not installed_path.exists():
        return set()
    installed = json.loads(installed_path.read_text())
    # `plugins` maps plugin name to a list of per-scope install records.
    plugin_records: dict[str, list[dict[str, str]]] = installed.get("plugins", {})
    return {name for name, records in plugin_records.items() if records}


def main() -> None:
    """Prints a warning for each enabled plugin that has no install record.

    The warning goes out twice: as `systemMessage`, which the terminal shows
    directly to the user at session start, and as `additionalContext`, which
    lands in Claude's context so Claude can repeat the fix.
    """
    missing_plugins = sorted(_enabled_plugins(_project_dir()) - _installed_plugins())
    if not missing_plugins:
        return
    warnings = "\n".join(
        f"WARNING: plugin [{plugin_name}] is enabled in .claude/settings.json "
        f"but is not installed on this machine, so its skills and hooks will "
        f"not load. To fix, run: claude plugin install {plugin_name} "
        f"--scope project"
        for plugin_name in missing_plugins
    )
    print(
        json.dumps(
            {
                "systemMessage": warnings,
                "hookSpecificOutput": {
                    "hookEventName": "SessionStart",
                    "additionalContext": (
                        f"{warnings}\n"
                        f"Tell the user about the warnings above at the start "
                        f"of your next reply."
                    ),
                },
            }
        )
    )


if __name__ == "__main__":
    sys.exit(main())
