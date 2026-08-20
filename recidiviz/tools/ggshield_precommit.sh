#!/usr/bin/env bash
#
# Blocks a commit that contains a secret, without blocking a commit that merely
# lacks a GitGuardian token.
#
# WHY THIS WRAPPER EXISTS INSTEAD OF ggshield's OWN pre-commit HOOK
#   The upstream hook (repo: github.com/gitguardian/ggshield) makes pre-commit
#   build its own ggshield venv, so EVERY contributor gets the binary whether or
#   not they installed it. Detection is server-side, so a contributor without a
#   token then gets `Error: No token is saved for this instance` and exit 3 - on
#   every commit, including clean ones. That is a hard block on everyone who is
#   not on a managed Mac.
#
#   On macOS it is worse than a failed commit: a tokenless run makes ggshield
#   consult python-keyring, which raises a "Keychain Not Found" system dialog in
#   front of the user. Checking for a token first means ggshield is never invoked
#   without credentials, so neither the exit 3 nor the dialog can happen.
#
# WHY PYTHON_KEYRING_BACKEND IS PINNED TO null
#   Belt and braces for that same dialog. ggshield only consults the keyring to
#   find a stored token; we always supply one via the environment or
#   auth_config.yaml, so denying it the keyring costs nothing and guarantees no
#   GUI prompt from a git hook - including when HOME is unusual, which is where
#   the prompt appeared in testing.
#
# TOKEN LOCATIONS
#   $GITGUARDIAN_API_KEY, or the auth_config.yaml written by `ggshield auth
#   login`. macOS does NOT put that under ~/.config - it lives in Application
#   Support. Recidiviz Macs have it written by Jamf; both paths are checked so a
#   manually authenticated Linux or CI contributor works too.
#
#   GitGuardian's API is stateless for this call: scanned content is not stored,
#   only metadata such as call time and request size.
#
# FUTURE: once every contributor's machine has ggshield and a token, turn these
# warnings into hard failures so an unscanned commit becomes impossible.

set -uo pipefail

gg_auth_macos="$HOME/Library/Application Support/ggshield/auth_config.yaml"
gg_auth_xdg="${XDG_CONFIG_HOME:-$HOME/.config}/ggshield/auth_config.yaml"

if ! command -v ggshield >/dev/null 2>&1; then
    echo "WARNING: ggshield not found - this commit was NOT scanned for secrets."
    echo "         See #security if you expected it to be installed."
    exit 0
fi

if [ -z "${GITGUARDIAN_API_KEY:-}" ] &&
    [ ! -s "$gg_auth_macos" ] &&
    [ ! -s "$gg_auth_xdg" ]; then
    echo "WARNING: no GitGuardian token - this commit was NOT scanned for secrets."
    echo "         See #security if you expected your machine to have one."
    exit 0
fi

PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring \
    exec ggshield secret scan pre-commit
