#!/usr/bin/env bash

set -eo pipefail

NEW_VERSION="${1}"
PREV_LATEST="$(mike list --json | jq --raw-output '.[] | select(.aliases == ["latest"]) | .version')"

if [[ "${PREV_LATEST}" == "" ]]; then
  echo "No previous version found using the latest alias. Nothing to re-title."
elif [[ "${IS_PRERELEASE}" != "true" ]]; then
  echo "mike retitle --message \"Remove latest from title of ${PREV_LATEST}\" \"${PREV_LATEST}\" \"${PREV_LATEST}\""
  mike retitle --message "Remove latest from title of ${PREV_LATEST}" "${PREV_LATEST}" "${PREV_LATEST}"
fi

if [[ "${IS_PRERELEASE}" == "true" ]]; then
  echo "Pre-release version. This release will NOT be given the \"latest\" alias."
  echo "mike deploy --title \"${NEW_VERSION}\" \"${NEW_VERSION}\""
  mike deploy --title "${NEW_VERSION}" "${NEW_VERSION}"
else
  echo "mike deploy --update-aliases --title \"${NEW_VERSION} (latest)\" \"${NEW_VERSION}\" \"latest\""
  mike deploy --update-aliases --title "${NEW_VERSION} (latest)" "${NEW_VERSION}" "latest"
fi