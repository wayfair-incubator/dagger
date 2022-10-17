#!/usr/bin/env bash

set -eo pipefail

DEFAULT_BRANCH="main"
PRE_RELEASE_ARG="pre_release"

DEV_RELEASE="false"
PART=""

function usage() {
  echo "usage: bump_version.sh [--dev] pre-release|major|minor|patch"
  echo ""
  echo " --dev             : Update to a dev pre-release for the new version. Can't be used with pre-release."
  echo " pre-release       : Update pre-release component for the current version."
  echo " major|minor|patch : Part of version string to updated for a final release."
  exit 1
}

while [[ $# -gt 0 ]]; do
  arg="$1"
  case $arg in
  --dev)
    DEV_RELEASE="true"
    ;;
  -h | --help)
    usage
    ;;
  pre-release | major | minor | patch)
    if [[ "${PART}" != "" ]]; then
      echo "Part has already been given"
      usage
    fi
    PART="${arg}"
    if [[ "${PART}" == "pre-release" ]]; then
      # part names can't have dashes, but dashes are better for a CLI
      PART="${PRE_RELEASE_ARG}"
    fi
    ;;
  "")
    # ignore
    ;;
  *)
    echo "Unexpected argument: ${arg}"
    usage
    ;;
  esac
  shift
done

if [[ "${PART}" == "" ]]; then
  echo "Part to update not given"
  usage
elif [[ "${PART}" == "${PRE_RELEASE_ARG}" && "${DEV_RELEASE}" == "true" ]]; then
  echo "pre-release and --dev can not be used at the same time"
  usage
fi

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
CURRENT_STATUS=$(git status --short --untracked-files=no)

if [[ "${CURRENT_BRANCH}" != "${DEFAULT_BRANCH}" ]]; then
  echo "A version bump must be run from the default branch."
  echo "Run 'git switch ${DEFAULT_BRANCH}'"
  exit 2
elif [[ "$CURRENT_STATUS" != "" ]]; then
  echo "The working tree has uncommitted changes."
  echo "Commit or stash the changes before running a version bump."
  exit 3
fi

# Capture value of new version
NEW_VERSION=$(bump2version --dry-run --list "${PART}" | grep new_version | sed -r s,"^.*=",,)
# bump2version currently makes it hard increment a part and start in a dev state
# https://github.com/c4urself/bump2version/issues/63
if [[ "${DEV_RELEASE}" == "true" ]]; then
  NEW_VERSION="${NEW_VERSION}-dev"
fi

# Create bump branch
BUMP_BRANCH_NAME="bump_version_to_${NEW_VERSION}"
echo "Creating new branch ${BUMP_BRANCH_NAME}"
echo
git checkout --quiet -b "${BUMP_BRANCH_NAME}"
# Update files
bump2version --new-version "${NEW_VERSION}" "${PART}"

# Updating the changelog has to be done manually
#   - bump2version doesn't support inserting dates https://github.com/c4urself/bump2version/issues/133
#   - It is not possible to have a multiline string in an INI file where a line after the first line starts with '#'.
#     The config parser reads it as a comment line.
TODAY=$(date +%Y-%m-%d)
sed -i "s/## \[Unreleased\]/## \[Unreleased\]\n\n## \[${NEW_VERSION}\] - ${TODAY}/g" CHANGELOG.md

# Add changelog to bump commit
git add CHANGELOG.md
git commit --amend --no-edit
# Show effected files
git show --pretty="" --name-only
# Back to original branch
git checkout --quiet "${DEFAULT_BRANCH}"

echo
echo "Run 'git push --set-upstream origin ${BUMP_BRANCH_NAME}' to create a pull request"