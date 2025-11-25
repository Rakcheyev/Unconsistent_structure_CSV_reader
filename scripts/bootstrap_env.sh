#!/usr/bin/env bash
set -euo pipefail

DEV=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dev)
      DEV=true
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PATH="${REPO_ROOT}/../.venv"

if [[ ! -d "${VENV_PATH}" ]]; then
  echo "Creating virtual environment at ${VENV_PATH}"
  python -m venv "${VENV_PATH}"
fi

# shellcheck disable=SC1090
source "${VENV_PATH}/bin/activate"

python -m pip install --upgrade pip setuptools wheel

INSTALL_TARGET="."
if [[ "${DEV}" == true ]]; then
  INSTALL_TARGET=".[dev]"
fi

echo "Installing project (${INSTALL_TARGET})"
pip install "${INSTALL_TARGET}"

echo "Environment ready. Activate with: source ${VENV_PATH}/bin/activate"
