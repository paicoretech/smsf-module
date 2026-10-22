#!/bin/bash
#
# Entrypoint shared by the three SMSF images.
#
# Applies the environment overrides listed in conf/mapping.conf on top of the packaged
# application.properties, then hands over to start.sh. Each module ships its own mapping.conf,
# so an image only accepts the variables its module actually reads.
#
# mapping.conf format, one entry per line:
#   ENV_VAR_NAME|property.key
# Blank lines and lines starting with '#' are ignored.
#
set -euo pipefail

: "${APP_NAME:?APP_NAME is not set}"
: "${APP_HOME:?APP_HOME is not set}"

CONF_FILE="${APP_HOME}/conf/application.properties"
MAPPING_FILE="${APP_HOME}/conf/mapping.conf"

echo "Configuring ${APP_NAME}..."

if [ ! -f "${CONF_FILE}" ]; then
  echo "Error: ${CONF_FILE} not found" >&2
  exit 1
fi
if [ ! -f "${MAPPING_FILE}" ]; then
  echo "Error: ${MAPPING_FILE} not found" >&2
  exit 1
fi

# Escapes a replacement string so sed treats it literally: backslash, the '#' delimiter and the
# '&' back-reference. Without this a password or a URL would corrupt the properties file.
escape_replacement() {
  printf '%s' "$1" | sed -e 's/[\\&#]/\\&/g'
}

# Escapes a property key so its dots and dashes are matched literally on the left-hand side.
escape_pattern() {
  printf '%s' "$1" | sed -e 's/[].[^$*\\\/]/\\&/g'
}

applied=0
while IFS='|' read -r env_var prop_key || [ -n "${env_var}" ]; do
  # Tolerate comments, blank lines and trailing whitespace from editors.
  env_var="${env_var%%#*}"
  env_var="$(printf '%s' "${env_var}" | tr -d '[:space:]')"
  prop_key="$(printf '%s' "${prop_key:-}" | tr -d '[:space:]')"
  [ -z "${env_var}" ] && continue
  if [ -z "${prop_key}" ]; then
    echo "Warning: no property key mapped for ${env_var}, skipping" >&2
    continue
  fi

  # Unset variables are left alone so the packaged default wins. An explicitly empty value is a
  # deliberate override (an empty Redis password, for instance) and is applied.
  if [ -z "${!env_var+defined}" ]; then
    continue
  fi
  value="${!env_var}"

  key_pattern="$(escape_pattern "${prop_key}")"
  replacement="$(escape_replacement "${value}")"

  if grep -qE "^${key_pattern}=" "${CONF_FILE}"; then
    sed -i "s#^${key_pattern}=.*#${prop_key}=${replacement}#" "${CONF_FILE}"
  else
    # The module reads the key but the packaged file does not list it; append rather than drop it.
    printf '%s=%s\n' "${prop_key}" "${value}" >> "${CONF_FILE}"
  fi
  applied=$((applied + 1))
done < "${MAPPING_FILE}"

echo "${APP_NAME}: applied ${applied} environment override(s)"

cd "${APP_HOME}/bin"
exec ./start.sh
