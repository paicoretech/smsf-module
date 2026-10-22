#!/bin/bash
#
# Launcher shared by the three SMSF images. The jar and the install directory come from the
# environment so the same script serves smsf-core, smsf-n21-registration and smsf-service-mt.
#
set -euo pipefail

: "${APP_HOME:?APP_HOME is not set}"
: "${APP_JAR:?APP_JAR is not set}"

# Default JVM heap settings if not provided as environment variables
JVM_XMS="${JVM_XMS:--Xms128m}"
JVM_XMX="${JVM_XMX:--Xmx256m}"

CONF_FILE="${APP_HOME}/conf/application.properties"
LOGBACK_FILE="${APP_HOME}/conf/logback.xml"

# conf/logback.xml reads ${LOG_LEVEL} for the root level. SMSF_TRACE_LEVEL is the name the rest of
# the platform uses, so it is accepted here and translated.
export LOG_LEVEL="${SMSF_TRACE_LEVEL:-${LOG_LEVEL:-INFO}}"

if [ "${ENABLE_JMX:-false}" = "true" ]; then
  if [ -z "${IP_JMX:-}" ]; then
      echo "Error: ENABLE_JMX is active, but IP_JMX is not defined." >&2
      exit 1
  fi

  rmiPort=${JMX_PORT:-9010}
  jmxPort=${JMX_PORT:-9010}

  echo "JMX_PORT: ${jmxPort}"
  echo "JMX_IP: ${IP_JMX}"

  exec java \
    "${JVM_XMS}" "${JVM_XMX}" \
    -Dcom.sun.management.jmxremote \
    -Dcom.sun.management.jmxremote.port="${jmxPort}" \
    -Dcom.sun.management.jmxremote.local.only=false \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Djava.rmi.server.hostname="${IP_JMX}" \
    -Dcom.sun.management.jmxremote.rmi.port="${rmiPort}" \
    -Dspring.config.location="${CONF_FILE}" \
    -jar "${APP_JAR}" \
    --logging.config="${LOGBACK_FILE}"
else
  exec java \
    "${JVM_XMS}" "${JVM_XMX}" \
    -Dspring.config.location="${CONF_FILE}" \
    -jar "${APP_JAR}" \
    --logging.config="${LOGBACK_FILE}"
fi
