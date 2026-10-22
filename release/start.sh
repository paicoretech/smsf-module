#!/bin/bash
# Launcher for the non-containerised install. @APP_JAR@ and @APP_NAME@ are substituted by
# release/build.xml when the module zip is assembled.
set -euo pipefail

APP_NAME="@APP_NAME@"
APP_JAR="@APP_JAR@"

JVM_XMS="${JVM_XMS:--Xms128m}"
JVM_XMX="${JVM_XMX:--Xmx256m}"

echo "Starting ${APP_NAME}..."

exec java \
  "${JVM_XMS}" "${JVM_XMX}" \
  -Dspring.config.location=../conf/application.properties \
  -jar "${APP_JAR}" \
  --logging.config=../conf/logback.xml
