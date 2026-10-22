# smsf-core — SBI/API layer, ScyllaDB persistence, NRF integration and the Redis token publisher.
#
# Build from the repository root so the module jar and the shared entrypoint are in context:
#   mvn -B clean package -DskipTests
#   docker build -f docker/core/Dockerfile --build-arg VERSION=<version> -t smsf-core:<version> .
FROM eclipse-temurin:21.0.2_13-jdk-alpine AS runtime

ARG VERSION
ENV APP_NAME=smsf-core \
    APP_HOME=/opt/paic/SMSF_CORE \
    APP_JAR=smsf-core.jar

# Runtime dependencies and unprivileged user
RUN test -n "$VERSION" || (echo "VERSION build-arg is required" >&2; exit 1) && \
    apk add --no-cache bash && \
    addgroup -S smscgroup && \
    adduser -S smscuser -G smscgroup --disabled-password && \
    mkdir -p ${APP_HOME}/bin ${APP_HOME}/conf ${APP_HOME}/logs

COPY smsf-core/target/smsf-core-${VERSION}.jar ${APP_HOME}/bin/${APP_JAR}
# The module's own properties are the single source of truth; run.sh overlays the environment on top.
COPY --chown=smscuser:smscgroup smsf-core/src/main/resources/application.properties ${APP_HOME}/conf/application.properties
COPY --chown=smscuser:smscgroup docker/common/conf/logback.xml ${APP_HOME}/conf/logback.xml
COPY --chown=smscuser:smscgroup docker/core/mapping.conf ${APP_HOME}/conf/mapping.conf
COPY docker/common/run.sh docker/common/start.sh ${APP_HOME}/bin/

# run.sh rewrites application.properties at startup, so conf must stay writable by the app user.
# bin stays owned by root: the app only needs to read and execute what is in it, and leaving the
# jar and the entrypoint writable would let a compromised process rewrite what runs on the restart.
RUN chmod +x ${APP_HOME}/bin/run.sh ${APP_HOME}/bin/start.sh && \
    chown -R smscuser:smscgroup ${APP_HOME}/conf ${APP_HOME}/logs && \
    chmod -R g+rw ${APP_HOME}/conf ${APP_HOME}/logs

USER smscuser
WORKDIR ${APP_HOME}
EXPOSE 9070
ENTRYPOINT ["/opt/paic/SMSF_CORE/bin/run.sh"]
