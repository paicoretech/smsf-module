# smsf-module

SMSF for the 5G core: Nsmsf_SMService over HTTP/2 SBI, with TPDU/SS7 interworking.

Three independently deployable Spring Boot applications plus one internal library, built from this
repository as a Maven multi-module project.

| Module | Deployable | Role |
|---|---|---|
| [`smsf-core`](docs/smsf-core.md) | yes | SBI/API towards the AMF, ScyllaDB persistence, NRF integration, publishes the token to Redis and the registration event to Kafka |
| [`smsf-n21-registration`](docs/smsf-n21-registration.md) | yes | Consumes the registration event and forwards it to the UDM over N21 |
| [`smsf-service-mt`](docs/smsf-service-mt.md) | yes | Consumes the priority MT topics and delivers to the AMF |
| [`smsf-common`](docs/smsf-common.md) | no | Shared code: TPDU processing, NRF connection, token cache |

The three applications never call each other in process: they meet through Redis and Kafka only.

## Documentation

- **[docs/](docs/README.md)** — architecture, shared Redis and Kafka contracts, external
  dependencies, build and deployment, and a page per module with its full property reference.
- **[SMSF_Module_Separation_Guidelines.md](SMSF_Module_Separation_Guidelines.md)** — why the module
  was split this way and the decisions taken while doing it.

## Build

```bash
mvn -B clean package          # four modules, runs the tests
mvn -B clean install -DskipTests
```

Requires JDK 21. It also needs `smsc-utils` and the `sba-stack` artifacts in the local repository;
both are built from their own repositories.

## Docker

The build context is the repository root:

```bash
mvn -B clean package -DskipTests
docker build -f docker/core/Dockerfile --build-arg VERSION=<version> -t smsf-core:<version> .
docker build -f docker/n21/Dockerfile  --build-arg VERSION=<version> -t smsf-n21-registration:<version> .
docker build -f docker/mt/Dockerfile   --build-arg VERSION=<version> -t smsf-service-mt:<version> .
```

Each module ships a reference `docker-compose.yml` under `docker/<module>/`. To bring all three up
on one host, merge them with `-f`. See [docs/README.md](docs/README.md#build-and-deployment).

## Tests

```bash
mvn -B test
```

## External services

Redis, Kafka, ScyllaDB, NRF, AMF and UDM are external. Which module needs which is in
[docs/README.md](docs/README.md#external-dependencies).
