# Read Me First

This module provides the SMSF (Short Message Service Function) for 5G Core,
exposing the Nsmsf_SMService SBI over HTTP/2 (h2c). TLS termination is delegated
to NGINX/Apache. It interworks with legacy SS7/MAP (TPDU) for SMS delivery to
2G/3G/4G subscribers.

Base package: `com.paicbd.smsc.smsf.module`

# Getting Started

```bash
./mvnw clean package
java -jar target/smsf-module-1.0.0-SNAPSHOT.jar
```

The server listens on `server.port` (default `9070`) with HTTP/2 cleartext (h2c)
enabled. Test with:

```bash
curl --http2-prior-knowledge http://localhost:9070/actuator/health
```

# Reference Documentation

- 3GPP TS 29.540 — Nsmsf_SMService
- [Spring Boot 3.4.12 Reference](https://docs.spring.io/spring-boot/3.4.12/reference/)
- [Spring WebFlux](https://docs.spring.io/spring-framework/reference/web/webflux.html)
- [Reactor Netty HTTP/2](https://projectreactor.io/docs/netty/release/reference/index.html#http2)
- [Restcomm jSS7](https://github.com/RestComm/jss7)
