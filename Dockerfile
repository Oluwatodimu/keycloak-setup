FROM maven:3.9.6-eclipse-temurin-21 AS MAVEN_BUILD

# copy the pom and src code to the container
COPY ./ ./

# package our application code
RUN mvn clean package

FROM quay.io/keycloak/keycloak:26.7.3 AS builder

ENV KC_HEALTH_ENABLED=true
ENV KC_METRICS_ENABLED=true
ENV KC_FEATURES=token-exchange,admin-fine-grained-authz
ENV QUARKUS_TRANSACTION_MANAGER_ENABLE_RECOVERY=true

ENV KC_DB=mysql

WORKDIR /opt/keycloak

COPY --from=MAVEN_BUILD ./keycloak-theme/target/*.jar /opt/keycloak/providers

RUN /opt/keycloak/bin/kc.sh build

FROM quay.io/keycloak/keycloak:26.7.3

COPY --from=builder /opt/keycloak/ /opt/keycloak/

EXPOSE 8080
EXPOSE 9000
EXPOSE 8443

ENTRYPOINT ["/opt/keycloak/bin/kc.sh", "start", "--optimized",  "--http-enabled=true", "--proxy-headers=xforwarded"]