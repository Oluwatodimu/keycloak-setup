package com.todimu.keycloak.events;

import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.AbstractKeycloakTransaction;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.util.JsonSerialization;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class KafkaEventListenerProvider implements EventListenerProvider {

    private static final Logger log = Logger.getLogger(KafkaEventListenerProvider.class);

    private final KeycloakSession session;

    private final KafkaEventPublisher publisher;

    private final Set<EventType> eventTypes;

    public KafkaEventListenerProvider(KeycloakSession session, KafkaEventPublisher publisher, Set<EventType> eventTypes) {
        this.session = session;
        this.publisher = publisher;
        this.eventTypes = eventTypes;
    }

    @Override
    public void onEvent(Event event) {
        if (!eventTypes.contains(event.getType())) {
            return; // not one we publish
        }

        String key = event.getUserId() != null ? event.getUserId() : event.getRealmId();
        String type = event.getType().name();
        String json;
        try {
            json = JsonSerialization.writeValueAsString(toPayload(event));
        } catch (Exception e) {
            log.warnf("Kafka event listener: could not serialize %s event: %s", type, e.getMessage());
            return;
        }

        session.getTransactionManager().enlistAfterCompletion(new AbstractKeycloakTransaction() {
            @Override
            protected void commitImpl() {
                publisher.publish(key, type, json); // queued; returns immediately
            }

            @Override
            protected void rollbackImpl() {
                // transaction failed: nothing happened, nothing to publish
            }
        });
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        // Admin events are not published (yet).
    }

    @Override
    public void close() {
        // nothing to close: the publisher is shared and owned by the factory
    }

    private Map<String, Object> toPayload(Event event) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", event.getId());
        payload.put("time", event.getTime());
        payload.put("type", event.getType().name());
        payload.put("realmId", event.getRealmId());
        payload.put("realmName", realmName(event.getRealmId()));
        payload.put("clientId", event.getClientId());
        payload.put("userId", event.getUserId());
        payload.put("sessionId", event.getSessionId());
        payload.put("ipAddress", event.getIpAddress());
        payload.put("error", event.getError());
        payload.put("details", event.getDetails());
        return payload;
    }

    private String realmName(String realmId) {
        if (realmId == null) {
            return null;
        }
        RealmModel realm = session.realms().getRealm(realmId);
        return realm != null ? realm.getName() : null;
    }
}
