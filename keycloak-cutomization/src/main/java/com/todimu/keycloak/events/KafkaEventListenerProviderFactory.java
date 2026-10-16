package com.todimu.keycloak.events;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.events.EventType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;


public class KafkaEventListenerProviderFactory implements EventListenerProviderFactory {

    public static final String PROVIDER_ID = "kafka";

    private static final Logger log = Logger.getLogger(KafkaEventListenerProviderFactory.class);

    private static final String DEFAULT_EVENT_TYPES = "LOGIN,LOGIN_ERROR,REGISTER";

    private KafkaEventPublisher publisher;

    private Set<EventType> eventTypes;

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    @Override
    public void init(Config.Scope config) {
        String servers = config.get("bootstrap-servers", "kafka:9092");
        String clientId = config.get("client-id", "keycloak-event-listener");
        String topic = config.get("topic", "keycloak.events");
        int queueSize = config.getInt("queue-size", 10000);
        eventTypes = parseEventTypes(config.get("event-types", DEFAULT_EVENT_TYPES));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        String auth = configureSecurity(config, props);
        // The producer is created later, on the publisher's background thread, so an
        // unreachable Kafka can never stop Keycloak from starting.
        publisher = new KafkaEventPublisher(() -> createProducer(props), topic, queueSize);
        publisher.start();

        log.infof("Kafka event listener ready: servers=%s topic=%s auth=%s eventTypes=%s", servers, topic, auth, eventTypes);
    }

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        return new KafkaEventListenerProvider(session, publisher, eventTypes);
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        // nothing to do
    }

    @Override
    public void close() {
        if (publisher != null) {
            // sends what is still queued, flushes and closes the producer
            publisher.close();
        }
    }

    /**
     * Kafka loads classes by name (serializers, SASL login modules) through the
     * thread's context class loader. Under Keycloak that loader can't see this
     * jar, so point it at the loader that loaded Kafka (our jar) while creating
     * the producer.
     */
    private static KafkaProducer<String, String> createProducer(Properties props) {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(KafkaProducer.class.getClassLoader());
            return new KafkaProducer<>(props);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    static String configureSecurity(Config.Scope config, Properties props) {
        String username = config.get("username");
        String password = config.get("password");
        boolean hasCredentials = username != null && !username.isBlank();

        String protocol = config.get("security-protocol", hasCredentials ? "SASL_SSL" : "PLAINTEXT").trim().toUpperCase();
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);

        String truststore = config.get("ssl-truststore-location");
        if (truststore != null && !truststore.isBlank()) {
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
            String truststorePassword = config.get("ssl-truststore-password");
            if (truststorePassword != null) {
                props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
            }
        }

        if (!protocol.startsWith("SASL")) {
            return protocol;
        }
        if (!hasCredentials || password == null) {
            throw new IllegalStateException("Kafka event listener: security-protocol " + protocol
                    + " needs both 'username' and 'password' to be set");
        }

        String mechanism = config.get("sasl-mechanism", "PLAIN").trim().toUpperCase();
        String loginModule = switch (mechanism) {
            case "PLAIN" -> "org.apache.kafka.common.security.plain.PlainLoginModule";
            case "SCRAM-SHA-256", "SCRAM-SHA-512" -> "org.apache.kafka.common.security.scram.ScramLoginModule";
            default -> throw new IllegalStateException("Kafka event listener: unsupported sasl-mechanism '" + mechanism
                    + "' (use PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512)");
        };
        props.put(SaslConfigs.SASL_MECHANISM, mechanism);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, loginModule + " required"
                + " username=\"" + jaasEscape(username) + "\""
                + " password=\"" + jaasEscape(password) + "\";");

        return protocol + "/" + mechanism + " as " + username;
    }

    /** Escapes backslashes and quotes so any password works inside the JAAS string. */
    private static String jaasEscape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /** Turns "LOGIN, LOGIN_ERROR,REGISTER" into a set; "*" means every type; unknown names are logged and skipped. */
    static Set<EventType> parseEventTypes(String value) {
        if (value == null || value.isBlank()) {
            return Collections.emptySet();
        }
        if (value.trim().equals("*")) {
            return Collections.unmodifiableSet(EnumSet.allOf(EventType.class));
        }
        Set<EventType> types = EnumSet.noneOf(EventType.class);
        for (String name : value.split(",")) {
            String trimmed = name.trim().toUpperCase();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                types.add(EventType.valueOf(trimmed));
            } catch (IllegalArgumentException e) {
                log.warnf("Kafka event listener: ignoring unknown event type '%s'", trimmed);
            }
        }
        return Collections.unmodifiableSet(types);
    }
}
