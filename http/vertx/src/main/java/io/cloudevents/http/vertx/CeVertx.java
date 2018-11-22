package io.cloudevents.http.vertx;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventBuilder;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpServerRequest;

import java.net.URI;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.stream.Collectors;

import static io.cloudevents.CloudEvent.CLOUD_EVENTS_VERSION_KEY;
import static io.cloudevents.CloudEvent.EVENT_ID_KEY;
import static io.cloudevents.CloudEvent.EVENT_TIME_KEY;
import static io.cloudevents.CloudEvent.EVENT_TYPE_KEY;
import static io.cloudevents.CloudEvent.EVENT_TYPE_VERSION_KEY;
import static io.cloudevents.CloudEvent.HEADER_PREFIX;
import static io.cloudevents.CloudEvent.SCHEMA_URL_KEY;
import static io.cloudevents.CloudEvent.SOURCE_KEY;

public final class CeVertx {

    private CeVertx() {
        // no-op
    }

    public static void writeToHttpClientRequest(final CloudEvent<?> ce, final HttpClientRequest request) {

        request
                // required
                .putHeader("content-type", "application/json")
                .putHeader(CLOUD_EVENTS_VERSION_KEY, ce.getCloudEventsVersion())
                .putHeader(EVENT_TYPE_KEY, ce.getEventType())
                .putHeader(SOURCE_KEY, ce.getSource().toString())
                .putHeader(EVENT_ID_KEY, ce.getEventID());

                // optionl
        ce.getEventTypeVersion().ifPresent(eventTypeVersion -> {
            request.putHeader(EVENT_TYPE_VERSION_KEY, eventTypeVersion);
        });

        ce.getEventTime().ifPresent(eventTime -> {
            request.putHeader(EVENT_TIME_KEY, eventTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        });

        ce.getSchemaURL().ifPresent(schemaUrl -> {
            request.putHeader(SCHEMA_URL_KEY, schemaUrl.toString());
        });

        ce.getData().ifPresent(data -> {
            request.write(Buffer.buffer(data.toString()));
        });

    }

    public static CloudEvent readFromRequest(final HttpServerRequest request) {

        final MultiMap headers = request.headers();

        final CloudEventBuilder builder = new CloudEventBuilder();

        // just check, no need to set the version
        readRequiredHeaderValue(headers, CLOUD_EVENTS_VERSION_KEY);

        builder
                // set required values
                .eventType(readRequiredHeaderValue(headers, EVENT_TYPE_KEY))
                .source(URI.create(readRequiredHeaderValue(headers ,SOURCE_KEY)))
                .eventID(readRequiredHeaderValue(headers, EVENT_ID_KEY))

                // set optional values

                .eventTypeVersion(headers.get(EVENT_TYPE_VERSION_KEY));

                final String eventTime = headers.get(EVENT_TIME_KEY);
                if (eventTime != null) {
                    builder.eventTime(ZonedDateTime.parse(eventTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME));
                }

                final String schemaURL = headers.get(SCHEMA_URL_KEY);
                if (schemaURL != null) {
                    builder.schemaURL(URI.create(schemaURL));
                }


        // get the extensions
        final Map<String, String> extensions =
                headers.entries().stream()
                        .filter(header -> header.getKey().startsWith(HEADER_PREFIX))
                        .collect(Collectors.toMap(h -> h.getKey(), h -> h.getValue()));


        builder.extensions(extensions);
        request.bodyHandler(buff -> builder.data(buff.toString()));

        return builder.build();
    }

    private static String readRequiredHeaderValue(final MultiMap headers, final String headerName) {
        return requireNonNull(headers.get(headerName));
    }

    private static String requireNonNull(final String val) {
        if (val == null) {
            throw new IllegalArgumentException();
        } else {
            return val;
        }
    }
}
