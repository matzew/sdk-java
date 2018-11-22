package io.cloudevents.http.jaxrs;


import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;

import static io.cloudevents.CloudEvent.CLOUD_EVENTS_VERSION_KEY;
import static io.cloudevents.CloudEvent.EVENT_ID_KEY;
import static io.cloudevents.CloudEvent.EVENT_TIME_KEY;
import static io.cloudevents.CloudEvent.EVENT_TYPE_KEY;
import static io.cloudevents.CloudEvent.EVENT_TYPE_VERSION_KEY;
import static io.cloudevents.CloudEvent.SCHEMA_URL_KEY;
import static io.cloudevents.CloudEvent.SOURCE_KEY;

public class CeJax {


    public static Response postToHttp(final CloudEvent<?> ce, final Invocation.Builder requestBuilder) {

        requestBuilder
                // required
                .header("content-type", "application/json")
                .header(CLOUD_EVENTS_VERSION_KEY, ce.getCloudEventsVersion())
                .header(EVENT_TYPE_KEY, ce.getEventType())
                .header(SOURCE_KEY, ce.getSource().toString())
                .header(EVENT_ID_KEY, ce.getEventID());

        // optionl
        ce.getEventTypeVersion().ifPresent(eventTypeVersion -> {
            requestBuilder.header(EVENT_TYPE_VERSION_KEY, eventTypeVersion);
        });

        ce.getEventTime().ifPresent(eventTime -> {
            requestBuilder.header(EVENT_TIME_KEY, eventTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        });

        ce.getSchemaURL().ifPresent(schemaUrl -> {
            requestBuilder.header(SCHEMA_URL_KEY, schemaUrl.toString());
        });

        if (ce.getData().isPresent()) {
            return requestBuilder.post(Entity.entity(ce.getData().get(), MediaType.APPLICATION_JSON_TYPE));
        } else {
            return requestBuilder.post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE));
        }
    }

    public static CloudEvent<?> fromHttpServletRequest(final HttpServletRequest request) {

        final CloudEventBuilder builder = new CloudEventBuilder();

        // just check, no need to set the version
        readRequiredHeaderValue(request, CLOUD_EVENTS_VERSION_KEY);

        builder
                // set required values
                .eventType(readRequiredHeaderValue(request, EVENT_TYPE_KEY))
                .source(URI.create(readRequiredHeaderValue(request ,SOURCE_KEY)))
                .eventID(readRequiredHeaderValue(request, EVENT_ID_KEY))

                // set optional values
                .eventTypeVersion(request.getHeader(EVENT_TYPE_VERSION_KEY));

        final String eventTime = request.getHeader(EVENT_TIME_KEY);
        if (eventTime != null) {
            builder.eventTime(ZonedDateTime.parse(eventTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        }

        final String schemaURL = request.getHeader(SCHEMA_URL_KEY);
        if (schemaURL != null) {
            builder.schemaURL(URI.create(schemaURL));
        }


        try {
            builder.data(request.getReader().lines().collect(Collectors.joining(System.lineSeparator())));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return builder.build();
    }

    private static String readRequiredHeaderValue(final HttpServletRequest request, final String headerName) {
        return requireNonNull(request.getHeader(headerName));
    }

    private static String requireNonNull(final String val) {
        if (val == null) {
            throw new IllegalArgumentException();
        } else {
            return val;
        }
    }
}