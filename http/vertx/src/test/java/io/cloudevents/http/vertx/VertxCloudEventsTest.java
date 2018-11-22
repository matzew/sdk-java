package io.cloudevents.http.vertx;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventBuilder;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URI;

@RunWith(VertxUnitRunner.class)
public class VertxCloudEventsTest {

    private HttpServer server;
    private Vertx vertx;

    @Before
    public void setUp(TestContext context) throws IOException {
        vertx = Vertx.vertx();
        server = vertx.createHttpServer();
    }

    @Test
    public void endToEndTest(TestContext context) {
        final Async async = context.async();

        // Create the actuak CloudEvents object;
        final CloudEvent<String> cloudEvent = new CloudEventBuilder<String>()
                .source(URI.create("http://knative-eventing.com"))
                .eventID("foo-bar")
                .eventType("pushevent")
                .build();

        // set up the server and add a handler to check the values
        server.requestHandler(req -> {

            final CloudEvent<String> receivedEvent = CeVertx.readFromRequest(req);

            context.assertEquals(receivedEvent.getEventID(), cloudEvent.getEventID());
            context.assertEquals(receivedEvent.getSource().toString(), cloudEvent.getSource().toString());
            context.assertEquals(receivedEvent.getEventType(), cloudEvent.getEventType());

            req.response().end();
        }).listen(7890);

        // sending it to the test-server
        final HttpClientRequest request = vertx.createHttpClient().post(7890, "localhost", "/");

        CeVertx.writeToHttpClientRequest(cloudEvent, request);
        request.handler(resp -> {
            context.assertEquals(resp.statusCode(), 200);
            async.complete();
        });
        request.end();
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }
}
