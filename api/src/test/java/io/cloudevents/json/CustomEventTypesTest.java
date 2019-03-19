/**
 * Copyright 2018 The CloudEvents Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cloudevents.json;

import com.fasterxml.jackson.core.type.TypeReference;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventBuilder;
import io.cloudevents.json.types.GlusterVolumeClaim;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.UUID;

import static io.cloudevents.json.Json.MAPPER;
import static org.assertj.core.api.Assertions.assertThat;
public class CustomEventTypesTest {

    @Test
    public void testBinding() throws IOException {

        // given
        final Map<String, Object> storagePayload = (MAPPER.readValue(Thread.currentThread().getContextClassLoader().getResourceAsStream("pvc.json"), Map.class));
        final CloudEvent<Map<String, Object>> storageCloudEventWrapper = new CloudEventBuilder<Map<String, Object>>()
                .type("ProvisioningSucceeded")
                .source(URI.create("/scheduler"))
                .id(UUID.randomUUID().toString())
                .data(storagePayload)
                .build();

        // when
        final String httpSerializedPayload = MAPPER.writeValueAsString(storageCloudEventWrapper);
        assertThat(httpSerializedPayload).contains("PersistentVolumeClaim");
        //PARSE into real object, on the other side
        final CloudEvent<GlusterVolumeClaim> event = Json.decodeValue(httpSerializedPayload, new TypeReference<CloudEvent<GlusterVolumeClaim>>() {});

        // then
        assertThat(event.getData().get()).isNotNull();
        assertThat(event.getData().get().getSpec().getCapacity().get("storage")).isEqualTo("2Gi");
        assertThat(event.getData().get().getSpec().getAccessModes()).containsExactly("ReadWriteMany");
    }

    @Test
    public void bloadedString() throws IOException {

        final String raw = "{\"payload\":\"{\\\"signal\\\":2,\\\"data\\\":{\\\"brokerage\\\":{\\\"broker\\\":{\\\"id\\\":\\\"BMS-USB-G-JACE\\\",\\\"meta\\\":{\\\"protocol\\\":\\\"BACNET\\\",\\\"building\\\":\\\"Urban Sciences Building\\\",\\\"buildingFloor\\\":\\\"G\\\"}},\\\"id\\\":\\\"Drivers.GF_Event_Space_Electricity_Meters.GF_ES_Mechcanical_Plant.points.GF_ES_CHW_Pump_Current\\\",\\\"meta\\\":{}},\\\"entity\\\":{\\\"name\\\":\\\"Urban Sciences Building: Floor G\\\",\\\"meta\\\":{\\\"building\\\":\\\"Urban Sciences Building\\\",\\\"buildingFloor\\\":\\\"G\\\"}},\\\"feed\\\":{\\\"metric\\\":\\\"GF ES CHW Pump Current\\\",\\\"meta\\\":{}},\\\"timeseries\\\":{\\\"unit\\\":\\\"no units\\\",\\\"value\\\":{\\\"time\\\":\\\"2019-03-19T10:05:23.196Z\\\",\\\"timeAccuracy\\\":4.953,\\\"data\\\":0.15316040813922882,\\\"type\\\":\\\"Real\\\"}}},\\\"recipients\\\":0}\",\"schemaURL\":null,\"contentType\":null,\"specVersion\":\"0.2\",\"data\":\"{\\\"signal\\\":2,\\\"data\\\":{\\\"brokerage\\\":{\\\"broker\\\":{\\\"id\\\":\\\"BMS-USB-G-JACE\\\",\\\"meta\\\":{\\\"protocol\\\":\\\"BACNET\\\",\\\"building\\\":\\\"Urban Sciences Building\\\",\\\"buildingFloor\\\":\\\"G\\\"}},\\\"id\\\":\\\"Drivers.GF_Event_Space_Electricity_Meters.GF_ES_Mechcanical_Plant.points.GF_ES_CHW_Pump_Current\\\",\\\"meta\\\":{}},\\\"entity\\\":{\\\"name\\\":\\\"Urban Sciences Building: Floor G\\\",\\\"meta\\\":{\\\"building\\\":\\\"Urban Sciences Building\\\",\\\"buildingFloor\\\":\\\"G\\\"}},\\\"feed\\\":{\\\"metric\\\":\\\"GF ES CHW Pump Current\\\",\\\"meta\\\":{}},\\\"timeseries\\\":{\\\"unit\\\":\\\"no units\\\",\\\"value\\\":{\\\"time\\\":\\\"2019-03-19T10:05:23.196Z\\\",\\\"timeAccuracy\\\":4.953,\\\"data\\\":0.15316040813922882,\\\"type\\\":\\\"Real\\\"}}},\\\"recipients\\\":0}\",\"time\":null,\"id\":\"2f9f3459-7446-42c3-a716-991dfe6d8dc5\",\"type\":\"kafka-event\",\"extensions\":[],\"source\":\"my-topic\"}";


        CloudEvent<?> ce = Json.decodeCloudEvent(raw);

        System.out.println(ce.getData().isPresent());


    }



}
