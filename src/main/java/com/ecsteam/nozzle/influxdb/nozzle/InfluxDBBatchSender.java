/*******************************************************************************
 *  Copyright 2017 ECS Team, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  this file except in compliance with the License. You may obtain a copy of the
 *  License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed
 *  under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 ******************************************************************************/

package com.ecsteam.nozzle.influxdb.nozzle;

import com.ecsteam.nozzle.influxdb.config.NozzleProperties;
import com.ecsteam.nozzle.influxdb.destination.MetricsDestination;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class InfluxDBBatchSender implements Runnable {

	private final ResettableCountDownLatch latch;
	private final List<String> messages;
	private final NozzleProperties properties;
	private final MetricsDestination influxDbDestination;

	private RestTemplate template = new RestTemplate();
	private URI uri;

	@Override
	public void run() {
		template.setErrorHandler(new ResponseErrorHandler() {
			@Override
			public boolean hasError(ClientHttpResponse clientHttpResponse) throws IOException {
				return clientHttpResponse.getRawStatusCode() > 399;
			}

			@Override
			public void handleError(ClientHttpResponse clientHttpResponse) throws IOException {

			}
		});

		ArrayList<String> msgClone = new ArrayList<>();
		while (true) {
			try {
				latch.await();
			} catch (InterruptedException e) {
				break;
			}

			msgClone.clear();
			msgClone.addAll(messages);

			final StringBuilder builder = new StringBuilder();
			msgClone.forEach(s -> builder.append(s).append("\n"));

			String body = builder.toString();

			RequestEntity<String> entity =
					new RequestEntity<>(body, HttpMethod.POST, getUri());

			ResponseEntity<String> response;

			try {
				response = template.exchange(entity, String.class);


				if (response.getStatusCode() != HttpStatus.NO_CONTENT) {
					System.err.println("Unexpected error writing to influx. Expected 204, got " +
							response.getStatusCodeValue());

					System.err.println("Request:");
					System.err.println(body);
					System.err.println("Response:");
					System.err.println(response.getBody());
				}

				messages.clear();
				latch.reset();
			} catch (ResourceAccessException e) {
				e.printStackTrace();
			}
		}
	}

	private URI getUri() {
		if (uri == null) {
			uri = URI.create(String.format("%s/write?db=%s",
					influxDbDestination.getInfluxDbHost(), properties.getDbName()));
		}

		return uri;
	}
}
