/*******************************************************************************
 * Copyright 2017 ECS Team, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package com.ecsteam.nozzle.influxdb.nozzle;

import com.ecsteam.nozzle.influxdb.config.NozzleProperties;
import com.ecsteam.nozzle.influxdb.destination.MetricsDestination;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.backoff.UniformRandomBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;

@RequiredArgsConstructor
@Slf4j
public class InfluxDBSender implements Runnable {
	private RestTemplate httpClient = new RestTemplate();
	private URI uri;

	private final List<String> messages;
	private final NozzleProperties properties;
	private final MetricsDestination influxDbDestination;

	@Override
	public void run() {
		httpClient.setErrorHandler(new ResponseErrorHandler() {
			@Override
			public boolean hasError(ClientHttpResponse clientHttpResponse) throws IOException {
				return clientHttpResponse.getRawStatusCode() > 399;
			}

			@Override
			public void handleError(ClientHttpResponse clientHttpResponse) throws IOException {

			}
		});

		RetryTemplate retryable = new RetryTemplate();
		retryable.setBackOffPolicy(getBackOffPolicy());
		retryable.setRetryPolicy(new SimpleRetryPolicy(properties.getMaxRetries(),
				Collections.singletonMap(ResourceAccessException.class, true)));

		retryable.execute(context -> {

			final StringBuilder builder = new StringBuilder();
			messages.forEach(s -> builder.append(s).append("\n"));

			String body = builder.toString();

			RequestEntity<String> entity =
					new RequestEntity<>(body, HttpMethod.POST, getUri());

			ResponseEntity<String> response;

			response = httpClient.exchange(entity, String.class);


			if (response.getStatusCode() != HttpStatus.NO_CONTENT) {
				System.err.println("Unexpected error writing to influx. Expected 204, got " +
						response.getStatusCodeValue());

				System.err.println("Request:");
				System.err.println(body);
				System.err.println("Response:");
				System.err.println(response.getBody());
			}

			return null;
		});
	}

	private URI getUri() {
		if (uri == null) {
			uri = URI.create(String.format("%s/write?db=%s",
					influxDbDestination.getInfluxDbHost(), properties.getDbName()));
		}

		return uri;
	}

	private BackOffPolicy getBackOffPolicy() {
		BackOffPolicy policy;
		switch (properties.getBackoffPolicy()) {

			case linear:
				policy = new FixedBackOffPolicy();
				((FixedBackOffPolicy) policy).setBackOffPeriod(properties.getMinBackoff());
				break;
			case random:
				policy = new UniformRandomBackOffPolicy();
				((UniformRandomBackOffPolicy) policy).setMinBackOffPeriod(properties.getMinBackoff());
				((UniformRandomBackOffPolicy) policy).setMaxBackOffPeriod(properties.getMaxBackoff());
				break;
			case exponential:
			default:
				policy = new ExponentialBackOffPolicy();
				((ExponentialBackOffPolicy) policy).setInitialInterval(properties.getMinBackoff());
				((ExponentialBackOffPolicy) policy).setMaxInterval(properties.getMaxBackoff());
				break;

		}

		return policy;
	}
}
