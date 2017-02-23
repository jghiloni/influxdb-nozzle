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
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Sends a batch of messages to InfluxDB with retry logic.
 */
@RequiredArgsConstructor
@Slf4j
@Service
public class InfluxDBSender {
	private RestTemplate httpClient = new RestTemplate();
	private URI uri;
	private BackOffPolicy backOffPolicy;

	private final NozzleProperties properties;
	private final MetricsDestination influxDbDestination;

	@Async
	public void sendBatch(List<String> messages) {
		log.debug("ENTER sendBatch");
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

		final AtomicInteger counter = new AtomicInteger(0);
		retryable.execute(retryContext -> {
			int count = counter.incrementAndGet();
			log.trace("Attempt {} to deliver this batch", count);
			final StringBuilder builder = new StringBuilder();
			messages.forEach(s -> builder.append(s).append("\n"));

			String body = builder.toString();

			RequestEntity<String> entity =
					new RequestEntity<>(body, HttpMethod.POST, getUri());

			ResponseEntity<String> response;

			response = httpClient.exchange(entity, String.class);


			if (response.getStatusCode() != HttpStatus.NO_CONTENT) {
				log.error("Failed to write logs to InfluxDB! Expected error code 204, got {}", response.getStatusCodeValue());

				log.trace("Request Body: {}", body);
				log.trace("Response Body: {}", response.getBody());

			} else {
				log.debug("batch sent successfully!");
			}

			log.debug("EXIT sendBatch");

			return null;
		}, recoveryContext -> {
			log.trace("Failed after {} attempts!", counter.get());
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
		if (backOffPolicy == null) {
			log.info("Using backoff policy {}", properties.getBackoffPolicy().name());
			switch (properties.getBackoffPolicy()) {
				case linear:
					backOffPolicy = new FixedBackOffPolicy();
					((FixedBackOffPolicy) backOffPolicy).setBackOffPeriod(properties.getMinBackoff());
					break;
				case random:
					backOffPolicy = new UniformRandomBackOffPolicy();
					((UniformRandomBackOffPolicy) backOffPolicy).setMinBackOffPeriod(properties.getMinBackoff());
					((UniformRandomBackOffPolicy) backOffPolicy).setMaxBackOffPeriod(properties.getMaxBackoff());
					break;
				case exponential:
				default:
					backOffPolicy = new ExponentialBackOffPolicy();
					((ExponentialBackOffPolicy) backOffPolicy).setInitialInterval(properties.getMinBackoff());
					((ExponentialBackOffPolicy) backOffPolicy).setMaxInterval(properties.getMaxBackoff());
					break;
			}
		}

		return backOffPolicy;
	}
}
