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
public class InfluxDBBatchListener implements Runnable {

	private final ResettableCountDownLatch latch;
	private final List<String> messages;
	private final NozzleProperties properties;
	private final MetricsDestination influxDbDestination;

	private final ArrayList<String> msgClone = new ArrayList<>();

	@Override
	public void run() {
		while (true) {
			try {
				latch.await();
			} catch (InterruptedException e) {
				break;
			}

			msgClone.clear();
			msgClone.addAll(messages);
			new Thread(new InfluxDBSender(msgClone, properties, influxDbDestination)).start();

			messages.clear();
			latch.reset();
		}
	}
}
