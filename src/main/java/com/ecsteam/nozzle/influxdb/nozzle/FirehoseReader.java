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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cloudfoundry.doppler.Envelope;
import org.cloudfoundry.doppler.FirehoseRequest;
import org.cloudfoundry.reactor.doppler.ReactorDopplerClient;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;

/**
 * Read events from the firehose
 */
@RequiredArgsConstructor
@Slf4j
public class FirehoseReader implements SmartLifecycle {
	private final ReactorDopplerClient dopplerClient;
	private final NozzleProperties properties;
	private final InfluxDBWriter writer;

	private boolean running = false;

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable runnable) {
		runnable.run();
		stop();
	}

	@Override
	public void start() {
		log.info("Connecting to the Firehose");
		dopplerClient.firehose(FirehoseRequest.builder()
			.subscriptionId(properties.getSubscriptionId()).build())
			.subscribe(this::receiveEvent, this::receiveError);
	}

	@Override
	public void stop() {
		running = false;
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	private void receiveEvent(Envelope envelope) {
		switch (envelope.getEventType()) {
			case COUNTER_EVENT:
			case VALUE_METRIC:
				writer.writeMessage(envelope);
				break;
		}
	}

	private void receiveError(Throwable error) {
		log.error("Error in receiving Firehose event: {}", error.getMessage());
		if (log.isDebugEnabled()) {
			error.printStackTrace();
		}
	}
}