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
import org.cloudfoundry.doppler.CounterEvent;
import org.cloudfoundry.doppler.Envelope;
import org.cloudfoundry.doppler.ValueMetric;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class InfluxDBWriter {

	private final ResettableCountDownLatch latch;
	private final List<String> messages;

	private String foundation;

	@Autowired
	public InfluxDBWriter(NozzleProperties properties, MetricsDestination destination) {
		this.messages = Collections.synchronizedList(new ArrayList<>());
		this.latch = new ResettableCountDownLatch(properties.getBatchSize());

		this.foundation = properties.getFoundation();

		new Thread(new InfluxDBBatchListener(latch, messages, properties, destination)).start();
	}

	@Async
	public void writeMessage(Envelope envelope) {
		final StringBuilder messageBuilder = new StringBuilder();

		CounterEvent ce = envelope.getCounterEvent();
		ValueMetric vm = envelope.getValueMetric();

		messageBuilder.append(ce == null ? vm.getName() : ce.getName());
		getTags(envelope).forEach((k, v) -> messageBuilder.append(",").append(k).append("=").append(v));

		messageBuilder.append(" value=").append(ce == null ? vm.value() : ce.getTotal())
			.append(" ")
			.append(envelope.getTimestamp());

		this.messages.add(messageBuilder.toString());

		latch.countDown();
	}

	private Map<String, String> getTags(Envelope envelope) {
		final Map<String, String> tags = new HashMap<>();

		if (StringUtils.hasText(foundation)) {
			tags.put("foundation", foundation);
		}

		if (!CollectionUtils.isEmpty(envelope.getTags())) {
			envelope.getTags().forEach((k, v) -> {
				if (StringUtils.hasText(v)) {
					tags.put(k, v);
				}
			});
		}

		if (StringUtils.hasText(envelope.getOrigin())) {
			tags.put("origin", envelope.getOrigin());
		}

		if (StringUtils.hasText(envelope.getIp())) {
			tags.put("ip", envelope.getIp());
		}

		if (StringUtils.hasText(envelope.getDeployment())) {
			tags.put("deployment", envelope.getDeployment());
		}

		if (StringUtils.hasText(envelope.getJob())) {
			tags.put("job", envelope.getJob());
		}

		if (StringUtils.hasText(envelope.getIndex())) {
			tags.put("index", envelope.getIndex());
		}

		if (envelope.getValueMetric() != null) {
			if (StringUtils.hasText(envelope.getValueMetric().getUnit())) {
				tags.put("unit", envelope.getValueMetric().getUnit());
			}

			tags.put("eventType", "ValueMetric");
		}

		if (envelope.getCounterEvent() != null) {
			if (envelope.getCounterEvent().getDelta() != null) {
				tags.put("delta", envelope.getCounterEvent().getDelta().toString());
			}

			tags.put("eventType", "CounterEvent");
		}

		return tags;
	}
}
