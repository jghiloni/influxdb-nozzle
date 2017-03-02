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

import com.ecsteam.nozzle.influxdb.cache.AppSummary;
import com.ecsteam.nozzle.influxdb.cache.StructureCache;
import com.ecsteam.nozzle.influxdb.config.NozzleProperties;
import com.ecsteam.nozzle.influxdb.destination.MetricsDestination;
import com.ecsteam.nozzle.influxdb.utils.ResettableCountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.cloudfoundry.doppler.ContainerMetric;
import org.cloudfoundry.doppler.CounterEvent;
import org.cloudfoundry.doppler.Envelope;
import org.cloudfoundry.doppler.ValueMetric;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;

/**
 * Captures messages from the Cloud Foundry Firehose and batches them to be sent to InfluxDB
 */
@Service
@Slf4j
public class InfluxDBWriter {

	private final ResettableCountDownLatch latch;
	private final List<String> messages;

	private StructureCache cache;

	private String foundation;

	@Autowired
	public InfluxDBWriter(NozzleProperties properties, MetricsDestination destination, InfluxDBSender sender) {
		log.info("Initializing DB Writer with batch size {}", properties.getBatchSize());
		this.messages = Collections.synchronizedList(new ArrayList<>());
		this.latch = new ResettableCountDownLatch(properties.getBatchSize());

		this.foundation = properties.getFoundation();

		new Thread(new InfluxDBBatchListener(latch, messages, sender)).start();
	}

	/**
	 * Convert an envelope into an InfluxDB compatible message. In general, the format is
	 *
	 * <tt>message[,tag=value]* value timestamp</tt>
	 *
	 * Add each message String to a batch list and count down a latch. When the latch reaches 0,
	 * it will write to InfluxDB and reset.
	 *
	 * @param envelope The event from the Firehose
	 */
	@Async
	public void writeMessage(Envelope envelope) {

		Set<String> newMessages;
		switch (envelope.getEventType()) {
			case CONTAINER_METRIC:
				newMessages = writeContainerMetric(envelope);
				break;
			case COUNTER_EVENT:
				newMessages = writeCounterEvent(envelope);
				break;
			case VALUE_METRIC:
				newMessages = writeValueMetric(envelope);
				break;
			default:
				return;
		}

		synchronized (this) {
			this.messages.addAll(newMessages);
			latch.countDown(newMessages.size());
		}
	}

	private Set<String> writeCounterEvent(Envelope envelope) {
		final StringBuilder messageBuilder = new StringBuilder();

		CounterEvent ce = envelope.getCounterEvent();
		messageBuilder.append(ce.getName());

		Map<String, String> tags = getTags(envelope);
		if (ce.getDelta() != null) {
			tags.put("delta", ce.getDelta().toString());
		}

		tags.put("eventType", "CounterEvent");
		tags.forEach((k, v) -> messageBuilder.append(",").append(k).append("=").append(v));
		messageBuilder.append(" value=").append(ce.getTotal())
				.append(" ")
				.append(envelope.getTimestamp());

		return Collections.singleton(messageBuilder.toString());
	}

	private Set<String> writeValueMetric(Envelope envelope) {
		final StringBuilder messageBuilder = new StringBuilder();

		ValueMetric vm = envelope.getValueMetric();
		messageBuilder.append(vm.getName());

		Map<String, String> tags = getTags(envelope);
		if (StringUtils.hasText(vm.getUnit())) {
			tags.put("unit", vm.getUnit());
		}

		tags.put("eventType", "ValueMetric");
		tags.forEach((k, v) -> messageBuilder.append(",").append(k).append("=").append(v));

		messageBuilder.append(" value=").append(vm.value())
				.append(" ")
				.append(envelope.getTimestamp());

		return Collections.singleton(messageBuilder.toString());
	}

	private Set<String> writeContainerMetric(Envelope envelope) {
		Set<String> metrics = new HashSet<>();

		ContainerMetric cm = envelope.getContainerMetric();

		Map<String, String> tags = getTags(envelope);
		tags.put("eventType", "ContainerMetric");
		tags.put("applicationId", cm.getApplicationId());
		tags.put("instanceIndex", cm.getInstanceIndex().toString());

		AppSummary summary;
		if (cache != null) {
			summary = cache.getAppSummary(cm.getApplicationId());
			tags.put("applicationName", summary.getApp().getName());
			tags.put("spaceId", summary.getSpace().getId());
			tags.put("spaceName", summary.getSpace().getName());
			tags.put("organizationId", summary.getOrg().getId());
			tags.put("organizationName", summary.getOrg().getName());
		}

		final StringBuilder builder = new StringBuilder();

		builder.setLength(0);
		builder.append("cpuPercentage");
		tags.forEach((k, v) -> builder.append(",").append(k).append("=").append(v));
		builder.append(" value=").append(cm.getCpuPercentage()).append(" ").append(envelope.getTimestamp());
		metrics.add(builder.toString());

		builder.setLength(0);
		builder.append("diskBytes");
		tags.forEach((k, v) -> builder.append(",").append(k).append("=").append(v));
		builder.append(" value=").append(cm.getDiskBytes()).append(" ").append(envelope.getTimestamp());
		metrics.add(builder.toString());

		builder.setLength(0);
		builder.append("diskBytesQuota");
		tags.forEach((k, v) -> builder.append(",").append(k).append("=").append(v));
		builder.append(" value=").append(cm.getDiskBytesQuota()).append(" ").append(envelope.getTimestamp());
		metrics.add(builder.toString());

		builder.setLength(0);
		builder.append("memoryBytes");
		tags.forEach((k, v) -> builder.append(",").append(k).append("=").append(v));
		builder.append(" value=").append(cm.getMemoryBytes()).append(" ").append(envelope.getTimestamp());
		metrics.add(builder.toString());

		builder.setLength(0);
		builder.append("memoryBytesQuota");
		tags.forEach((k, v) -> builder.append(",").append(k).append("=").append(v));
		builder.append(" value=").append(cm.getMemoryBytesQuota()).append(" ").append(envelope.getTimestamp());
		metrics.add(builder.toString());

		return metrics;
	}

	/**
	 * Get all the tags from the Envelope plus any EventType-specific fields into a single Map
	 *
	 * @param envelope the Event
	 * @return the tag map
	 */
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

		}


		return tags;
	}

	@Autowired(required = false)
	public void setStructureCache(StructureCache cache) {
		this.cache = cache;
	}
}
