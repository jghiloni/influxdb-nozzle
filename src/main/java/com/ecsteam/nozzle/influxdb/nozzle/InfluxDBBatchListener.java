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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@Slf4j
public class InfluxDBBatchListener implements Runnable {

	private final ResettableCountDownLatch latch;
	private final List<String> messages;
	private final InfluxDBSender sender;

	private final ArrayList<String> msgClone = new ArrayList<>();

	@Override
	public void run() {
		while (true) {
			try {
				latch.await();
			} catch (InterruptedException e) {
				break;
			}

			log.debug("Batch size reached, sending to target");

			msgClone.clear();
			synchronized (this) {
				/*
				 there is a situation where a message could come in in the miniscule amount of time it takes to copy
				 the messages into the clone, so synchronize on that.
				  */
				msgClone.addAll(messages);
				messages.clear();
			}
			sender.sendBatch(msgClone);

			latch.reset();
		}
	}
}
