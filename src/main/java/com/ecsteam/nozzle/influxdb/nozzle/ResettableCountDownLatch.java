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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 * A drop-in replacement for a <tt>java.util.concurrent.CountDownLatch</tt>
 * that can be reset for looping awaits.
 *
 * Taken from http://stackoverflow.com/a/40288284/4462517
 */
public class ResettableCountDownLatch {
	private final int initialCount;
	private volatile CountDownLatch latch;

	public ResettableCountDownLatch(int count) {
		initialCount = count;
		latch = new CountDownLatch(count);
	}

	public void reset() {
		latch = new CountDownLatch(initialCount);
	}

	public void countDown() {
		latch.countDown();
	}

	public void await() throws InterruptedException {
		latch.await();
	}

	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return latch.await(timeout, unit);
	}
}
