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

package com.ecsteam.nozzle.influxdb.destination;

import com.ecsteam.nozzle.influxdb.config.NozzleProperties;
import lombok.RequiredArgsConstructor;

/**
 * Returns the user-defined influx DB location
 */
@RequiredArgsConstructor
public class StandaloneInfluxDbDestination implements MetricsDestination {
	private final NozzleProperties properties;

	@Override
	public String getInfluxDbHost() {
		return properties.getDbHost();
	}
}
