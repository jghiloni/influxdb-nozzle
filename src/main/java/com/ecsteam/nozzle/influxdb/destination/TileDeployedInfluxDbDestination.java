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

import java.net.MalformedURLException;
import java.net.URL;

/**
 * An instance of MetricsDestination that will send information to http://influxdb.{SYSTEM_DOMAIN}, as that
 * is how the influxdb system will be deployed as a tile
 */
public class TileDeployedInfluxDbDestination implements MetricsDestination {

	private final String apiEndpoint;

	public TileDeployedInfluxDbDestination(String apiEndpoint) {
		this.apiEndpoint = apiEndpoint;
	}

	@Override
	public String getInfluxDbHost() {
		return String.format("http://influxdb.%s", getSystemDomain());
	}

	private String getSystemDomain() {
		try {
			URL url = new URL(apiEndpoint);
			String hostname = url.getHost();

			// the api endpoint is always api.<system_domain>, so just strip off the first four characters
			return hostname.substring(4);
		} catch (MalformedURLException e) {
			return null;
		}
	}
}
