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

package com.ecsteam.nozzle.influxdb.cache;

import lombok.RequiredArgsConstructor;
import org.cloudfoundry.client.CloudFoundryClient;
import org.cloudfoundry.client.v2.applications.ListApplicationsRequest;
import org.cloudfoundry.client.v2.applications.ListApplicationsResponse;
import org.cloudfoundry.client.v2.organizations.ListOrganizationsRequest;
import org.cloudfoundry.client.v2.spaces.ListSpacesRequest;
import org.cloudfoundry.operations.CloudFoundryOperations;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.net.URI;

@RequiredArgsConstructor
public class StructureCache {

	private final CloudFoundryClient client;

	private RedisConnectionFactory redisConnectionFactory;

	private RedisTemplate<String, AppInfo> appTemplate;
	private RedisTemplate<String, OrgInfo> orgTemplate;
	private RedisTemplate<String, SpaceInfo> spaceTemplate;

	@PostConstruct
	public void postConstruct() {
		RedisSerializer<String> stringSerializer = new StringRedisSerializer();
		RedisSerializer<AppInfo> appSerializer = new Jackson2JsonRedisSerializer<>(AppInfo.class);
		RedisSerializer<OrgInfo> orgSerializer = new Jackson2JsonRedisSerializer<>(OrgInfo.class);
		RedisSerializer<SpaceInfo> spaceSerializer = new Jackson2JsonRedisSerializer<>(SpaceInfo.class);

		appTemplate = new RedisTemplate<>();
		appTemplate.setConnectionFactory(redisConnectionFactory);
		appTemplate.setKeySerializer(stringSerializer);
		appTemplate.setHashKeySerializer(stringSerializer);
		appTemplate.setValueSerializer(appSerializer);
		appTemplate.setHashValueSerializer(appSerializer);

		orgTemplate = new RedisTemplate<>();
		orgTemplate.setConnectionFactory(redisConnectionFactory);
		orgTemplate.setKeySerializer(stringSerializer);
		orgTemplate.setHashKeySerializer(stringSerializer);
		orgTemplate.setValueSerializer(orgSerializer);
		orgTemplate.setHashValueSerializer(orgSerializer);

		spaceTemplate = new RedisTemplate<>();
		spaceTemplate.setConnectionFactory(redisConnectionFactory);
		spaceTemplate.setKeySerializer(stringSerializer);
		spaceTemplate.setHashKeySerializer(stringSerializer);
		spaceTemplate.setValueSerializer(spaceSerializer);
		spaceTemplate.setHashValueSerializer(spaceSerializer);
	}

	public void setRedisConnectionFactory(RedisConnectionFactory factory) {
		this.redisConnectionFactory = factory;
	}

	public AppSummary getAppSummary(String appId) {
		AppInfo appInfo = appTemplate.<String, AppInfo>boundHashOps("apps").get(appId);
		SpaceInfo spaceInfo = spaceTemplate.<String, SpaceInfo>boundHashOps("spaces").get(appInfo.getSpaceId());
		OrgInfo orgInfo = orgTemplate.<String, OrgInfo>boundHashOps("orgs").get(spaceInfo.getOrgId());

		return AppSummary.builder().app(appInfo).space(spaceInfo).org(orgInfo).build();
	}

	@Scheduled(fixedRate = 3600 * 1000L)
	public void populateCache() {
		populateApps(ListApplicationsRequest.builder().build());
		populateSpaces(ListSpacesRequest.builder().build());
		populateOrgs(ListOrganizationsRequest.builder().build());
	}

	private void populateApps(ListApplicationsRequest request) {
		client.applicationsV2().list(request).subscribe(response -> {
			response.getResources().forEach(resource -> {
				AppInfo info = AppInfo.builder()
						.id(resource.getMetadata().getId())
						.name(resource.getEntity().getName())
						.spaceId(resource.getEntity().getSpaceId())
						.build();

				appTemplate.boundHashOps("apps").put(info.getId(), info);
			});

			if (StringUtils.hasText(response.getNextUrl())) {
				URI uri = URI.create(response.getNextUrl());
				String query = uri.getQuery();

				String[] params = query.split("&");
				for (String p : params) {
					if (p.startsWith("page=")) {
						String pageNum = p.substring(5);
						populateApps(ListApplicationsRequest.builder()
								.page(Integer.valueOf(pageNum))
								.build());
						return;
					}
				}
			}
		});
	}

	private void populateSpaces(ListSpacesRequest request) {
		client.spaces().list(request).subscribe(response -> {
			response.getResources().forEach(resource -> {
				SpaceInfo info = SpaceInfo.builder()
						.id(resource.getMetadata().getId())
						.name(resource.getEntity().getName())
						.orgId(resource.getEntity().getOrganizationId())
						.build();

				spaceTemplate.boundHashOps("spaces").put(info.getId(), info);
			});

			if (StringUtils.hasText(response.getNextUrl())) {
				URI uri = URI.create(response.getNextUrl());
				String query = uri.getQuery();

				String[] params = query.split("&");
				for (String p : params) {
					if (p.startsWith("page=")) {
						String pageNum = p.substring(5);
						populateSpaces(ListSpacesRequest.builder()
								.page(Integer.valueOf(pageNum))
								.build());
						return;
					}
				}
			}
		});
	}

	private void populateOrgs(ListOrganizationsRequest request) {
		client.organizations().list(request).subscribe(response -> {
			response.getResources().forEach(resource -> {
				OrgInfo info = OrgInfo.builder()
						.id(resource.getMetadata().getId())
						.name(resource.getEntity().getName())
						.build();

				orgTemplate.boundHashOps("orgs").put(info.getId(), info);
			});

			if (StringUtils.hasText(response.getNextUrl())) {
				URI uri = URI.create(response.getNextUrl());
				String query = uri.getQuery();

				String[] params = query.split("&");
				for (String p : params) {
					if (p.startsWith("page=")) {
						String pageNum = p.substring(5);
						populateOrgs(ListOrganizationsRequest.builder()
								.page(Integer.valueOf(pageNum))
								.build());
						return;
					}
				}
			}
		});
	}
}
