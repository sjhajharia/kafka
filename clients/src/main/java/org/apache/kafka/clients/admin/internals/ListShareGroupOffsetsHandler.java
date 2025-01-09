/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ListShareGroupOffsetsHandler extends AdminApiHandler.Batched<CoordinatorKey, Map<TopicPartition, Long>> {

    private final Map<String, ListShareGroupOffsetsSpec> groupSpecs;
    private final Logger log;
    private final CoordinatorStrategy lookupStrategy;

    public ListShareGroupOffsetsHandler(
        Map<String, ListShareGroupOffsetsSpec> groupSpecs,
        LogContext logContext
    ) {
        this.log = logContext.logger(ListShareGroupOffsetsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(FindCoordinatorRequest.CoordinatorType.GROUP, logContext);
        this.groupSpecs = groupSpecs;
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Long>> newFuture(Collection<String> coordinatorKeys) {
        return AdminApiFuture.forKeys(coordinatorKeys(coordinatorKeys));
    }

    @Override
    public String apiName() {
        return "listShareGroupOffsets";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    private void validateKeys(Set<CoordinatorKey> coordinatorKeys) {
        Set<CoordinatorKey> keys = coordinatorKeys(groupSpecs.keySet());
        if (!keys.containsAll(coordinatorKeys)) {
            throw new IllegalArgumentException("Received unexpected group ids " + coordinatorKeys +
                " (expected among " + keys + ")");
        }
    }

    @Override
    public OffsetFetchRequest.Builder buildBatchedRequest(int brokerId, Set<CoordinatorKey> coordinatorKeys) {
        // Create a map that only contains the consumer groups owned by the coordinator.
        Map<String, List<TopicPartition>> coordinatorGroupIdToTopicPartitions = new HashMap<>(coordinatorKeys.size());
        coordinatorKeys.forEach(g -> {
            ListShareGroupOffsetsSpec spec = groupSpecs.get(g.idValue);
            List<TopicPartition> partitions = spec.topicPartitions() != null ? List.copyOf(spec.topicPartitions()) : null;
            coordinatorGroupIdToTopicPartitions.put(g.idValue, partitions);
        });

        return new OffsetFetchRequest.Builder(coordinatorGroupIdToTopicPartitions, false, false);
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, Long>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> coordinatorKeys,
        AbstractResponse abstractResponse
    ) {
        validateKeys(coordinatorKeys);

        final OffsetFetchResponse response = (OffsetFetchResponse) abstractResponse;

        Map<CoordinatorKey, Map<TopicPartition, Long>> completed = new HashMap<>();
        Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        List<CoordinatorKey> unmapped = new ArrayList<>();
        for (CoordinatorKey coordinatorKey : coordinatorKeys) {
            String group = coordinatorKey.idValue;
            if (response.groupHasError(group)) {
                handleGroupError(CoordinatorKey.byGroupId(group), response.groupLevelError(group), failed, unmapped);
            } else {
                final Map<TopicPartition, Long> groupOffsetsListing = new HashMap<>();
                Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = response.partitionDataMap(group);
                for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> partitionEntry : responseData.entrySet()) {
                    final TopicPartition topicPartition = partitionEntry.getKey();
                    OffsetFetchResponse.PartitionData partitionData = partitionEntry.getValue();
                    final Errors error = partitionData.error;

                    if (error == Errors.NONE) {
                        final long offset = partitionData.offset;
                        groupOffsetsListing.put(topicPartition, offset);
                    } else {
                        log.warn("Skipping return offset for {} due to error {}.", topicPartition, error);
                    }
                }
                completed.put(CoordinatorKey.byGroupId(group), groupOffsetsListing);
            }
        }
        return new ApiResult<>(completed, failed, unmapped);
    }

    private static Set<CoordinatorKey> coordinatorKeys(Collection<String> coordinatorKeys) {
        return coordinatorKeys.stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet());
    }


    private void handleGroupError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        List<CoordinatorKey> groupsToUnmap
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
            case GROUP_ID_NOT_FOUND:
                log.debug("`OffsetFetch` request for group id {} failed due to error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
                break;
            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`OffsetFetch` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`OffsetFetch` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            default:
                log.error("`OffsetFetch` request for group id {} failed due to unexpected error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
        }
    }
}
