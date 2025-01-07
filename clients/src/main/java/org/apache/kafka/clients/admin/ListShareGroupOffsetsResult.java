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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * The result of the {@link Admin#listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListShareGroupOffsetsResult {

    private final Map<String, KafkaFuture<Map<TopicPartition, Long>>> futures;

    public ListShareGroupOffsetsResult(Map<String, KafkaFuture<Map<TopicPartition, Long>>> futures) {
        this.futures = futures;
    }

    /**
     * Return a future which yields all Map<String, Map<TopicPartition, Long> objects, if requests for all the groups succeed.
     */
    public KafkaFuture<Map<String, Map<TopicPartition, Long>>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).thenApply(
                nil -> {
                    Map<String, Map<TopicPartition, Long>> offsets = new HashMap<>(futures.size());
                    futures.forEach((key, future) -> {
                        try {
                            offsets.put(key, future.get());
                        } catch (InterruptedException | ExecutionException e) {
                            // This should be unreachable, since the KafkaFuture#allOf already ensured
                            // that all the futures completed successfully.
                            throw new RuntimeException(e);
                        }
                    });
                    return offsets;
                });
    }

    /**
     * Return a future which yields a map of topic partitions to offsets for the specified group.
     */
    public KafkaFuture<Map<TopicPartition, Long>> partitionsToOffset(String groupId) {
        KafkaFutureImpl<Map<TopicPartition, Long>> future = new KafkaFutureImpl<>();
        if (futures.containsKey(groupId))
            return futures.get(groupId);
        else
            future.completeExceptionally(new IllegalArgumentException("Group ID not found: " + groupId));
        return future;
    }
}