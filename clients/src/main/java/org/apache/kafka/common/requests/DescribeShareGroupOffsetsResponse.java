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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DescribeShareGroupOffsetsResponse extends AbstractResponse {
    private final DescribeShareGroupOffsetsResponseData data;

    public DescribeShareGroupOffsetsResponse(DescribeShareGroupOffsetsResponseData data) {
        super(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS);
        this.data = data;
    }

    @Override
    public DescribeShareGroupOffsetsResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        data.responses().forEach(
                result -> result.partitions().forEach(
                        partitionResult -> updateErrorCounts(counts, Errors.forCode(partitionResult.errorCode()))
                )
        );
        return counts;
    }

    @Override
    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        // No op
    }

    public static DescribeShareGroupOffsetsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeShareGroupOffsetsResponse(
                new DescribeShareGroupOffsetsResponseData(new ByteBufferAccessor(buffer), version)
        );
    }
}
