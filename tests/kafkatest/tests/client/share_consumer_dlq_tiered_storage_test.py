# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import Counter

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.verifiable_share_consumer_test import VerifiableShareConsumerTest


class ShareConsumerDLQTieredStorageTest(VerifiableShareConsumerTest):
    """System tests for share group DLQ (KIP-1191) record-copy correctness when the source records
    have been offloaded to tiered storage. Mirrors the scenarios in
    ShareConsumerDLQTest.java#testDlqCopiesRecordsReadFromRemoteStorage /
    #testDlqCopiesRecordsReadFromRemoteAndLocalStorage against a real single-broker cluster with
    LocalTieredStorage wired onto the broker's classpath (via core's testFixtures(':storage')
    dependency).
    """

    num_consumers = 1
    num_producers = 1
    num_brokers = 1

    share_group_id = "dlq-tiered-test-group"
    record_count = 10

    default_timeout_sec = 180

    # Broker properties needed for tiered storage (LocalTieredStorage RSM), mirroring
    # ShareConsumerDLQTest.java's @ClusterTest settings for its tiered-storage scenarios.
    # remote.log.metadata.manager.listener.name is "PLAINTEXT" (not the JUnit suite's "EXTERNAL")
    # because ducktape's KafkaService names its default client listener after its security
    # protocol. remote.log.storage.manager.class.path is filled in later, in setUp(), once the
    # exact test-fixtures jar path is resolved.
    SERVER_PROP_OVERRIDES = [
        ["remote.log.storage.system.enable", "true"],
        ["remote.log.storage.manager.class.name", "org.apache.kafka.server.log.remote.storage.LocalTieredStorage"],
        ["remote.log.manager.task.interval.ms", "500"],
        ["remote.log.metadata.manager.listener.name", "PLAINTEXT"],
        ["rlmm.config.remote.log.metadata.topic.replication.factor", "1"],
        ["rlmm.config.remote.log.metadata.topic.num.partitions", "1"],
        ["log.retention.check.interval.ms", "500"],
        ["log.initial.task.delay.ms", "100"],
    ]

    def __init__(self, test_context):
        super(ShareConsumerDLQTieredStorageTest, self).__init__(test_context, num_consumers=self.num_consumers,
            num_producers=self.num_producers, num_zk=0, num_brokers=self.num_brokers, topics={})

        # DLQ (KIP-1191) is gated behind share.version=2, which is not yet the production default
        # (LATEST_PRODUCTION=SV_1) -- bootstrap the cluster with it explicitly. KafkaTest's constructor
        # already built a throwaway self.kafka above; nothing has started yet, but ducktape allocates
        # cluster nodes at Service.__init__ time (not at start()), so the throwaway service's nodes
        # (and its isolated controller quorum's, if any) must be freed before building the replacement,
        # or the cluster runs out of nodes.
        self.kafka.free()
        if self.kafka.isolated_controller_quorum:
            self.kafka.isolated_controller_quorum.free()
        self.kafka = KafkaService(test_context, self.num_brokers, self.zk, topics=self.topics,
                                   controller_num_nodes_override=self.num_zk, share_version="2",
                                   server_prop_overrides=list(self.SERVER_PROP_OVERRIDES))

    def setUp(self):
        # LocalTieredStorage lives in the storage module's test-fixtures, which core only depends on via
        # testImplementation testFixtures(project(':storage')) (build.gradle) - i.e. it's on core's *test*
        # classpath (core/build/dependant-testlibs/*.jar after copyDependantTestLibs), not the main runtime
        # classpath that bin/kafka-run-class.sh builds for the broker process. remote.log.storage.manager
        # .class.path (a real, supported RSM plugin-loading mechanism - see
        # RemoteLogManager#createRemoteStorageManager) loads it through its own ChildFirstClassLoader
        # instead, without growing the broker's main classpath.
        #
        # That directory also holds the (non-test-fixtures) kafka-storage/kafka-storage-api jars already
        # on the broker's main classpath -- ChildFirstClassLoader's "dir/*" convention loads every jar in
        # the directory, so pointing it at the whole directory reloads RemoteStorageManager itself through
        # the child-first loader too, and every cast between the RSM plugin and the interface then fails
        # with ClassCastException (same class name, two different classloaders). So resolve and use the
        # single test-fixtures jar's exact path instead of the directory wildcard.
        node = self.kafka.nodes[0]
        jar_pattern = "%s/core/build/dependant-testlibs/kafka-storage-*-test-fixtures.jar" % self.kafka.path.home()
        matches = [line.strip() for line in node.account.ssh_capture("ls %s" % jar_pattern) if line.strip()]
        assert matches, "Could not find the storage test-fixtures jar (containing LocalTieredStorage) matching %s " \
                         "-- did the build.gradle testFixtures(project(':storage')) dependency get picked up?" % jar_pattern
        self.kafka.server_prop_overrides.append(["remote.log.storage.manager.class.path", matches[0]])

        super(ShareConsumerDLQTieredStorageTest, self).setUp()

    def create_dlq_topic(self, name):
        self.kafka.create_topic({
            "topic": name,
            "partitions": 1,
            "replication-factor": 1,
            "configs": {"errors.deadletterqueue.group.enable": "true"}
        })

    def create_remote_storage_source_topic(self, name, retention_ms, local_retention_ms):
        self.kafka.create_topic({
            "topic": name,
            "partitions": 1,
            "replication-factor": 1,
            "configs": {
                "remote.storage.enable": "true",
                "retention.ms": retention_ms,
                "local.retention.ms": local_retention_ms,
                # Roll a segment for every record so each inactive segment can be offloaded then
                # deleted locally.
                "index.interval.bytes": 1,
                "segment.index.bytes": 12,
            }
        })

    def setup_dlq_group_config(self, dlq_topic, copy_record_enable=None):
        wait_until(lambda: self.kafka.set_share_group_offset_reset_strategy(group=self.share_group_id, strategy="earliest"),
                   timeout_sec=20, backoff_sec=2, err_msg="share.auto.offset.reset not set to earliest")
        wait_until(lambda: self.kafka.set_share_group_dlq_config(group=self.share_group_id, topic_name=dlq_topic,
                                                                  copy_record_enable=copy_record_enable),
                   timeout_sec=20, backoff_sec=2, err_msg="DLQ config not applied to share group")

    def read_dlq_topic(self, dlq_topic, min_messages, timeout_sec=None):
        """Consume dlq_topic with a plain (non-share) VerifiableConsumer and return the collected record_data events."""
        timeout_sec = timeout_sec or self.default_timeout_sec
        dlq_records = []

        def on_dlq_record(event, node):
            dlq_records.append(event)

        dlq_consumer = VerifiableConsumer(self.test_context, 1, self.kafka, dlq_topic,
                                           group_id="%s-dlq-verifier" % self.share_group_id,
                                           on_record_consumed=on_dlq_record)
        dlq_consumer.start()
        wait_until(lambda: dlq_consumer.total_consumed() >= min_messages, timeout_sec=timeout_sec,
                   err_msg="Timed out waiting to consume %d records from DLQ topic %s" % (min_messages, dlq_topic))
        dlq_consumer.stop_all()
        return dlq_records

    def produce_messages(self, topic, count):
        """Produce exactly `count` messages to `topic` and return the (stopped) producer, so its
        acked_values can be cross-checked against DLQ content afterwards."""
        producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka, topic, max_messages=count,
                                       throughput=500, request_timeout_sec=self.PRODUCER_REQUEST_TIMEOUT_SEC,
                                       log_level="DEBUG")
        producer.start()
        wait_until(lambda: producer.num_acked >= count, timeout_sec=self.default_timeout_sec,
                   err_msg="Timed out waiting for %d records to be produced to %s" % (count, topic))
        producer.stop()
        return producer

    def await_all_rejected(self, consumer, count):
        wait_until(lambda: consumer.total_rejected() >= count, timeout_sec=self.default_timeout_sec,
                   err_msg="Timed out waiting for all records to be rejected")

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_tiered_topic_dlq_reject(self, metadata_quorum=quorum.isolated_kraft):
        """Every record is offloaded to remote storage and deleted locally before being REJECTed; with
        record copy enabled, the DLQ record's value can only match the original if the DLQ fetcher
        pulled it back from remote storage."""
        dlq_topic = "dlq.tiered-reject"
        source_topic = "dlq-tiered-source"
        self.create_dlq_topic(dlq_topic)
        self.create_remote_storage_source_topic(source_topic, retention_ms=45_000, local_retention_ms=5_000)
        self.setup_dlq_group_config(dlq_topic, copy_record_enable=True)

        producer = self.produce_messages(source_topic, self.record_count)

        # The last record stays in the active (never-offloaded) segment, so the earliest local offset
        # should reach record_count - 1 once the earlier segments are tiered and removed locally.
        wait_until(lambda: self.kafka.earliest_local_offset(source_topic, 0) >= self.record_count - 1,
                   timeout_sec=self.default_timeout_sec, backoff_sec=1,
                   err_msg="Source records were not tiered to remote storage and removed locally in time")

        consumer = self.setup_share_group(source_topic, group_id=self.share_group_id,
                                           acknowledgement_mode="sync", ack_pattern=["reject"])
        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)
        self.await_all_rejected(consumer, self.record_count)
        consumer.stop_all()

        dlq_records = self.read_dlq_topic(dlq_topic, self.record_count)
        assert len(dlq_records) == self.record_count

        actual_values = Counter(int(record["value"]) for record in dlq_records)
        expected_values = Counter(producer.acked_values)
        assert actual_values == expected_values, \
            "DLQ record values did not match the original produced values read back from remote storage"

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_mixed_tiered_and_local_dlq_reject(self, metadata_quorum=quorum.isolated_kraft):
        """First batch is tiered and deleted locally, second batch stays local; both are REJECTed and
        must be copied to the DLQ correctly regardless of whether the source record was fetched from
        remote or local storage."""
        dlq_topic = "dlq.mixed-tiered-reject"
        source_topic = "dlq-mixed-tiered-source"
        self.create_dlq_topic(dlq_topic)
        self.create_remote_storage_source_topic(source_topic, retention_ms=45_000, local_retention_ms=10_000)
        self.setup_dlq_group_config(dlq_topic, copy_record_enable=True)

        tiered_producer = self.produce_messages(source_topic, self.record_count)

        wait_until(lambda: self.kafka.earliest_local_offset(source_topic, 0) >= self.record_count - 1,
                   timeout_sec=self.default_timeout_sec, backoff_sec=1,
                   err_msg="Source records were not tiered to remote storage and removed locally in time")

        # Second batch stays in the active (local-only) segment for the rest of the test.
        local_producer = self.produce_messages(source_topic, self.record_count)

        total = self.record_count * 2
        consumer = self.setup_share_group(source_topic, group_id=self.share_group_id,
                                           acknowledgement_mode="sync", ack_pattern=["reject"])
        consumer.start()
        self.await_all_members(consumer, timeout_sec=self.default_timeout_sec)
        self.await_all_rejected(consumer, total)

        # Confirm the second batch's offsets were still local at reject time (not yet tiered), so this
        # test actually exercised the local-storage read path for those records - checked right after
        # the reject, before the (slower, consumer-group-based) DLQ read below, since local_retention_ms
        # eventually tiers this batch too and a check made later could lose that race.
        assert self.kafka.earliest_local_offset(source_topic, 0) < total - 1, \
            "Offsets from the second (local-only) produce batch were unexpectedly tiered before being rejected"

        consumer.stop_all()

        dlq_records = self.read_dlq_topic(dlq_topic, total)
        assert len(dlq_records) == total

        actual_values = Counter(int(record["value"]) for record in dlq_records)
        expected_values = Counter(tiered_producer.acked_values) + Counter(local_producer.acked_values)
        assert actual_values == expected_values, \
            "DLQ record values did not match the original produced values for the mixed tiered/local batch"
