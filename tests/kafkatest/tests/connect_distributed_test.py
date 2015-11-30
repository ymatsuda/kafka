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

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.connect import ConnectDistributedService, VerifiableSource, VerifiableSink
from kafkatest.services.console_consumer import ConsoleConsumer
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
import subprocess, itertools, time
from collections import Counter

class ConnectDistributedTest(KafkaTest):
    """
    Simple test of Kafka Connect in distributed mode, producing data from files on one cluster and consuming it on
    another, validating the total output is identical to the input.
    """

    INPUT_FILE = "/mnt/connect.input"
    OUTPUT_FILE = "/mnt/connect.output"

    TOPIC = "test"
    OFFSETS_TOPIC = "connect-offsets"
    CONFIG_TOPIC = "connect-configs"

    # Since tasks can be assigned to any node and we're testing with files, we need to make sure the content is the same
    # across all nodes.
    FIRST_INPUT_LIST = ["foo", "bar", "baz"]
    FIRST_INPUTS = "\n".join(FIRST_INPUT_LIST) + "\n"
    SECOND_INPUT_LIST = ["razz", "ma", "tazz"]
    SECOND_INPUTS = "\n".join(SECOND_INPUT_LIST) + "\n"

    SCHEMA = { "type": "string", "optional": False }

    def __init__(self, test_context):
        super(ConnectDistributedTest, self).__init__(test_context, num_zk=1, num_brokers=1, topics={
            'test' : { 'partitions': 1, 'replication-factor': 1 }
        })

        self.cc = ConnectDistributedService(test_context, 3, self.kafka, [self.INPUT_FILE, self.OUTPUT_FILE])
        self.cc.log_level = "DEBUG"
        self.key_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.value_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.schemas = True

    def test_file_source_and_sink(self):
        """
        Tests that a basic file connector works across clean rolling bounces. This validates that the connector is
        correctly created, tasks instantiated, and as nodes restart the work is rebalanced across nodes.
        """

        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))

        self.cc.start()

        self.logger.info("Creating connectors")
        for connector_props in [self.render("connect-file-source.properties"), self.render("connect-file-sink.properties")]:
            connector_config = dict([line.strip().split('=', 1) for line in connector_props.split('\n') if line.strip() and not line.strip().startswith('#')])
            self.cc.create_connector(connector_config)

        # Generating data on the source node should generate new records and create new output on the sink node. Timeouts
        # here need to be more generous than they are for standalone mode because a) it takes longer to write configs,
        # do rebalancing of the group, etc, and b) without explicit leave group support, rebalancing takes awhile
        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.FIRST_INPUTS) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self._validate_file_output(self.FIRST_INPUT_LIST), timeout_sec=70, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

        # Restarting both should result in them picking up where they left off,
        # only processing new data.
        self.cc.restart()

        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.SECOND_INPUTS) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self._validate_file_output(self.FIRST_INPUT_LIST + self.SECOND_INPUT_LIST), timeout_sec=70, err_msg="Sink output file never converged to the same state as the input file")


    @matrix(clean=[True, False])
    def test_bounce(self, clean):
        """
        Validates that source and sink tasks that run continuously and produce a predictable sequence of messages
        run correctly and deliver messages exactly once when Kafka Connect workers undergo clean rolling bounces.
        """
        num_tasks = 3

        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        self.source = VerifiableSource(self.cc, tasks=num_tasks)
        self.source.start()
        self.sink = VerifiableSink(self.cc, tasks=num_tasks)
        self.sink.start()

        for _ in range(3):
            for node in self.cc.nodes:
                started = time.time()
                self.logger.info("%s bouncing Kafka Connect on %s", clean and "Clean" or "Hard", str(node.account))
                self.cc.stop_node(node, clean_shutdown=clean)
                with node.account.monitor_log(self.cc.LOG_FILE) as monitor:
                    self.cc.start_node(node)
                    monitor.wait_until("Starting connectors and tasks using config offset", timeout_sec=90,
                                       err_msg="Kafka Connect worker didn't successfully join group and start work")
                self.logger.info("Bounced Kafka Connect on %s and rejoined in %f seconds", node.account, time.time() - started)
                # If this is a hard bounce, give additional time for the consumer groups to recover. If we don't give
                # some time here, the next bounce may cause consumers to be shut down before they have any time to process
                # data and we can end up with zero data making it through the test.
                if not clean:
                    time.sleep(15)


        self.source.stop()
        self.sink.stop()
        self.cc.stop()

        # Validate at least once delivery of everything that was reported as written since we should have flushed and
        # cleanly exited. Currently this only tests at least once delivery because the sink task may not have consumed
        # all the messages generated by the source task. This needs to be done per-task since seqnos are not unique across
        # tasks.
        success = True
        errors = []
        allow_dups = not clean
        src_messages = self.source.messages()
        sink_messages = self.sink.messages()
        for task in range(num_tasks):
            # Validate source messages
            src_seqnos = [msg['seqno'] for msg in src_messages if msg['task'] == task]
            # Every seqno up to the largest one we ever saw should appear. Each seqno should only appear once because clean
            # bouncing should commit on rebalance.
            src_seqno_max = max(src_seqnos)
            self.logger.debug("Max source seqno: %d", src_seqno_max)
            src_seqno_counts = Counter(src_seqnos)
            missing_src_seqnos = sorted(set(range(src_seqno_max)).difference(set(src_seqnos)))
            duplicate_src_seqnos = sorted([seqno for seqno,count in src_seqno_counts.iteritems() if count > 1])

            if missing_src_seqnos:
                self.logger.error("Missing source sequence numbers for task " + str(task))
                errors.append("Found missing source sequence numbers for task %d: %s" % (task, missing_src_seqnos))
                success = False
            if not allow_dups and duplicate_src_seqnos:
                self.logger.error("Duplicate source sequence numbers for task " + str(task))
                errors.append("Found duplicate source sequence numbers for task %d: %s" % (task, duplicate_src_seqnos))
                success = False


            # Validate sink messages
            sink_seqnos = [msg['seqno'] for msg in sink_messages if msg['task'] == task and 'flushed' in msg]
            # Every seqno up to the largest one we ever saw should appear. Each seqno should only appear once because
            # clean bouncing should commit on rebalance.
            sink_seqno_max = max(sink_seqnos)
            self.logger.debug("Max sink seqno: %d", sink_seqno_max)
            sink_seqno_counts = Counter(sink_seqnos)
            missing_sink_seqnos = sorted(set(range(sink_seqno_max)).difference(set(sink_seqnos)))
            duplicate_sink_seqnos = sorted([seqno for seqno,count in sink_seqno_counts.iteritems() if count > 1])

            if missing_sink_seqnos:
                self.logger.error("Missing sink sequence numbers for task " + str(task))
                errors.append("Found missing sink sequence numbers for task %d: %s" % (task, missing_sink_seqnos))
                success = False
            if not allow_dups and duplicate_sink_seqnos:
                self.logger.error("Duplicate sink sequence numbers for task " + str(task))
                errors.append("Found duplicate sink sequence numbers for task %d: %s" % (task, duplicate_sink_seqnos))
                success = False

            # Validate source and sink match
            if sink_seqno_max > src_seqno_max:
                self.logger.error("Found sink sequence number greater than any generated sink sequence number for task %d: %d > %d", task, sink_seqno_max, src_seqno_max)
                errors.append("Found sink sequence number greater than any generated sink sequence number for task %d: %d > %d" % (task, sink_seqno_max, src_seqno_max))
                success = False
            if src_seqno_max < 1000 or sink_seqno_max < 1000:
                errors.append("Not enough messages were processed: source:%d sink:%d" % (src_seqno_max, sink_seqno_max))
                success = False

        if not success:
            self.mark_for_collect(self.cc)
            # Also collect the data in the topic to aid in debugging
            consumer_validator = ConsoleConsumer(self.test_context, 1, self.kafka, self.source.topic, consumer_timeout_ms=1000, print_key=True)
            consumer_validator.run()
            self.mark_for_collect(consumer_validator, "consumer_stdout")

        assert success, "Found validation errors:\n" + "\n  ".join(errors)



    def _validate_file_output(self, input):
        input_set = set(input)
        # Output needs to be collected from all nodes because we can't be sure where the tasks will be scheduled.
        # Between the first and second rounds, we might even end up with half the data on each node.
        output_set = set(itertools.chain(*[
            [line.strip() for line in self._file_contents(node, self.OUTPUT_FILE)] for node in self.cc.nodes
        ]))
        return input_set == output_set

    def _file_contents(self, node, file):
        try:
            # Convert to a list here or the CalledProcessError may be returned during a call to the generator instead of
            # immediately
            return list(node.account.ssh_capture("cat " + file))
        except subprocess.CalledProcessError:
            return []
