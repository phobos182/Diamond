"""
Send metrics to [kafka](http://http://incubator.apache.org/kafka/)
"""

from Handler import Handler
from brod import zk
import brod
import time

class KafkaProducerHandler(Handler):
    """
    Implements the abstract Handler class, sending data to kafka
    """
    ATTEMPTS = 0
    RETRY = 3

    def __init__(self, config=None):
        """
        Create a new instance of the KafkaProducerHandler class
        """
        # Initialize Handler
        Handler.__init__(self, config)

        # Initialize Options
        self.conn = None
        self.batch = []
        self.batch_size = int(self.config['batch'])
        self.topic = self.config['topic']
        self.timeout = int(self.config['timeout'])
        self.zookeeper = self.config['zookeeper']

        # Connect to Kafka
        self._connect()

    def process(self, metric):
        """
        Process a metric by sending it to Kafka
        """
        # Acquire lock
        self.lock.acquire()
        # Add the metric to the batch. Remove newlines
        self.batch.append(str(metric).rstrip())
        # If there are sufficient metrics, then send
        if len(self.batch) >= self.batch_size:
            # Send batch request
            self._send_batch()
        # Release lock
        self.lock.release()

    def flush(self):
        self.log.debug("KafkaProducerHandler: Final batch data send")
        # Acquire lock
        self.lock.acquire()
        # Send batch request
        self._send_batch()
        # Release lock
        self.lock.release()

    def _send_batch(self):
        # If anything is in the batch, then send it
        if len(self.batch) > 0:
            self.log.debug("KafkaProducerHandler: Sending batch data. batch size: %d/%d" % (len(self.batch), self.batch_size))
            self._send(self.batch)
            # Clear batch
            self.batch = []

    def _backoff(self, failed):
        sleep = (2 ** failed)
        self.log.error("KafkaProducerHandler: Exponential backoff. Sleeping %d seconds" % (sleep))
        time.sleep(sleep)

    def _send(self, data):
        """
        Send data to Kafka.
        """
        attempts = self.ATTEMPTS
        # Attempt to send any data in the batch
        while attempts < self.RETRY:
            # Check if Kafka is connected
            if not self.conn:
                # Log Error
                self.log.error("KafkaProducerHandler: Kafka unavailable.")
                # Attempt to restablish connection
                self._connect()
                # Increment attempts
                attempts += 1
                # Exponential backoff
                self._backoff(attempts)
                # Try again
                continue
            try:
                # Send data to Kafka
                self.conn.send(data)
                break
            except kafka.error, e:
                # Log Error
                self.log.error("KafkaProducerHandler: Failed sending data. %s." % (e))
                # Attempt to restablish connection
                self._close()
                self._connect()
                # Increment attempts
                attempts += 1
                # Exponential backoff
                self._backoff(attempts)
                # try again
                continue

    def _connect(self):
        """
        Connect to the Kafka server
        """
        # Create Kafka Producer
        try:
            self.conn = zk.ZKProducer(self.zookeeper, self.topic)
            self.log.debug("Established connection to Kafka server %s topic %s" % (self.zookeeper, self.topic))
        except Exception, ex:
            self.log.error("KafkaProducerHandler: Failed to connect to %s topic %s. %s" % (self.zookeeper, self.topic, ex))
            self._close()
            return

    def _close(self):
        """
        Close the Kafka connection
        """
        if self.conn:
            self.conn.close()
