from kafka import KafkaProducer
import logging
import re


class KafkaLoggingHandler(logging.Handler):
    '''
    Class to provide logging handler
    How to Use:
    step1: define the SERVER and TOPIC need to send
        KAFKA_SERVER = 'kafka:9092'
        TOPIC = 'kafka_log'
    step2: instantiate the handler and add into logger
        logger = logging.getLogger("")
        kafka_handler = KafkaLoggingHandler(host=KAFKA_SERVER, topic=TOPIC)
        logger.addHandler(kafka_handler)
    '''

    def __init__(self, host, topic, key=None):
        logging.Handler.__init__(self)
        self.kafka_topic = topic
        self.key = key
        self.producer = KafkaProducer(bootstrap_servers=host)

    def emit(self, record):
        # drop kafka logging to avoid infinite recursion
        if re.match('kafka', record.name):
            return
        try:
            msg = self.format(record).encode('utf-8')
            if self.key is None:
                self.producer.send(self.kafka_topic, msg)
            else:
                self.producer.send(self.kafka_topic, self.key, msg)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def close(self):
        self.producer.close()
        logging.Handler.close(self)


if __name__ == "__main__":

    logger = logging.getLogger("")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(kh)
    logger.info("The %s boxing wizards jump %s" % (5, "quickly"))
