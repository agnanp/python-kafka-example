import cv2
from kafka import KafkaConsumer
import numpy as np
from time import sleep

class Consumers():
    def __init__(self, topic):
        self.topics = topic
        self.consumers = []
        for con in self.topics:
            consumer = KafkaConsumer(
                        con, 
                        bootstrap_servers=['localhost:9092'])
            self.consumers.append(consumer)

    def get_video_stream(self, cons):
        consume = cons
        for msg in consume:
            yield msg.value
    
    def decodeFrame(self, cons):
        for c in self.get_video_stream(cons):
            data = np.frombuffer(c, dtype='uint8')
            img = cv2.imdecode(data, cv2.IMREAD_UNCHANGED)
            sleep(0.2)
            return img