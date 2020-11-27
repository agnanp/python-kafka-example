import sys
import time
import cv2
from kafka import KafkaProducer
from utils.videostreams import *


setting = {
   "address": ("rtsp://*****/streaming/channels/201", #example of ip cctv
                "rtsp://*****/streaming/channels/301", #example of ip cctv
                "rtsp://*****/streaming/channels/101", #example of ip cctv
                "rtsp://*****/streaming/channels/401", #example of ip cctv
                "rtsp://*****/streaming/channels/501" #example of ip cctv
                ), 
   "topics" : ("distributed-video1", "distributed-video2", "distributed-video3", "distributed-video4",  "distributed-video5")
}
    
def publishCamera():
    """
    Publish camera video stream to specified Kafka topic.
    Kafka Server is expected to be running on the localhost. Not partitioned.
    """

    # Start up producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    camera = multiCamera(setting["address"])
    time.sleep(2)
    try:
        while(True):
            #success, frame = camera.read()
            #frame = camera.read()
            frames = camera.capture()
            _, buf0 = cv2.imencode('.jpg', frames[0])
            _, buf1 = cv2.imencode('.jpg', frames[1])
            _, buf2 = cv2.imencode('.jpg', frames[2])
            _, buf3 = cv2.imencode('.jpg', frames[3])
            _, buf4 = cv2.imencode('.jpg', frames[4])

            producer.send(setting["topics"][0], buf0.tobytes())
            time.sleep(0.2)
            producer.send(setting["topics"][1], buf1.tobytes())
            # Choppier stream, reduced load on processor
            time.sleep(0.2)
            producer.send(setting["topics"][2], buf2.tobytes())
            time.sleep(0.2)
            producer.send(setting["topics"][3], buf3.tobytes())
            time.sleep(0.2)
            producer.send(setting["topics"][4], buf4.tobytes())
            time.sleep(0.2)

    except:
        print("\nExiting.")
        sys.exit(1)
        
    camera.stop()


if __name__ == '__main__':
    """
    Producer will publish to Kafka Server a video file given as a system arg. 
    Otherwise it will default by streaming webcam feed.
    """
    print("publishing feed!")
    publishCamera()
