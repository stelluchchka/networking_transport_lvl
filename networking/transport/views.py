from rest_framework.response import Response
from rest_framework.views import APIView
from django.views.decorators.csrf import csrf_exempt
import base64
import requests
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
from rest_framework.decorators import api_view
from drf_yasg.utils import swagger_auto_schema
from rest_framework.decorators import api_view
from drf_yasg import openapi

import base64
import datetime
from typing import Optional

class Segment:
    def __init__(self, segment_num: int, payload: bytes, time: datetime.datetime):
        self.segment_num = segment_num
        self.payload = payload
        self.time = time
        self.segment_error = False


class Message:
    def __init__(self, user: str = "incognito", file: Optional[bytes] = None):
        self.user = user
        self.file = file
        self.time = datetime.datetime.now()
        self.error = False
    @property
    def binary_file(self):
        if self.file is not None:
            return base64.b64encode(self.file).decode("utf-8")
        return None
    @property
    def segments_len(self):
        if self.binary_file:
            file_length = len(self.binary_file)
            return (file_length + 999) // 1000
        return 0
    def __str__(self):
        return f"{self.segments_len}"
    @property
    def segments(self):
        segments = []
        for i in range(self.segments_len):
            segments.append(i, base64.b64decode((self.binary_file[i * 1000:(i+1) * 1000])), time=self.time)


@swagger_auto_schema(methods=['post'], request_body=openapi.Schema(type=openapi.TYPE_OBJECT, properties={
        'segment_data': openapi.Schema(type=openapi.TYPE_STRING, description='Тело сообщения'),
        'time': openapi.Schema(type=openapi.TYPE_INTEGER, description='Время отправки сообщения'),
        'user': openapi.Schema(type=openapi.TYPE_STRING, description='Отправитель'),
    }),
    operation_description="Разбить сообщение на сегменты длинной 1000 байт и передать их на канальный уровень"
)
@api_view(['POST'])
def SendSegment(request):

    try:
        print(request.data['user'])
        print(request.data['file'])
        cur_message = Message(user=request.data['user'], file=request.data['file'])
    except:
        return Response({"message": "message error"})
    print(cur_message.segments_len) # segments_len

    # channel_service_url = "http://192.168.95.40:8000/dl/"

    for i in range(cur_message.segments_len):
        data={
            'segment_data': cur_message.segments[i].payload,
            'time': cur_message.time,
            'segment_len': cur_message.segments_len,
            'segment_num': i,
        }
        # response = requests.get(channel_service_url,data=data)
        # print(response.status_code)
        print(data)

    # if response.status_code == 200:
    #     return Response({"message": "ok"}, status=response.status_code)
    # else:
    #     return Response({"message": "error"}, status=response.status_code)
    return 0

    # print(request.data)   segment_data time segment_len segment_num    



@swagger_auto_schema(methods=['post'], request_body=openapi.Schema(type=openapi.TYPE_OBJECT, properties={
        'segment_num': openapi.Schema(type=openapi.TYPE_INTEGER, description='Номер сегмента'),
        'segment_len': openapi.Schema(type=openapi.TYPE_INTEGER, description='Общее число сегментов'),
        'segment_data': openapi.Schema(type=openapi.TYPE_STRING, description='Тело сегмента'),
        'time': openapi.Schema(type=openapi.TYPE_INTEGER, description='Время отправки сообщения'),
        'user': openapi.Schema(type=openapi.TYPE_STRING, description='Отправитель'),
    }),
    operation_description="Положить сегмент в брокер сообщений Kafka"
)
@csrf_exempt
@api_view(['POST'])
def TransferSegment(request):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    data = {
        'id': request.data['time'],
        'segment_data': request.data['segment_data'],
        'segment_len': int(request.data['segment_len']),
        'segment_num': int(request.data['segment_num'])
    }

    value_to_send = json.dumps(data).encode('utf-8')

    future = producer.send('segment_topic', key=json.dumps(data['id']).encode('utf-8'), value=value_to_send)
    result = future.get(timeout=120)
    producer.flush()
    producer.send('segment_topic', key=b'segment_category_id', value=b'data')
    return Response({"message": "success"})

class ConsumerSegmentView(APIView):
    @csrf_exempt
    def post(self, request):
        consumer = KafkaConsumer('segment_topic')
                #     consumer = KafkaConsumer(
                # 'segment_topic',
                # bootstrap_servers='localhost:9092', # Укажите адрес вашего Kafka брокера
                # group_id='discount_segment_group',
                # auto_offset_
        for msg in consumer:
            print (msg)
        consumer = KafkaConsumer('segment_topic', group_id='discount_segment_group')
        for msg in consumer:
            print (msg)
    
def consume_kafka_messages(topic, group_id=None):
    kafka_config = {
        'bootstrap_servers': 'localhost:9092',
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': False,
        'group_id': group_id,
    }
    
    consumer = KafkaConsumer(topic, **kafka_config)
    
    for msg in consumer:
        print(msg)
        # Здесь можно добавить обработку сообщения
    
    consumer.close()

class ConsumerSegmentView(APIView):
    @csrf_exempt
    def post(self, request):
        consume_kafka_messages('segment_topic', group_id='discount_segment_group')
        return Response({"message": "Kafka messages consumed successfully."})