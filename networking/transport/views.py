import datetime
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
import json
from kafka import KafkaProducer, KafkaConsumer
from rest_framework.response import Response
from rest_framework.decorators import api_view
import requests
import threading
import time

channel_layer_url = "http://localhost:8000/dl/"
transport_layer_url = "http://localhost:8000/transfer/"
application_layer_url = "http://localhost:8000/receiveFile/" #5000

topic = "segment_topic"
producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        batch_size=1
    )
consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='segment_topic_group',
        auto_offset_reset='latest',
        enable_auto_commit=True
    )

class Segment:
    def __init__(self, username: str, segment_num: int, segment_len: int, payload: bytes, time: datetime.datetime):
        self.username = username
        self.segment_num = segment_num
        self.segment_len = segment_len
        self.payload = payload
        self.time = time
        # self.segment_error = False

class Message:
    def __init__(self, username: str = "incognito", file: str = None, time: datetime.datetime = datetime.datetime.now()):
        self.username = username
        self.time = time
        # if segments is not None:
        #     self.segments = segments
        #     self.segments.sort(key=lambda s: s.segment_num)
        #     if not self.error:
        #         self.file = b''.join(segment for segment in self.segments)
        # else:
        self.segments = []
        self.file = file

    @property
    def binary_file(self):
        if self.file is not None:
            return self.file.encode('utf-8')
        return None
    @property
    def segments_len(self):
        if self.binary_file:
            file_length = len(self.binary_file)
            return (file_length + 999) // 1000
        return 0
    @property
    def error(self):
        if self.segments:
            if len(self.segments) == int(self.segments[0].segment_len):
                return False
        return True
    def create_file(self):
        if not self.error:
            self.file = b''.join(segment for segment in self.segments)
    def add_segment(self, segment: Segment):
        self.segments.append(segment)
        self.segments.sort(key=lambda s: s.segment_num)
    def __str__(self):
        return f"{self.segments_len}"

@swagger_auto_schema(methods=['post'], request_body=openapi.Schema(type=openapi.TYPE_OBJECT, properties={
        'segment_data': openapi.Schema(type=openapi.TYPE_STRING, description='Тело сообщения'),
        'time': openapi.Schema(type=openapi.TYPE_INTEGER, description='Время отправки сообщения'),
        'username': openapi.Schema(type=openapi.TYPE_STRING, description='Отправитель'),
    }),
    operation_description="Разбить сообщение на сегменты длинной 1000 байт и передать их на канальный уровень"
)
# 1 сегментация
@api_view(['POST'])
def SendSegment(request):
    try:
        cur_message = Message(username=request.data['username'], file=request.data['file'])
    except:
        return Response({"message": "message error"})

    for i in range(cur_message.segments_len):
        data={
            'username': cur_message.username,
            'segment_data': cur_message.binary_file[i * 1000:(i+1) * 1000],
            'time': cur_message.time,
            'segment_len': cur_message.segments_len,
            'segment_num': i,
        }
        response = requests.get(channel_layer_url,data=data)
        # print(data)

    if response.status_code == 200:
        return Response({"message": "ok"}, status=response.status_code)
    else:
        return Response({"message": "error"}, status=response.status_code)


@swagger_auto_schema(methods=['post'], request_body=openapi.Schema(type=openapi.TYPE_OBJECT, properties={
        'segment_num': openapi.Schema(type=openapi.TYPE_INTEGER, description='Номер сегмента'),
        'segment_len': openapi.Schema(type=openapi.TYPE_INTEGER, description='Общее число сегментов'),
        'segment_data': openapi.Schema(type=openapi.TYPE_STRING, description='Тело сегмента'),
        'time': openapi.Schema(type=openapi.TYPE_INTEGER, description='Время отправки сообщения'),
        'username': openapi.Schema(type=openapi.TYPE_STRING, description='Отправитель'),
    }),
    operation_description="Положить сегмент в брокер сообщений Kafka"
)
# 3 метод TransferSegment
@csrf_exempt
@api_view(['POST'])
def TransferSegment(request):
    print("это метод TransferSegment", "\nusername: ", request.data['username'], "\nsegment_num: ", request.data['segment_num'])
    # print(request.data)
    producer.send(topic, request.data)
    return HttpResponse(status=200) 

# 5 отправить на прикладной уровень
def send_mesg_to_app_layer(messages):
    print("отправка на прикладной уровень") #
    for message in messages:
        if not message.error:
            print("send") #
            json_data = {
                "time": message.time,
                "username": message.username,
                "file": message.binary_file,
                "isError": 0
            }
            print(json_data)
            requests.post(application_layer_url, data=json_data)
        else:
            print("lost") #
            json_data = {
                "time": message.time,
                "username": message.username,
                "file": "-",
                "isError": 1
            }
            print(json_data)
            requests.post(application_layer_url, data=json_data)
    return 0

# 4 чтение из кафки
def read_messages_from_kafka(consumer):
    messages = []
    print("начало считывания")
    start_time = time.time()
    print("время: ", start_time)
    for item in consumer:
        time.sleep(7)
        segment = Segment(item.value['username'], item.value['segment_num'], item.value['segment_len'], item.value['segment_data'], item.value['time'])
        if_found = False
        for message in messages:
            if message.time == segment.time and message.username == segment.username:
                message.add_segment(segment)
                if_found = True
                break
        if not if_found:
            message = Message(segment.username, None, None, segment.time)
            message.add_segment(segment)
            messages.append(message)

        now = time.time()
        if now - start_time >= 7:
            print("конец считывания")
            print("считанные соо:\n")
            for m in messages:
                print(m.username)
            print("прошло времени: ", now - start_time)
            send_mesg_to_app_layer(messages)
            print("начало считывания")
            start_time = time.time()
            print("время: ", start_time)
            messages = []


consumer_thread = threading.Thread(target=read_messages_from_kafka, args=(consumer,))
consumer_thread.daemon=True
consumer_thread.start()

# 2 канальный уровень
@api_view(['GET'])
def dl(request):
    data={
        'username': request.data['username'],
        'segment_data': request.data['segment_data'],
        'time': request.data['time'],
        'segment_len': request.data['segment_len'],
        'segment_num': request.data['segment_num'],
    }
    response = requests.post(transport_layer_url,data=data)
    print("это канальный уровень", "\nusername: ", data['username'], "\nsegment_num: ", data['segment_num'])
    # print(data)

    if response.status_code == 200:
        return Response({"message": "ok"}, status=response.status_code)
    else:
        return Response({"message": "error"}, status=response.status_code)

# 6 прикладной уровень
@api_view(['POST'])
def receiveFile(request):
    print("это прикладной уровень")
    # print(request.data)
    return Response({"message": "ok?"}, status=200)