import base64
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
from typing import Optional

# topic = "segment_topic"
channel_layer_url = "http://192.168.95.40:8000/dl/"
application_layer_url = "http://192.168.95.40:5000/receiveFile/"
# producer = KafkaProducer(
#         bootstrap_servers=['localhost:9092'],
#         value_serializer=lambda x: json.dumps(x).encode('utf-8'),
#         batch_size=1
#     )

class Segment:
    def __init__(self, segment_num: int, payload: bytes, time: datetime.datetime):
        self.segment_num = segment_num
        self.payload = payload
        self.time = time
        self.segment_error = False


class Message:
    def __init__(self, username: str = "incognito", file: str = None):
        self.username = username
        self.file = file
        self.time = datetime.datetime.now()
        self.error = False
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
    def __str__(self):
        return f"{self.segments_len}"


@swagger_auto_schema(methods=['post'], request_body=openapi.Schema(type=openapi.TYPE_OBJECT, properties={
        'segment_data': openapi.Schema(type=openapi.TYPE_STRING, description='Тело сообщения'),
        'time': openapi.Schema(type=openapi.TYPE_INTEGER, description='Время отправки сообщения'),
        'username': openapi.Schema(type=openapi.TYPE_STRING, description='Отправитель'),
    }),
    operation_description="Разбить сообщение на сегменты длинной 1000 байт и передать их на канальный уровень"
)
@api_view(['POST'])
def SendSegment(request):
    try:
        cur_message = Message(username=request.data['username'], file=request.data['file'])
    except:
        return Response({"message": "message error"})

    for i in range(cur_message.segments_len):
        data={
            'segment_data': cur_message.binary_file[i * 1000:(i+1) * 1000],
            'time': cur_message.time,
            'segment_len': cur_message.segments_len,
            'segment_num': i,
        }
        response = requests.get(channel_layer_url,data=data)
        # print(response.status_code)
        print(data)

    if response.status_code == 200:
        return Response({"message": "ok"}, status=response.status_code)
    else:
        return Response({"message": "error"}, status=response.status_code)

    # print(request.data)   segment_data time segment_len segment_num    








# @swagger_auto_schema(methods=['post'], request_body=openapi.Schema(type=openapi.TYPE_OBJECT, properties={
#         'segment_num': openapi.Schema(type=openapi.TYPE_INTEGER, description='Номер сегмента'),
#         'segment_len': openapi.Schema(type=openapi.TYPE_INTEGER, description='Общее число сегментов'),
#         'segment_data': openapi.Schema(type=openapi.TYPE_STRING, description='Тело сегмента'),
#         'time': openapi.Schema(type=openapi.TYPE_INTEGER, description='Время отправки сообщения'),
#         'username': openapi.Schema(type=openapi.TYPE_STRING, description='Отправитель'),
#     }),
#     operation_description="Положить сегмент в брокер сообщений Kafka"
# )
# @csrf_exempt
# @api_view(['POST'])
# def TransferSegment(request):
#     producer.send(topic, request.data)
#     return HttpResponse(status=200) 


# def send_mesg_to_app_layer(time, sender, text, isError):
#     json_data = {
#         "time": time,
#         "sender": sender,
#         "text": text,
#         "isError": isError
#     } 
#     requests.post(application_service_url, json=json_data)

#     return 0

# def read_messages_from_kafka(consumer):
#     message_recieved = []
#     while True:
#         for message in consumer:
#             message_str = message.value
#             if (not len(message_recieved) or message_recieved[-1]['dispatch_time'] == message_str['dispatch_time']):
#                 message_recieved.append(message_str)
#                 if (message_str['segment_num'] == 0):
#                     if (message_str['segments_len'] == len(message_recieved)):
#                         sorted_message = sorted(message_recieved, key=lambda x: x['segment_num'], reverse=True)
#                         msg = ""
#                         for i in range(len(sorted_message)):
#                             msg += sorted_message[i]['segment_data']

#                         send_mesg_to_app_layer(message_str['dispatch_time'], message_str['sender'], msg, 0)
#                     else:
#                         send_mesg_to_app_layer(message_str['dispatch_time'], message_str['sender'], "Error", 1)
#                     message_recieved = []
#             else:
#                 send_mesg_to_app_layer(message_recieved[-1]['dispatch_time'], message_recieved[-1]['sender'], "Error", 1)
#                 message_recieved = []
#                 message_recieved.append(message_str)
#                 print("Lost segment")
            

# consumer = KafkaConsumer(
#         topic,
#         bootstrap_servers=['localhost:9092'],
#         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
#         group_id='test',
#         auto_offset_reset='earliest',
#         enable_auto_commit=True
#     )

# consumer_thread = threading.Thread(target=read_messages_from_kafka, args=(consumer,))

# consumer_thread.start()