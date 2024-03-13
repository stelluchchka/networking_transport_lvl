from rest_framework.response import Response
from transport.serializers import MessageSerializer
from transport.serializers import SegmentSerializer
from transport.models import Message
from transport.models import Segment
from rest_framework.views import APIView
from django.views.decorators.csrf import csrf_exempt
import base64
import requests
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
from PIL import Image
import io

class SendSegmentView(APIView):
    def get(self, request):
        message = Message.objects.all()
        serializer = MessageSerializer(message, many=True)
        return Response(serializer.data)

    @csrf_exempt
    def post(self, request):
        serializer = MessageSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors)
        cur_message = serializer.save()
        print(cur_message.segments_len)
        channel_service_url = "http://localhost:8000/transfer/"
        for i in range(cur_message.segments_len):
            cur_message.add_segment(i, base64.b64decode((cur_message.binary_file[i * 1000:(i+1) * 1000])))
            data={
                'segment_data': cur_message.binary_file[i * 1000:(i+1) * 1000],
                'time': cur_message.time,
                'segment_len': cur_message.segments_len,
                'segment_num': i,
            }
            # print(len(''.join([f'{j:08b}' for j in base64.b64decode(cur_message.binary_file[i * 1000:(i+1) * 1000])])))
            print(len(cur_message.binary_file[i * 1000:(i+1) * 1000]))
            response = requests.post(channel_service_url,data=data)
            # print(response.status_code)
        message = Message.objects.latest('id')
        segments = Segment.objects.filter(message=message).order_by('segment_num')
        serializer = SegmentSerializer(segments, many=True) #вывод всех сегментов
        
        # messages = Message.objects.all()       # вывод всех сообщений
        # serializer = MessageSerializer(message)
        if response.status_code == 200:
            return Response(serializer.data)
        else:
            return Response({"message": "error"}, status=response.status_code)

        # print(request.data)   segment_data time segment_len segment_num       
class TransferSegmentView(APIView):
    @csrf_exempt
    def post(self, request):
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

# class ConsumerSegmentView(APIView):
#     @csrf_exempt
#     def post(self, request):
# # def Consume():
#         consumer = KafkaConsumer('segment_topic')
#             #     consumer = KafkaConsumer(
#             # 'segment_topic',
#             # bootstrap_servers='localhost:9092', # Укажите адрес вашего Kafka брокера
#             # group_id='discount_segment_group',
#             # auto_offset_

#         for msg in consumer:
#             print (msg)
#         consumer = KafkaConsumer('segment_topic', group_id='discount_segment_group')
#         for msg in consumer:
#             print (msg)
    
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