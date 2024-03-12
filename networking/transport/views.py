from rest_framework.response import Response
from transport.serializers import MessageSerializer
from transport.serializers import SegmentSerializer
from transport.models import Message
from transport.models import Segment
from rest_framework.views import APIView
from django.views.decorators.csrf import csrf_exempt
import base64
import requests
# from kafka import KafkaProducer
# from kafka import KafkaConsumer
import json

Messages = []

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
        channel_service_url = "http://192.168.1.225:8000/dl/"
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
            response = requests.get(channel_service_url,data=data)
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

        # producer = KafkaProducer(bootstrap_servers='localhost:9000')

        data = {
            'id': request.data['time'],
            'segment_data': request.data['segment_data'],
            'segment_len': int(request.data['segment_len']),
            'segment_num': int(request.data['segment_num'])
        }
        # value_to_send = json.dumps(data).encode('utf-8')

        # future = producer.send('product_topic', key=json.dumps(id).encode('utf-8'), value=value_to_send)
        # result = future.get(timeout=120)
        # producer.flush()
        # # producer.send('product_topic', key=b'product_category_id', value=b'product_data')

        Messages.append(data)

        if int(request.data['segment_num']) == int(request.data['segment_len']) - 1:
            file = ''
            # id = data.id
            Messages.sort(key=lambda x: x['segment_num'])
            for i in range(int(request.data['segment_len'])):
                print(i)
            print(file)

        return Response({"message": "success"})


# def Consume():
#     consumer = KafkaConsumer('product_topic')
#     for msg in consumer:
#         print (msg)
#     consumer = KafkaConsumer('product_topic', group_id='discount_product_group')
#     for msg in consumer:
#         print (msg)