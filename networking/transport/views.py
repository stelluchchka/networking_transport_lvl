from django.shortcuts import render
from rest_framework.response import Response
from django.shortcuts import get_object_or_404
from rest_framework import status
from transport.serializers import MessageSerializer
from transport.serializers import SegmentSerializer
from transport.models import Message
from transport.models import Segment
from rest_framework.views import APIView
from rest_framework.decorators import api_view
from django.views import View
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.files.uploadedfile import InMemoryUploadedFile
import base64

# import requests

# @api_view(['Get'])
# def get(self, format=None):
#     message = Message.objects.all()
#     serializer = MessageSerializer(message, many=True)

#     return Response(serializer.data)


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
        for i in range(cur_message.segments_len):
            cur_message.add_segment(i, base64.b64decode(cur_message.binary_file[i * 1000:(i+1) * 1000]))

        message = Message.objects.latest('id')
        segments = Segment.objects.filter(message=message).order_by('segment_num')
        serializer = SegmentSerializer(segments, many=True) #вывод всех сегментов
        
        # messages = Message.objects.all()       # вывод всех сообщений
        # serializer = MessageSerializer(message)
        if message.error == False:
            return Response(serializer.data)
        else:
            return Response(serializer.errors)


        # binary_data = b''.join(segment.payload for segment in segments)

        # file_content = InMemoryUploadedFile(
        #     file=None,
        #     field_name=None,
        #     name='combined_file',
        #     content_type='application/octet-stream',
        #     size=len(binary_data),
        #     charset=None,
        # )
        # file_content.file = BytesIO(binary_data)  # Присваиваем байтовые данные
        # new_message = Message(                    # сообщение из соединенных сегментов
        #     user=cur_message.user,
        #     time=message.time,
        #     file=file_content,
        #     error=message.error,
        # )
        # new_message.save()
        # serializer = MessageSerializer(new_message)   # вывод 1 сообщения
        # return Response(serializer.data)


#response = requests.post(url, files=data)           #!!!!!!!
                            # Проверяем ответ
                # if response.status_code !=  200:
                #     print(f"Error sending segment {i +  1}: {response.text}")
        
class TransferSegmentView(APIView):
    def get(self, request):
        message = Message.objects.all()
        serializer = MessageSerializer(message, many=True)
        return Response(serializer.data)