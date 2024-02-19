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

# import requests

# @api_view(['Get'])
# def get(self, format=None):
#     message = Message.objects.all()
#     serializer = MessageSerializer(message, many=True)

#     return Response(serializer.data)


class SegmentView(APIView):
    def get(self, request):
        message = Message.objects.all()
        serializer = MessageSerializer(message)
        return Response(serializer.data)

    @csrf_exempt
    def post(self, request):
        serializer = MessageSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors)
        new_message = serializer.save()
        for i in range(new_message.segments_len):
            new_message.add_segment(i, new_message.file.read(1000))

        message = Message.objects.latest('id')
        segments =  Segment.objects.filter(message=message)
        serializer = SegmentSerializer(segments, many=True)
        
        # message = Message.objects.all()       вывод всех сообщений
        # serializer = MessageSerializer(message)
        if message.error == False:
            return Response(serializer.data)
        else:
            return Response(serializer.errors)


#response = requests.post(url, files=data)           #!!!!!!!
                            # Проверяем ответ
                # if response.status_code !=  200:
                #     print(f"Error sending segment {i +  1}: {response.text}")