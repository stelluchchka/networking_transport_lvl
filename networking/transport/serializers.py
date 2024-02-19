from transport.models import Message
from transport.models import Segment
from rest_framework import serializers

class MessageSerializer(serializers.ModelSerializer):
    binary_file = serializers.SerializerMethodField()

    class Meta:
        model = Message
        fields = ['user', 'time', 'file', 'binary_file']

    def get_binary_file(self, obj):
        return obj.binary_file

class SegmentSerializer(serializers.ModelSerializer):
    segment_len = serializers.SerializerMethodField()

    class Meta:
        model = Segment
        fields = ['payload', 'time', 'segment_len', 'segment_num']

    def get_segment_len(self, obj):
        return obj.message.segments_len