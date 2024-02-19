from transport.models import Message
from transport.models import Segment
from rest_framework import serializers

class MessageSerializer(serializers.ModelSerializer):
    class Meta:
        model = Message
        fields = "__all__"

class SegmentSerializer(serializers.ModelSerializer):
    segment_len = serializers.SerializerMethodField()

    class Meta:
        model = Segment
        fields = ['payload', 'time', 'segment_len', 'segment_num']

    def get_segment_len(self, obj):
        return obj.message.segments_len