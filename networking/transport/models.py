from django.db import models
import base64

class Message(models.Model):
    user = models.CharField(max_length=50, default="incognito")
    time = models.DateTimeField(auto_now=True)
    file = models.FileField(blank=True, upload_to='message_files/')
    error = models.BooleanField(default=False)

    @property
    def binary_file(self):
        if self.file:
            self.file.seek(0)
            binary_data = self.file.read()
            self.file.seek(0)
            # return binary_data
            return base64.b64encode(binary_data).decode('utf-8')
        return None
    @property
    def segments_len(self):
        if self.binary_file:
            file_length = len(self.binary_file)
            return (file_length + 999) // 1000
        return 0

    def __str__(self):
        return f'{self.segments_len}' 

    def add_segment(self, segment_num, payload):
        segment = self.segments.create(
            message=self,
            segment_num=segment_num,
            payload=payload,
            time=self.time
        )
        segment.save()
        # self.check_and_complete()

    # def check_and_complete(self):
    #     if self.segments.filter(segment_error=True).exists():  # есть хотя бы 1 пакет с ошибкой
    #         self.error = True
    #         self.save()
    #     else:
    #         segments = self.segments.order_by('segment_num')
    #         self.binary_file = b''.join(segment.payload for segment in segments)
    #         self.save()


class Segment(models.Model):
    message = models.ForeignKey('Message', on_delete=models.CASCADE, related_name='segments')
    payload = models.BinaryField()
    time = models.DateTimeField()      # id 
    segment_num = models.PositiveIntegerField()
    segment_error = models.BooleanField(default=False)