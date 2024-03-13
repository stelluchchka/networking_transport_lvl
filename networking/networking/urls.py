from django.contrib import admin
from django.urls import path
from transport.views import SendSegmentView
from transport.views import TransferSegmentView
from transport.views import ConsumerSegmentView
from django.conf import settings
from django.conf.urls.static import static


urlpatterns = [
    # path(r'messages/', views.get, name='get message'),
    path('segmentation/', SendSegmentView.as_view(), name='segmentation'),
    path('transfer/', TransferSegmentView.as_view(), name='transfer'),
    path('consume/', ConsumerSegmentView.as_view(), name='transfer'),
    path('admin/', admin.site.urls),
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
