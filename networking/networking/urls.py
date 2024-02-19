from django.contrib import admin
from django.urls import path
from transport.views import SegmentView
from django.conf import settings
from django.conf.urls.static import static


urlpatterns = [
    # path(r'messages/', views.get, name='get message'),
    path('segmentation/', SegmentView.as_view(), name='segment'),
    path('admin/', admin.site.urls),
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
