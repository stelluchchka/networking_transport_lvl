from django.contrib import admin
from django.urls import path
from transport import views
from transport.views import SegmentView

urlpatterns = [
    # path(r'messages/', views.get, name='get message'),
    path('segment/', SegmentView.as_view(), name='segment'),
    path('admin/', admin.site.urls),
]
