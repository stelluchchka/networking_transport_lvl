from django.contrib import admin
from django.urls import path
from transport.views import SendSegment
from transport.views import TransferSegment
from transport.views import dl
from transport.views import receiveFile
from django.conf import settings
from django.conf.urls.static import static


from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi


schema_view = get_schema_view(
    openapi.Info(
        title="Transport Layer",
        default_version='v1',
        description="API для сегментации сообщений и сбoрки и перессылки на канальный и прикладной уровни",
        terms_of_service="https://www.yourdomain.com/terms/",
        contact=openapi.Contact(email="contact@yourdomain.com"),
        license=openapi.License(name="Your License"),
    ),
    public=True,
    permission_classes=(permissions.AllowAny,),
)

urlpatterns = [
    path('segmentation/', SendSegment),
    path('transfer/', TransferSegment),
    path('dl/', dl),
    path('receiveFile/', receiveFile),
    path('swagger/', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
]
