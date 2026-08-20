from django.urls import re_path
from . import consumers

websocket_urlpatterns = [
    re_path(r'ws/plant2/live/$', consumers.Plant2Consumer.as_asgi()),
    re_path(r'ws/plant1/live/$', consumers.Plant1Consumer.as_asgi()),
]
