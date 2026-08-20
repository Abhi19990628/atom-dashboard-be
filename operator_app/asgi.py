import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
import apps.mqtt.routing  # Naya routing import

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'operator_app.settings')

application = ProtocolTypeRouter({
    # Normal HTTP requests (APIs/Pages) yahan handle hongi
    "http": get_asgi_application(),
    
    # Live WebSockets ki requests yahan handle hongi
    "websocket": AuthMiddlewareStack(
        URLRouter(
            apps.mqtt.routing.websocket_urlpatterns
        )
    ),
})