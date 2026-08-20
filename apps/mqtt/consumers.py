import json
from channels.generic.websocket import AsyncWebsocketConsumer

class Plant2Consumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.room_group_name = 'plant2_live_updates'

        # ✅ WebSocket connect hote hi client ko is room mein daal do
        await self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name
        )
        await self.accept()
        print("✅ Frontend WebSocket Connected!")

    async def disconnect(self, close_code):
        # ✅ Disconnect hone par room se hata do
        await self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name
        )
        print("❌ Frontend WebSocket Disconnected!")

    # ✅ Yeh function tab chalega jab simple_plant2.py se data aayega
    async def send_machine_update(self, event):
        try:
            message = event['message']
            # Frontend ko exactly wahi data bhej do jo backend ne bheja
            await self.send(text_data=json.dumps({
                'type': 'realtime_update',
                'data': message
            }))
        except Exception as e:
            print(f"❌ WebSocket Broadcast Error: {e}")
            
            
class Plant1Consumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.room_group_name = 'plant1_live_updates'
        await self.channel_layer.group_add(self.room_group_name, self.channel_name)
        await self.accept()
        print("✅ Plant 1 Frontend WebSocket Connected!")

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(self.room_group_name, self.channel_name)
        print("❌ Plant 1 Frontend WebSocket Disconnected!")

    async def send_machine_update(self, event):
        try:
            message = event['message']
            await self.send(text_data=json.dumps({
                'type': 'realtime_update',
                'data': message
            }))
        except Exception as e:
            print(f"❌ Plant 1 WebSocket Broadcast Error: {e}")