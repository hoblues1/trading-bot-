import asyncio
from collections import defaultdict


class EventBus:
    """
    Hedge-fund style event bus
    모든 모듈이 이 버스를 통해 통신한다
    """

    def __init__(self):
        self.listeners = defaultdict(list)

    def subscribe(self, event_type, callback):
        self.listeners[event_type].append(callback)

    async def publish(self, event_type, data):
        if event_type not in self.listeners:
            return

        tasks = []

        for callback in self.listeners[event_type]:
            tasks.append(callback(data))

        await asyncio.gather(*tasks)


class TradingEngine:

    def __init__(self):

        self.event_bus = EventBus()

        self.modules = []

        self.running = False

    def register_module(self, module):

        module.set_event_bus(self.event_bus)

        self.modules.append(module)

    async def start(self):

        print("Trading Engine Starting...")

        self.running = True

        tasks = []

        for module in self.modules:

            if hasattr(module, "start"):

                tasks.append(module.start())

        await asyncio.gather(*tasks)

    async def stop(self):

        print("Trading Engine Stopping...")

        self.running = False

        for module in self.modules:

            if hasattr(module, "stop"):
                await module.stop()
