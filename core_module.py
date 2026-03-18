class Module:

    def __init__(self):

        self.event_bus = None

    def set_event_bus(self, bus):

        self.event_bus = bus

    async def start(self):

        pass

    async def stop(self):

        pass
