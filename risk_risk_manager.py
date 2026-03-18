from core.module import Module


class RiskManager(Module):

    def __init__(self):

        super().__init__()

        self.max_positions = 3
        self.current_positions = 0

    def set_event_bus(self, bus):

        super().set_event_bus(bus)

        bus.subscribe("order_request", self.on_order_request)

    async def on_order_request(self, order):

        if self.current_positions >= self.max_positions:

            print("Risk limit reached")

            return

        await self.event_bus.publish("execute_order", order)
