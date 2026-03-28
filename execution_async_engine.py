class AsyncExecutionEngine:

    def __init__(self, client):

        self.client = client

    async def execute(self, order):

        try:

            result = self.client.futures_create_order(
                symbol=order["symbol"],
                side=order["side"],
                type=order["type"],
                quantity=order["qty"]
            )

            return result

        except Exception as e:

            print("EXECUTION ERROR:", e)

            return None
