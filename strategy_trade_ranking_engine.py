import heapq


class TradeRankingEngine:

    def __init__(self, alpha_engine):

        self.alpha_engine = alpha_engine

        self.max_candidates = 30
        self.top_n = 3

    def rank_markets(self, market_data_dict):

        rankings = []

        for symbol, data in market_data_dict.items():

            score = self.alpha_engine.compute_alpha(data)

            rankings.append((score, symbol))

        rankings.sort(reverse=True)

        top = rankings[:self.top_n]

        return top

    def get_trade_candidates(self, market_data_dict):

        ranked = self.rank_markets(market_data_dict)

        trades = []

        for score, symbol in ranked:

            data = market_data_dict[symbol]

            should_trade, alpha = self.alpha_engine.should_trade(data)

            if should_trade:

                trades.append({
                    "symbol": symbol,
                    "score": alpha
                })

        return trades
