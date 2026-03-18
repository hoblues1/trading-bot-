class FundingAnalyzer:

    def signal(self, funding):

        if funding > 0.05:

            return "SHORT_BIAS"

        if funding < -0.05:

            return "LONG_BIAS"

        return None
