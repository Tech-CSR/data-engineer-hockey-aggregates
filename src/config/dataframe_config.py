"""
This Module accommodates to initialize getters and setters

"""

class DataframeProvider(object):

    def __init__(self):
        self._hockey_df = None

    @property
    def hockey_df(self):
        return self._hockey_df

    @hockey_df.setter
    def hockey_df(self, df):
        self._hockey_df = df