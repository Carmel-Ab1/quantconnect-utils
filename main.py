# region imports
from AlgorithmImports import *
from update_map_files import update_map_files
# endregion

class Quantconnectutils(QCAlgorithm):
    def initialize(self):
        # Locally Lean installs free sample data, to download more data please visit https://www.quantconnect.com/docs/v2/lean-cli/datasets/downloading-data
        self.set_start_date(2020, 1, 1)  # Set Start Date
        self.set_end_date(2020, 1, 1)  # Set End Date
        self.log('STARTED RUNNING')
        update_map_files(self,)

    def on_data(self, data: Slice):
        pass
