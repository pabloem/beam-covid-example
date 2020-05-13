
from typing import Dict
from typing import Iterable

import apache_beam as beam
import numpy as np


class FindStateSpikesFn(beam.DoFn):

  def process(self, state_data: Iterable[Dict[str, str]]):
    state_data = list(state_data)
    state_data.sort(key=lambda x: x['date'])
    positives = [int(x['positive']) for x in state_data if 'positive' in x]
    diffs = np.diff(positives)
    seven_day_max = max(diffs[-8:-1])
    latest_value = diffs[-1]

    # The latest value is larger than all previous 7 values. This looks like a
    # serious spike. Let's yield this out.
    if latest_value > seven_day_max:
      yield state_data[-1]