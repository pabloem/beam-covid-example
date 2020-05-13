
import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from covidpipe import datasource

class DataSourceTest(unittest.TestCase):

  TEST_DATASOURCE_FILE = 'resources/test_datasource.csv'

  TEST_DATASOURCE_FILE_CONTENTS = [
      {'continent': 'asia', 'country': 'china', 'people': '1000000000'},
      {'continent': 'america', 'country': 'mexico', 'people': '100000000'},
      {'continent': 'africa', 'country': 'nigeria', 'people': '200000000'},
  ]

  TEST_DATA_EMPTY_COLUMNS = [
      {'col1': None, 'col2': '100', 'col4': 'notempty'},
      {'col1': 'ai', 'col3': '12345', 'col4': 'notempty'},
  ]

  def test_readall_from_csv(self):
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create([self.TEST_DATASOURCE_FILE])
          | datasource.ReadAllFromCsv()
      )

      assert_that(result, equal_to(self.TEST_DATASOURCE_FILE_CONTENTS))

  def test_read_from_csv(self):
    with beam.Pipeline() as p:
      result = p | datasource.ReadFromCsv(self.TEST_DATASOURCE_FILE)

      assert_that(result, equal_to(self.TEST_DATASOURCE_FILE_CONTENTS))

  def test_find_columns(self):
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create(self.TEST_DATA_EMPTY_COLUMNS)
          | datasource.FindEmptyAndNonEmptyColumns())

      emtpy_columns = result | beam.Map(
          lambda x: set(x[datasource.FindEmptyAndNonEmptyColumns.EMPTY]))

      non_emtpy_columns = result | beam.Map(
          lambda x: set(x[datasource.FindEmptyAndNonEmptyColumns.NON_EMPTY]))

      assert_that(emtpy_columns, equal_to([set(['col1', 'col2', 'col3'])]))
      assert_that(non_emtpy_columns, equal_to([set(['col4'])]),
                  label='nonemtpyassert')