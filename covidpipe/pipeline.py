
from typing import Dict
from typing import Set

import apache_beam as beam
from apache_beam.options import pipeline_options

import covidpipe
from covidpipe.options import CovidTrackingPipelineOptions


def run(options: pipeline_options.PipelineOptions):

  p =  beam.Pipeline(options=options)

  # Read in the CSV file
  input_data = read_data(
      p, options.view_as(CovidTrackingPipelineOptions).input_file)

  #
  column_information = beam.pvalue.AsSingleton(
      input_data
      | covidpipe.datasource.FindEmptyAndNonEmptyColumns())

  full_data = select_wanted_columns(
      input_data, column_information, ['positive', 'negative'])


#### After this point, there are transforms used in  the main pipeline
def read_data(pipeline, input_file):
  return pipeline | covidpipe.datasource.ReadFromCsv(input_file)


def select_wanted_columns(input_data, column_information, extra_columns):
  def select_wanted_columns(row: Dict[str, str],
      column_info: Dict[str, Set[str]]):
    empty_columns = set(column_info[
                          covidpipe.datasource.FindEmptyAndNonEmptyColumns.EMPTY])

    sanitized_row = {k: v for k, v in row.items()
                     if k not in empty_columns and k not in extra_columns}

    # If the row does not contain any values, then we must discard it.
    if sanitized_row:
      yield sanitized_row

  return input_data | 'SelectColumns' >> beam.FlatMap(select_wanted_columns,
                                                      column_information)


#### After this point, the pipeline is set up to run
if __name__ == '__main__':
  import sys
  options = pipeline_options.PipelineOptions(sys.argv)
  run(options)