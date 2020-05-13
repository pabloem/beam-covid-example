"""File with transforms to consume COVID-related data into Beam pipelines."""

from typing import Dict
from typing import Set

import csv
import io

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils.windowed_value import WindowedValue


class ReadAllFromCsv(beam.PTransform):

  def expand(self, pcoll):
    return (
        pcoll
        | 'MatchAll' >> fileio.MatchAll()
        | beam.Reshuffle()
        | 'ReadEach' >> fileio.ReadMatches()
        | beam.FlatMap(lambda rfile:
                       csv.DictReader(io.TextIOWrapper(rfile.open())))
    )


class ReadFromCsv(beam.PTransform):

  def __init__(self, filepattern):
    self.filepattern = filepattern

  def expand(self, pipeline):
    return (
        pipeline
        | beam.Create([self.filepattern])
        | ReadAllFromCsv()
    )


class FindEmptyAndNonEmptyColumns(beam.PTransform):
  """Receives rows, and it returns sets of columns with empty values."""

  EMPTY = 'empty'
  NON_EMPTY = 'non_empty'

  def expand(self, pcoll):
    all_columns = beam.pvalue.AsSingleton(
        pcoll
        | beam.Map(lambda x: list(x.keys()))
        | 'pregroupmerge' >> beam.ParDo(_MergeAllColumns())
        | beam.Map(lambda x: (None, x))  # Key by same key to merge all columns
        | beam.GroupByKey()
        | beam.FlatMap(lambda x: x[1])
        | 'postgroupmerge' >> beam.ParDo(_MergeAllColumns())
    )

    empty_columns = (
        pcoll
        | 'getemtpycolumns' >> beam.Map(
            lambda x, all_columns: [k for k in all_columns
                                    if k not in x or not x[k]], all_columns)
        | 'emptycolsmerge' >> beam.ParDo(_MergeAllColumns())
        | 'emptykey' >> beam.Map(lambda x: (None, x))
        | 'emtpygbk' >> beam.GroupByKey()
        | 'emptyflatmap' >> beam.FlatMap(lambda x: x[1])
        | 'emptycolspostgroupmerge' >> beam.ParDo(_MergeAllColumns())
    )

    return empty_columns | beam.Map(lambda x, all_cols: {
        self.EMPTY: x,
        self.NON_EMPTY: list(set(all_cols).difference(set(x))),
    }, all_columns)


class _MergeAllColumns(beam.DoFn):

  def start_bundle(self):
    self.all_columns = set()

  def process(self, element):
    self.all_columns = self.all_columns.union(set(element))

  def finish_bundle(self):
    yield WindowedValue(
        list(self.all_columns),
        timestamp=0,
        windows=[GlobalWindows()])