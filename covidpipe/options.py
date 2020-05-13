
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class CovidTrackingPipelineOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):  # type: (_BeamArgumentParser) -> None
    parser.add_argument('input_file', default='resources/daily.csv')
    parser.add_argument('spikes_output_file', default='spikes_out.json')