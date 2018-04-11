from __future__ import absolute_import

import logging
import argparse
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
import sys
#i am giving topic name in subscriber program as suggested in the google github repo  
sub_name="projects/speechml-demo/topics/test"
table_name="speechml-demo:dataflow_test.words"

'''def parse_pubsub(line):
    import json
    record = json.loads(line)
    return (record['mac']), (record['status']), (record['datetime'])'''


with beam.Pipeline(options=PipelineOptions(flags=sys.argv)) as p:
        lines = ( p | beam.io.ReadStringsFromPubSub(sub_name)
                #| beam.Map(parse_pubsub)
                #| beam.Map(lambda (mac_bq, status_bq, datetime_bq): {'mac': mac_bq, 'status': status_bq, 'datetime': datetime_bq})
                | beam.io.WriteToBigQuery(
                    table_name,
                    schema='word:STRING',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )

if __name__ == '__main__':
  run()
