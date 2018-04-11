from __future__ import print_function
import apache_beam as beam
import apache_beam.io
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

import urllib2
import json
import sys


topic_name="projects/speechml-demo/topics/test"

#this is a function to find meaning of the words from oxford api. This can be ignored for now since my use case is about publishing just words 
def find_meaning(words):
   for word in words:
     app_id = "a8bd0c50"
     app_key = "b0f4a0004768e2d7e28926e70cab7950"
     language = 'en'
     word_id =  word
     #print(word)
     try:
      url = 'https://od-api.oxforddictionaries.com:443/api/v1/entries/' + language + '/' + word_id.lower()
      headers = {'app_id': app_id, 'app_key': app_key}
      req_1 = urllib2.Request(url)
      req_1.add_header('app_id',app_id)
      req_1.add_header('app_key',app_key)
      p_1 = urllib2.urlopen(req_1)
      response_1=p_1.read()
      response_1=json.loads(response_1)
      meaning=response_1['results'][0]['lexicalEntries'][0]['entries'][0]['senses'][0]['definitions'][0]
     except:
        meaning="No such word!"

     json_value='{"word": "' + word + '", "meaning:' + meaning+ '" }'
     #print(json_value)
     yield meaning




with beam.Pipeline(options=PipelineOptions(flags=sys.argv)) as p:

    lines = p | 'read' >> ReadFromText("alibaba.txt")
    
    counts = (
        
        lines | 'Split' >> beam.FlatMap(lambda x: x.split(' '))
                               .with_output_types(unicode)
              #| 'GetMeaning' >> beam.Map(lambda x: (find_meaning(x))) .with_output_types(unicode)
              #| 'Print2' >> beam.Map(lambda x: print(x))
              #| 'Print2' >> beam.Map(lambda x: print("Success!"))
              #| 'Print ' >> beam.ParDo(lambda x: print(x))
              | 'Publish' >> beam.io.WriteStringsToPubSub(topic_name)
              #| 'Print' >> beam.Map(lambda x: print("Success!"))
     )
        
  



