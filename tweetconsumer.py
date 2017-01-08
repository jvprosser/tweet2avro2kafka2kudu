#!/usr/bin/env python
import threading, logging, time
import io
import avro.schema
import avro.io
import pprint
import kudu
from kudu.client import Partitioning


from kafka import KafkaConsumer


pp = pprint.PrettyPrinter(indent=4,width=80)

twitter_schema='''
{"namespace": "example.avro",                                                                                                                                         "type": "record",
 "name": "StatusTweet",
 "fields": [
     {"name": "tweet_id"       ,  "type": "long"},
     {"name": "followers_count",  "type": "int"},
     {"name": "statuses_count" ,  "type": "int"},
     {"name": "id_str"         ,  "type": "string"},
     {"name": "friends_count"  ,  "type": "int"},
     {"name": "text"           ,  "type": "string"},
     {"name": "tweet_ts"       ,  "type": "long"},
     {"name": "screen_name"    ,  "type": "string"}
 ]
}
'''

class Consumer(threading.Thread):
    daemon = True
    def __init__(self, name, partition_list ):

        threading.Thread.__init__(self)

        self.name = name

        self.partitions = partition_list
        self.client = kudu.connect(host='ip-172-31-6-171', port=7051)
        # Open a table
        self.table = self.client.table('impala::DEFAULT.STATUS_TWEETS')

        # Create a new session so that we can apply write operations
        self.session = self.client.new_session()


    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='ip-172-31-10-235:9092',
                                 auto_offset_reset='earliest', enable_auto_commit=True)

        consumer.subscribe(['twitterstream'])

        print "in run"
        for message in consumer:
            schema = avro.schema.parse(twitter_schema)

            bytes_reader = io.BytesIO(message.value)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(schema)

            data = reader.read(decoder)

            print ("%s:%d:%d: key=%s   %s" % (message.topic, message.partition,message.offset, message.key, data['text'][1:77] ))
            op = self.table.new_insert({
              'id_str'          :  data['id_str']          ,
              'tweet_ts'        :  data['tweet_ts']        ,
              'tweet_id'        :  data['tweet_id']        ,
              'followers_count' :  data['followers_count'] ,
              'statuses_count'  :  data['statuses_count']  ,
              'friends_count'   :  data['friends_count']   ,
              'text'            :  data['text']            ,
              'screen_name'     :  data['screen_name']     })
            self.session.apply(op)
            # Flush write operations, if failures occur, capture print them.
            try:
              self.session.flush()
            except kudu.KuduBadStatus as e:
              print(self.session.get_pending_errors())



def main():
    threads = [
        Consumer("three",(3))
    ]


    for t in threads:
        t.start()

    time.sleep(100)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
