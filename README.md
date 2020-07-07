# Consumer implementation
Consumer, allowing you to look at the list of messages of the broker and process Netflow packets. Currently supports working only with Kafka.  

## Kafka consuming
For connecting to Kafka brokers, type:
```
consumectl kafka --group<group_name> --topics<topics> [OPTIONS...]
```

**Supported options:**

```
      --group      Kafka consumer group definition (Requiered)
      --topics     Kafka topics to be consumed, as a comma separated list (Requiered)
      --strategy   Application work strategy (default and only only implemented "forever")
      --brokers    Kafka bootstrap brokers to connect to, as a comma separated list (default "127.0.0.1:9092,[::1]:9092")
      --version    Kafka cluster version (default "2.1.1")
      --assignor   Consumer group partition assignment strategy (range, roundrobin, sticky) (default "range")
      --oldest     Kafka consumer consume initial offset from oldest
```

After starting, the consumer will start reading messages and will display a json representation of the Netflow package on stdout. You can also find application logs in `/var/log/consumectl.log`.  

Example of output data:
```json
{
   "start":"2020-07-07T11:10:14Z",
   "type":0,
   "sampling":"rBUAAQ==",
   "src_ip":"10.99.0.0",
   "dst_ip":"10.99.0.19",
   "bytes":1,
   "packets":1,
   "src_port":2103,
   "dst_port":80,
   "etype":2048,
   "proto":6,
   "src_as":0,
   "dst_as":0
}
```
