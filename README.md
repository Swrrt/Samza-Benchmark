# Samza-Benchmark
Benchmark Applications 

### alter topic producer timestamp to LogApendTIme

``` bin/kafka-topics.sh --alter --zookeeper localhost:2181 --topic metrics --config message.timestamp.type=LogAppendTime
