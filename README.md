Simple Stateful aggregations in Flink.

## Prerequisites
- Hbase 2.3.3
- Hadoop 2.10.0
- Flink 1.11.2_2.11

## How to use it?
A quick usage is given in below.

### Flink
Flink is able to run in standalone. Start the flink cluster.

```
$ cd $FLINK_HOME

$ ./bin/start-cluster.sh
```

Upload your jar from Flink UI -> [localhost:8081](http://localhost:8081)

### Socket
Create a socket as a data source and submit your job in Flink UI. 

```
To send 1000 record per second
$ python3 socket_data.py 1000
```

#### Output
Query generated results from hbase shell.
```
##Id,TotalCount,Min_Trx,Max_Trx,Avg_Trx,First_trx_time,Last_trx_time
$hbase shell
> get 'customer_habit','[trx_id]'
```
