Simple Stateful aggregations in Flink.

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
Create socket as a data source and submit your job in Flink UI. 

```
$ nc -lk 9999
1 10 123
1 20 333
1 30 124
1 60 100
1 5 100
2 10 123
1 1 1
```

#### Output
Look at to Task Manager `stdout` panel in Flink UI.
```
Id,TotalCount,Min_Trx,Max_Trx,Avg_Trx,First_trx_time,Last_trx_time
Habit(1,1,10.0,10.0,10.0,123,123)
Habit(1,2,10.0,20.0,15.0,123,333)
Habit(1,3,10.0,30.0,20.0,123,333)
Habit(1,4,10.0,60.0,30.0,100,333)
Habit(1,5,5.0,60.0,25.0,100,333)
Habit(2,1,10.0,10.0,10.0,123,123)
Habit(1,6,1.0,60.0,21.0,1,333)
```
