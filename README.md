# SparkPlayground


```
gradle jar
gradle fatjar
gradle build
```

```
gradle fatjar
/Users/pengjianqing/Spark/spark-1.3.0-bin-hadoop2.4/bin/spark-submit --class me.pjq.Main --master local[*] build/libs/SparkPlayground.jar
/Users/pengjianqing/Spark/spark-1.3.0-bin-hadoop2.4/bin/spark-submit --class me.pjq.Main --master local[*] build/libs/SparkPlayground.jar  --jars $(echo /Users/pengjianqing/Spark/spark-1.3.0-bin-hadoop2.4/lib/*.jar|tr ' ' ',')
/Users/pengjianqing/Spark/spark-1.3.0-bin-hadoop2.4/bin/spark-submit --class me.pjq.Main --master local[*] build/libs/SparkPlayground-all-1.0.jar
```
