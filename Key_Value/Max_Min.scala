val lines = sc.textFile("EMPSAL.txt")
        
val keys = lines.map(x => x.toString().split("\t")(1))
          
val values = lines.map(x => x.toString().split("\t")(5)).map(_.replace("$","")).map(_.replace(",","")).map(_.toInt)

val tup = keys zip values

val max_sal = tup.reduceByKey(math.max(_,_))
val min_sal = tup.reduceByKey(math.min(_,_))

max_sal.coalesce(1).saveAsTextFile("Max")
max_sal.coalesce(1).saveAsTextFile("Min")
