
Q1) Word Count

val text = sc.textFile("alice.txt")

text.flatMap(_.split("\\W+")).map(word => (word.length, 1)).reduceByKey(_ + _).collect


Q2)

val lines = sc.textFile("TravelData.txt")

// a) Top 20 destination people travel the most

// Answer by amount of trips booked 
lines.map(x => x.toString().split("\t")(2)).map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(20)

// b) Top 20 locations from where people travel the most

// Answer by amount of trips booked 
lines.map(x => x.toString().split("\t")(1)).map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(20)

// c) Find top 20 cities that generate high airline revenues for travel

lines.map(_.split("\t")).map(c => (c(2), c(5).toInt + c(6).toInt + c(7).toInt + c(8).toInt)).reduceByKey(_ + _).sortBy(_._2, false).take(20)
