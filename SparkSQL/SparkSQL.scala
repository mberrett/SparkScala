val df = spark.read.option("header", true).csv("airports.csv")

// a) Print Schema 
df.printSchema()   

// Create Temp View in order to use spark.sql commands and answer following questions:
df.createOrReplaceTempView("myTable")

// b) How many airports are there in South east part city 

val b = spark.sql("SELECT COUNT(DISTINCT AirportID) FROM myTable WHERE (Latitude < 0) AND (Longitude >= 0)")
b.show // show answer

// c) How many unique cities have airports in each country?

val c = spark.sql("SELECT Country, COUNT(DISTINCT City) FROM myTable GROUP BY Country")
c.show // show answer

// d) What is the average Altitude (in feet) of airports in each Country?

val d = spark.sql("SELECT Country, AVG(Altitude) FROM myTable GROUP BY Country")
d.show // show answer

// e) How many airports are operating in each timezone?

val e = spark.sql("SELECT Timezone, COUNT(Distinct AirportID) FROM myTable GROUP BY Timezone")
e.show // show answer

// or, alternatively 
val e = spark.sql("SELECT Tz, COUNT(Distinct AirportID) FROM myTable GROUP BY Tz")
e.show // show answer

// f) Calculate average latitude and longitude for these airports in each country.

val f = spark.sql("SELECT Country, AVG(Latitude), AVG(Longitude) FROM myTable GROUP BY Country")
f.show // show answer

// g) How many different DSTs are there?

val g = spark.sql("SELECT COUNT(DISTINCT DST) FROM myTable")
g.show // show answer


