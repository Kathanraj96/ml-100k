/
// Top 10 films from dataset ml-100k (u.data file)
//
// Usage: scala> :load /home/training/Desktop/my_tests/ml-100k/top_10_films_ml-100k_u-data.scala
//

// ------------- spark-shell code
val films = sc.textFile("file:/home/training/Desktop/datasets/ml-100k/u.data")

val films_with_average = films.
    map(row => row.split("\t")).
    map(row => (row(1), row(2).toInt)).
    groupByKey().
    mapValues(rating => rating.sum/rating.size.toFloat)


// now we join with the film infor
val films_infor = sc.textFile("file:/home/training/Desktop/datasets/ml-100k/u.item")
val films_name = films_infor.
    map(_.split("\\|")).
    map(row => (row(0), row(1)))

// Used parallelize to return RDD so we can join below
val films_top_10_with_infor = sc.parallelize( films_with_average.join(films_name).
    map(pair => (pair._2._1, pair._2._2)).
    sortByKey(false).
    take(10).
    map(_.swap) )


// films_top_10_with_infor.
//  foreach{ println }



// another RDD with votes
val films_with_votes = films.
    map(row => row.split("\t")).
    map(row => (row(1), 1)).
    reduceByKey(_+_).
    join(films_name).
    map(pair => (pair._2._1, pair._2._2)).
    map(_.swap)


val films_top_10_with_votes = films_top_10_with_infor.
    join(films_with_votes).
    map(line => (line._1, line._2._1, line._2._2))

films_top_10_with_votes.foreach{ println }


// ------------- END spark-shell code
