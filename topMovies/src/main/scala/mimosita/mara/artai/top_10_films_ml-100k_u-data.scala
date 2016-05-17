//
// Top films from dataset ml-100k (u.data file)
//
// Usage: TopMovies <Number_of_films_to_show>
//
// 1) compile with maven
// 2) submit with, for example, master local 2:
//   $ spark-submit --class mimosita.mara.artai.TopMovies --master local[2] target/topmovies-1.0.jar 20
//

package mimosita.mara.artai

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Usage: TopMovies
 */
object TopMovies {

	def main(args: Array[String]) {
		if (args.length < 1) {
		  System.err.println("Usage: TopMovies <Number_of_films_to_show>")
		  System.exit(1)
		}

		val sparkConf 	= new SparkConf().setAppName("Top10Movies")
		val sc	 		= new SparkContext(sparkConf)

		val number_of_films = args(0).toInt

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
			take(number_of_films).
			map(_.swap) )


		// films_top_10_with_infor.
		films_top_10_with_infor.foreach{ println }



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


		sc.stop()
	}
}
