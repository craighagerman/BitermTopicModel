
import com.uncharted.bitermTopicModel._
import com.uncharted.btmBasic.BTMConfig
import org.apache.spark.SparkContext


// LDA topic model with 100,000 terms and 100 topics
val btmConfig = new BTMConfig()
btmConfig .setα(0.5)
btmConfig .setβ(0.01)
btmConfig .setNTopics(10)






val texts = Array("one two three")
val biterms = texts.map(text => transform(text, word_dict, stopwords))
val sc = SparkContext

val s = Solver(sc, )


val model = Solver.fitMetropolisHastings(sc, gc, rdd, ldaConfig, 100)

val model = Solver.fit(biterms)



