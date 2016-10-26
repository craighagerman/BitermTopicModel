package com.uncharted.btmBasic

/*
spark-shell  --master yarn-client  --executor-cores 2  --num-executors 5  --executor-memory 10G --driver-memory 2G --jars /home/chagerman/target/bitermTopicModel-1.0-SNAPSHOT.jar

spark-shell --master local[4] --driver-memory 2G  --jars target/bitermTopicModel-1.0-SNAPSHOT.jar

import com.uncharted.btmBasic._
import com.uncharted.btmGlint._
import com.uncharted.btmGlint._
import glintUtil._
 */


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


class Tester {

    val islocal = true

    def test(sc: SparkContext) = {
        val inpath = if (islocal) "/Users/chagerman/Projects/Topic_Modeling/Input/2016-02-clean.distinct.en/*" else "/user/chagerman/word2vec/isil-keywords-clean-distinct-dedupped.en/part-0010*"

        val basedir = if (islocal) "/Users/chagerman/Projects/Topic_Modeling/Input/"  else "/home/chagerman/Topic_Modeling/BTM/Input/"

        val btmConfig = new BTMConfig()
        btmConfig.setNTopics(10)
        btmConfig.setWordpath(basedir + "en_words.m.50.tsv")
        btmConfig.setWorddictpath(basedir + "en_word_dict.m.50.tsv")
        btmConfig.setStopwordpath(basedir +  "stopwords_all_en.v2.txt")
        btmConfig.setNIterations(5)

        val (words, word_dict, stopwords) = Vocab.loadWords(btmConfig)
        val textRdd = sc.textFile(inpath).map(_.toLowerCase)
        val bitermRdd = TextTransform.transform(textRdd, btmConfig, sc.broadcast(word_dict), sc.broadcast(stopwords) ).cache
        val nbiterms= bitermRdd.count
        btmConfig.setNBiterms(Option(nbiterms))
        val biterms = bitermRdd.distinct.collect

        println(btmConfig)

        // create BTM model, run Gibbs sampling estimation
        val model = BTMModel(btmConfig)
        val s = new Solver(sc, model)
        s.initialize(biterms)
        s.fit(biterms)

        // calculate parameters theta, phi & top N words-per-topics
        val theta = model.calcTheta(nbiterms)
        val phi = model.calcPhi()
        val topics = model.report_topics(theta, phi, words, 20)
    }


}
