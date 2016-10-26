package com.uncharted.btmGlint

/*
spark-shell --master yarn-client  --executor-cores 2  --num-executors 5  --executor-memory 10G --driver-memory 2G --jars /home/chagerman/target/bitermTopicModel-1.0-SNAPSHOT.jar,/home/chagerman/target/glint/target/scala-2.10/Glint-assembly-0.1-SNAPSHOT.jar

spark-shell --master local[4] --driver-memory 2G  --jars target/bitermTopicModel-1.0-SNAPSHOT.jar,/Users/chagerman/3rd_party/glint/target/scala-2.10/Glint-assembly-0.1-SNAPSHOT.jar

import com.uncharted.btmBasic._
import com.uncharted.btmGlint._
import com.uncharted.btmUtil._

 */

import com.typesafe.config.ConfigFactory
import com.uncharted.btmBasic.{Biterm, TextTransform, Vocab, BTMConfig}
import com.uncharted.btmUtil.IO
import glint.Client
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


class GTester_V2 {
    def timeit[R](block: => R): R = {
        val t0 = System.nanoTime()
        val result = block    // call-by-name
        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) / 1000000000.0  + " sec")
        result
    }

    val islocal = false
    def setup(sc: SparkContext, islocal: Boolean=true) = {
        //        val inpath = if (islocal) "/Users/chagerman/Projects/Topic_Modeling/Input/2016-02-clean.distinct.en/*" else "/user/chagerman/word2vec/isil-keywords-clean-distinct-dedupped.en/part-0010*"
        val inpath = if (islocal) "/Users/chagerman/Projects/Topic_Modeling/Input/2016-02-clean.distinct.en/*" else "/user/chagerman/word2vec/isil-keywords-clean-distinct-dedupped.en/part-001*"
        val localoutdir = if (islocal) "/Users/chagerman/Desktop/PS/" else "/home/chagerman/Topic_Modeling/BTM/Output/PS/"
        val basedir = if (islocal) "/Users/chagerman/Projects/Topic_Modeling/Input/" else "/home/chagerman/Topic_Modeling/BTM/Input/"

        val k = 10
        val btmConfig = new BTMConfig()
        btmConfig.setLocaloutdir(localoutdir)
        btmConfig.setNTopics(k)
        btmConfig.setWordpath(basedir + "en_words.m.50.tsv")
        btmConfig.setWorddictpath(basedir + "en_word_dict.m.50.tsv")
        btmConfig.setStopwordpath(basedir + "stopwords_all_en.v2.txt")
        btmConfig.setNIterations(5)
        btmConfig.setNPartitions(20)
        btmConfig.setCheckpointEvery(2)
        val (words, word_dict, stopwords) = Vocab.loadWords(btmConfig)
        btmConfig.setNTerms(words.length)
        btmConfig.setNwordXtopics(words.length * k)
        if (islocal) {
            btmConfig.setPsConfigfile("/Users/chagerman/Projects/Topic_Modeling/glint_btm/glint_akka.conf")
            btmConfig.setCheckpointSave("/Users/chagerman/Desktop/tmp/")
        }

        else {
            btmConfig.setPsConfigfile("/home/chagerman/glint_btm/glint_akka.conf")
            btmConfig.setCheckpointSave("/user/chagerman/tmp/")
        }

        val textRdd = sc.textFile(inpath).map(_.toLowerCase)
        val bitermRdd = TextTransform.transform(textRdd, btmConfig, sc.broadcast(word_dict), sc.broadcast(stopwords)).cache
        val nbiterms = bitermRdd.count
        btmConfig.setNBiterms(Option(nbiterms))
        val rdd = Partitioner.repartition(bitermRdd, btmConfig)

        // Set checkpoint directory
        if (!btmConfig.checkpointSave.isEmpty) {
            sc.setCheckpointDir(btmConfig.checkpointSave)
        }
        IO.saveConfig(localoutdir, btmConfig)
        (btmConfig, rdd, words)
    }



    def run(sc: SparkContext, rdd: RDD[Biterm], config: BTMConfig, words: Array[String]) = {
        val gc = Client(ConfigFactory.parseFile(new java.io.File(config.psConfigfile)))
        println("\nMCMC ESTIMATION...\n")
        val model = GSolver_V2.fit(sc, gc, rdd, config)

        println("\nCOMPUTING THETA, PHI, TOPICS\n")
        val (nz_final, nwz_final) = GSolver_V2.getPsModel(config.nwordXtopics, model)
        val theta = model.calcTheta(config.nbiterms.get, nz_final, nwz_final)
        val phi = model.calcPhi(nz_final, nwz_final)
        val topics = model.report_topics(theta, phi, words, 20)

        // save results
        println("\n SAVING RESULTS...\n")
        IO.saveParameters(theta, phi, config.localoutdir)
        IO.saveTopics(topics, config.localoutdir)
    }


    /*
    val g = new GTester_V2()
    val (btmConfig, rdd, words) = g.timeit{ g.setup(sc, false) }

    val configFile = "/home/chagerman/glint_btm/glint_akka.conf"
    val gc = Client(ConfigFactory.parseFile(new java.io.File(configFile)))
    g.timeit{ g.run(sc, rdd, btmConfig, words) }

     */


}
