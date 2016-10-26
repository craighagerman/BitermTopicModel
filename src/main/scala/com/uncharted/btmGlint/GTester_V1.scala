package com.uncharted.btmGlint

/*
spark-shell --master yarn-client  --executor-cores 2  --num-executors 5  --executor-memory 10G --driver-memory 2G --jars /home/chagerman/target/bitermTopicModel-1.0-SNAPSHOT.jar,/home/chagerman/target/glint/target/scala-2.10/Glint-assembly-0.1-SNAPSHOT.jar

spark-shell --master local[4] --driver-memory 2G  --jars target/bitermTopicModel-1.0-SNAPSHOT.jar,/Users/chagerman/3rd_party/glint/target/scala-2.10/Glint-assembly-0.1-SNAPSHOT.jar

import com.uncharted.btmBasic._
import com.uncharted.btmGlint._
import com.uncharted.btmUtil._


sbt "run master"
sbt "run server"
sbt "run server"

 */


import java.util.Calendar

import com.typesafe.config.ConfigFactory
import com.uncharted.btmUtil.{SimpleLock, IO}
import com.uncharted.btmBasic.{Biterm, TextTransform, Vocab, BTMConfig}
import glint.Client
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.util.Random


class GTester_V1 extends Serializable {

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



    def run(sc: SparkContext, config:BTMConfig, rdd: RDD[Biterm], words:Array[String]) = {
        val k = config.ntopics

        @transient val client = Client(ConfigFactory.parseFile(new java.io.File(config.psConfigfile)))
        val model = BTMGModel(client, config)
//        val s = new GSolver(sc, model)

        // Initial sampling ------------------------------------------------------------------
        println("Sampling biterms to initialize sample recorders...")
//        s.initialize(rdd)

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        val waittime = Duration(model.config.akkaWaittime, "seconds")
        val nz_keys = (0L until model.config.ntopics).toArray
        val nwz_keys = (0L until model.config.nwordXtopics).toArray

        def initialSetTopic(nwz:Array[Long], nz:Array[Long], b:Biterm) = {
            val (w1, w2) = b.biterm
            val z: Int = b.z
            nz(z) += 1
            nwz(w1*k+z) += 1
            nwz(w2*k+z) += 1
        }
        def build(it: Iterator[Biterm]) = {
            implicit val ec = ExecutionContext.Implicits.global
            val nz:Array[Long] = new Array(model.config.ntopics)
            val nwz:Array[Long] = new Array(model.config.nwordXtopics)
            val local = it.toArray
            local.foreach { case b =>
                initialSetTopic(nwz, nz, b)
            }

            model.globalTopicCounts.push(nz_keys, nz)
            model.granularVector.push(nwz_keys, nwz)
        }
        rdd.foreachPartition{ case (it) => build(it) }




        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // MCMC estimation ------------------------------------------------------------------
        println(s"Sampling from biterm distribution for {iterN} iterations...")
//        s.fit(rdd)


        def fit(samples: RDD[Biterm]) = {
            def now = Calendar.getInstance().getTime()
            var rdd = samples
            var prevRdd = rdd
            for (i <- 0 until model.config.niterations) {
                println(s"\niteration #${i}\t${now}\n")
                rdd = rdd.mapPartitions{ case (it) => mcmcSamplingArray(it)}.persist(model.config.datasetStorageLevel)

                // perform checkpointing
                if (i % model.config.checkpointEvery == 0) { println("Checkpointing RDD..."); rdd.checkpoint() }
                // evaluate map expression to force checkpointing
                rdd.foreachPartition(_ => ())           // n.b. could also use rdd.count to evaluate the expression

                val (n1, n2) = getPsModel(10)
                println(s"\tNZ: " + n1.mkString(", ") + "\tSum: " + n1.sum + "\n\tNWZ: " + n2.mkString(", ") + "\tSum: " + n2.sum)
                // unpersist previous RDD and delete old checkpointed data
                println("Unpersisting previous RDD and deleting its checkpoint data")
                removeRdd(prevRdd, sc)
                prevRdd = rdd
            }
        }


        def mcmcSamplingArray(it:Iterator[Biterm]) = {
            implicit val ec = ExecutionContext.Implicits.global
            // semaphore lock from SimpleLock
            val lock = new SimpleLock(16)

            // lock.acquire()?
            val nz = Await.result(model.globalTopicCounts.pull(nz_keys), waittime)
            val nwz = Await.result(model.granularVector.pull(nwz_keys), waittime)
            // lock.release()?
            val prevNz = nz.clone
            val prevNwz = nwz.clone

            val local = it.toArray
            val update = local.map { case b =>
                update_biterm(nwz, nz, b)
            }
            val nz_delta:Array[Long] = computeDelta(prevNz, nz)
            val nwz_delta:Array[Long] = computeDelta(prevNwz, nwz)

            // use Await to block until updates are pushed
            // use semaphores to limit access to PS
            lock.acquire()
            val res1 = Await.result(model.globalTopicCounts.push(nz_keys, nz_delta), waittime)
            val res2 = Await.result(model.granularVector.push(nwz_keys, nwz_delta), waittime)
            lock.release()
            // ToDo: the following should throw an exception and write to logger
            if (!(res1 & res2)) println("ERROR: Error pushing initial values of nz, nwz to PS")
            update.toIterator
        }



        // ---------------------------------------------------------------------------------------------------
        // MCMC Estimatation
        // ---------------------------------------------------------------------------------------------------
        /** resampling */
        def drawTopicIndex(nwz: Array[Long], nz: Array[Long], b:Biterm):Int = {
            val k = model.config.ntopics
            val m = model.config.nterms
            val alpha = model.config.alpha
            val beta = model.config.beta
            var table:Array[Double] = new Array(k)
            val (w1, w2) = b.biterm
            Iterator.range(0, k).map { z =>
                val h = m / (nz(z) * 2 + m * beta)
                val p_z_w1 = (nwz(w1*k+z) + beta) * h
                val p_z_w2 = (nwz(w2*k+z) + beta) * h
                (nz(z) + alpha) * p_z_w1 * p_z_w2
            }.scanLeft(0.0)(_ + _).drop(1).copyToArray(table)
            val r = Random.nextDouble * table.last
            val z = table.indexWhere(_ >= r)
            assert(z >= 0, s"drawTopicIndex: z < 0")
            z
        }
        def setTopic(nwz: Array[Long], nz: Array[Long], b:Biterm, z:Int) {
            val k = model.config.ntopics
            val (w1, w2) = b.biterm
            nz(z) += 1
            nwz(w1*k+z) += 1
            nwz(w2*k+z) += 1
        }
        def unsetTopic(nwz: Array[Long], nz: Array[Long], b:Biterm) {
            val k = model.config.ntopics
            val (w1, w2) = b.biterm
            val z = b.z
            assert(nz(z) > 0, s"unsetTopic: nz(z) < 0\tbiterm = (($w1, $w2), $z)")
            nz(z) -= 1
            assert(nwz(w1*k+z) > 0, s"unsetTopic: nwz(w1*k+z) < 0\tbiterm = (($w1, $w2), $z)")
            nwz(w1*k+z) -= 1
            assert(nwz(w2*k+z) > 0, s"unsetTopic: nwz(w2*k+z) < 0\tbiterm = (($w1, $w2), $z)")
            nwz(w2*k+z) -= 1
        }
        def update_biterm(nwz: Array[Long], nz: Array[Long], b: Biterm) = {
            unsetTopic(nwz, nz, b)
            val z = drawTopicIndex(nwz, nz, b)
            setTopic(nwz, nz, b, z)
            val updatedBiterm = Biterm(b.biterm, z)
            updatedBiterm
        }

        def computeDelta(a_old: Array[Long], a_new: Array[Long]) = {
            a_old zip a_new map{ case( x,y) => y - x}
        }

        // --------------------------------------------------------------------------------


        /**
         * Removes the old RDD and associated checkpoint data
         *
         * @param oldRdd The old RDD
         * @param sc The spark context (needed for deleting checkpoint data)
         */
        def removeRdd(oldRdd: RDD[Biterm], sc: SparkContext): Unit = {
            if (oldRdd.isCheckpointed) {
                try {
                    oldRdd.getCheckpointFile.foreach {
                        case s => FileSystem.get(sc.hadoopConfiguration).delete(new Path(s), true)
                    }
                } catch {
                    // case e: Exception => logger.error(s"Checkpoint deletion error: ${e.getMessage}\n${e.getStackTraceString}")
                    case e: Exception => println(s"Checkpoint deletion error: ${e.getMessage}\n${e.getStackTraceString}")
                }
            }
            oldRdd.unpersist()
        }




        // ---------------------------------------------------------------------------------------------------
        // UTILITY FUNCTIONS (for dev only)                                                 ToDo:  DELETE ME
        // ---------------------------------------------------------------------------------------------------
        def getPsModel( n: Int=0 ) = {
            implicit val ec = ExecutionContext.Implicits.global
            val nz = Await.result(model.globalTopicCounts.pull(nz_keys), waittime)
            val nwz = Await.result(model.granularVector.pull(nwz_keys), waittime)
            val nwz_out = if (n==0) nwz else nwz.slice(0, n)
            (nz, nwz_out)
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        println("\nMCMC ESTIMATION...\n")
        fit(rdd)



        println("\nCOMPUTING THETA, PHI, TOPICS\n")
        val (nz_final, nwz_final) = getPsModel()
        val theta = model.calcTheta(config.nbiterms.get, nz_final, nwz_final)
        val phi = model.calcPhi(nz_final, nwz_final)
        val topics = model.report_topics(theta, phi, words, 20)

        // save results
        println("\n SAVING RESULTS...\n")
        IO.saveParameters(theta, phi, config.localoutdir)
        IO.saveTopics(topics, config.localoutdir)
    }




    def timeit[R](block: => R): R = {
        val t0 = System.nanoTime()
        val result = block    // call-by-name
        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) / 1000000000.0  + " sec")
        result
    }





}

