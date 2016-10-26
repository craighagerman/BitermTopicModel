package com.uncharted.btmGlint

import java.util.Calendar

import com.uncharted.btmUtil.SimpleLock
import com.uncharted.btmBasic.{Biterm, Solver}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.concurrent.duration._


import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random


class GSolver_V1(sc: SparkContext, model: BTMGModel) extends Serializable {
    private val waittime = Duration(model.config.akkaWaittime, "seconds")         // ToDo: set the duration value by a BtmConfig parameter?
    private val nz_keys = (0L until model.config.ntopics).toArray
    private val nwz_keys = (0L until model.config.nwordXtopics).toArray

    def initialize(samples: RDD[Biterm]) = {
        val k = model.config.ntopics
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
    samples.foreachPartition{ case (it) => build(it) }
    }


    def fit(samples: RDD[Biterm]) = {
        def now = Calendar.getInstance().getTime()
        var rdd = samples
        var prevRdd = rdd
        for (i <- 0 until model.config.niterations) {
            println(s"iteration #${i}\t${now}")
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

}
