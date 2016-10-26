package com.uncharted.btmGlint


import com.uncharted.btmBasic._
import glint.Client
import glint.models.client.BigVector
import glint.models.client.granular.GranularBigVector





class BTMGModel(var globalTopicCounts: BigVector[Long], var granularVector: GranularBigVector[Long], val config: BTMConfig) extends Serializable {


    def calcTheta(n_biterms: Long, nz: Array[Long], nwz: Array[Long]) = {
        val theta = nz.map(z => (z + config.alpha) / (n_biterms + config.ntopics * config.alpha))
        theta
    }

    def calcPhi(nz: Array[Long], nwz: Array[Long]) = {
        val phi = Iterator.range(0, config.nterms).flatMap { w =>
            Iterator.range(0, config.ntopics).map { z =>
                (nwz(w*config.ntopics+z) + config.beta) / (nz(z) * 2 + config.nterms * config.beta)
            }
        }.toArray
        phi
    }


    def report_topics(theta: Array[Double], phi:Array[Double], words: Array[String], numWords: Int = 20): Array[(Double, scala.collection.immutable.Seq[String])] = {
        def top_indices(m: Int, z: Int, k: Int) = {
            val ws = (0 until m).sortBy(w => -phi(w*k+z)).take(numWords)
            ws
        }
        def get_words(ws: Array[Int], words: Array[String]) = {
            val top_words = ws.map(words).toSeq
            top_words
        }
        val topic_distribution = Iterator.range(0, config.ntopics).toArray.map { z =>
            val ws = (0 until config.nterms).sortBy(w => -phi(w*config.ntopics+z)).take(20)
            (theta(z), ws.map(words).toSeq)
        }
        topic_distribution
    }

}

object BTMGModel {

    /**
     * Constructs an empty LDA model based on given configuration
     *
     * @param config The LDA configuration
     */
    def apply(gc: Client, config: BTMConfig): BTMGModel = {
        val globalTopicCounts = gc.vector[Long](config.ntopics)
        val globalWordTopicCounts = gc.vector[Long](config.ntopics * config.nterms)
        val granularVector = new GranularBigVector[Long](globalWordTopicCounts, 10000) // maximum message size = 120000 keys
        new BTMGModel(globalTopicCounts, granularVector, config)
    }
}
