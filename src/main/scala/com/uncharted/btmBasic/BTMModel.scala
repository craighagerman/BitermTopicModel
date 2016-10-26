package com.uncharted.btmBasic

case class Biterm(biterm: (Int, Int), var z: Int) extends Serializable


class BTMModel(var nz: Array[Long], var nwz: Array[Long], val config: BTMConfig) extends Serializable {
    def calcTheta(n_biterms: Long) = {
        val theta = nz.map(z => (z + config.alpha) / (n_biterms + config.ntopics * config.alpha))
        theta
    }

    def calcPhi() = {
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


// ---------------------------------------------------------------------------------------------------
object BTMModel extends Serializable {
    def apply(config: BTMConfig) = {
        val nz:Array[Long] = new Array(config.ntopics)
        val nwz:Array[Long] = new Array(config.ntopics * config.nterms)
        new BTMModel(nz, nwz, config)
    }


}

