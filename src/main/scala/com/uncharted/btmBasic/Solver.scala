package com.uncharted.btmBasic

import java.util.Calendar
import org.apache.spark.SparkContext

import scala.util.Random



class Solver(sc: SparkContext, model: BTMModel) {
    def initialize(biterms: Array[Biterm]) = {
        biterms.foreach {b => setTopic(b,  b.z) }
    }

    def fit(biterms:Array[Biterm]) = {
        Iterator.range(0, model.config.niterations).foreach { n =>
            val now = Calendar.getInstance().getTime()
            println(s"${now}\titeration ${n+1}")
            biterms.foreach { case b =>
                update_biterm(b)
            }
        }
    }

    /** resampling */
    def drawTopicIndex(b:Biterm):Int = {
        var table:Array[Double] = new Array(model.config.ntopics)
        val (w1, w2) = b.biterm
        Iterator.range(0, model.config.ntopics).map { z =>
            val h = model.config.nterms / (model.nz(z) * 2 + model.config.nterms * model.config.beta)
            val p_z_w1 = (model.nwz(w1*model.config.ntopics+z) + model.config.beta) * h
            val p_z_w2 = (model.nwz(w2*model.config.ntopics+z) + model.config.beta) * h
            (model.nz(z) + model.config.alpha) * p_z_w1 * p_z_w2
        }.scanLeft(0.0)(_ + _).drop(1).copyToArray(table)
        val r = Random.nextDouble * table.last
        val z = table.indexWhere(_ >= r)
        assert(z >= 0, s"drawTopicIndex: z < 0")
        z
    }

    def setTopic(b:Biterm, z:Int) {
        val (w1, w2) = b.biterm
        b.z = z
        model.nz(z) += 1
        model.nwz(w1*model.config.ntopics+z) += 1
        model.nwz(w2*model.config.ntopics+z) += 1
    }

    def unsetTopic(b:Biterm) {
        val (w1, w2) = b.biterm
        val z = b.z
        assert(model.nz(z) > 0, s"unsetTopic: nz(z) > 0\tbiterm = (($w1, $w2), $z)")
        model.nz(z) -= 1
        assert(model.nwz(w1*model.config.ntopics+z) > 0, s"unsetTopic: nwz(w1*k+z) > 0\tbiterm = (($w1, $w2), $z)")
        model.nwz(w1*model.config.ntopics+z) -= 1
        assert(model.nwz(w2*model.config.ntopics+z) > 0, s"unsetTopic: nwz(w2*k+z) > 0\tbiterm = (($w1, $w2), $z)")
        model.nwz(w2*model.config.ntopics+z) -= 1
    }

    def update_biterm(b: Biterm) = {
        unsetTopic(b)
        val z = drawTopicIndex(b)
        setTopic(b, z)
    }

}
