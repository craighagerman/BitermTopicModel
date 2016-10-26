package com.uncharted.btmUtil

import java.io.{PrintWriter, File}

import com.uncharted.btmBasic.BTMConfig


object IO {

    def writeOutput(output: Array[String], outdir: String, fname:String) = {
        val out = new PrintWriter(new File(s"${outdir}/${fname}"))
        output.foreach{ out.println }
        out.close
    }

    def printConfig(config: BTMConfig) = {
        val settings = s"""BTMConfig {
                          |    input path      = $config.inpath
                          |    output path     = $config.outdir
                          |    alpha   = $config.alpha
                          |    beta    = $config.beta
                          |    # topics (k)    = $config.ntopics
                          |    # words         = $config.nterms
                          |    # words*topics  = $config.nwordsXtopics
                          |    # samples       = $config.nsamples
                          |    # biterms       = $config.nbiterms
                          |    # iterations    = $config.niterations
                          |    # partitions    = $config.npartitions
                          |    PS config file  = $config.psConfigFile
                          |    checkpointRead  = $config.checkpointRead
                          |    checkpointSave  = $config.checkpointSave
                          |    checkpointEvery = $config.checkpointEvery
                          |    datasetStorageLevel  $config.datasetStorageLevel
                          |}""".stripMargin
        settings
    }

    def saveConfig(outdir: String, config: BTMConfig) = {
        writeOutput(Array(printConfig(config)), outdir, "config.txt")
    }

    def saveParameters(theta: Array[Double], phi: Array[Double], outdir: String) = {
        writeOutput(theta.map(_.toString), outdir, "theta.txt")
        writeOutput(phi.map(_.toString), outdir, "phi.txt")
    }
    def saveTopics(topics: Array[(Double, scala.collection.immutable.Seq[String])], outdir: String) = {
        val output = topics.zipWithIndex.map{ case ((prob, words), z) => z + "\t" + prob + "\t" + words.mkString(", ")}
        writeOutput(output, outdir, "topics.txt")
    }

}
