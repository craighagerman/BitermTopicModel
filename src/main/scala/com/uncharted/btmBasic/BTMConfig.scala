package com.uncharted.btmBasic

import org.apache.spark.storage.StorageLevel

/**
 * Configuration for the BTM solver
 *
 * @param alpha             Prior on the document-topic distributions
 * @param beta              Prior on the topic-word distributions
 * @param npartitions       The number of partitions
 * @param niterations       The number of iterations
 * @param ntopics           The number of topics
 * @param nterms            The number of vocabulary terms
 * @param wordpath          Path to pre-computed vocabulary
 * @param worddictpath      Path to pre-computed word dictionary
 * @param stopwordpath      Path to stopwords
 * @param checkpointRead    The location where checkpointed data will be read from as a warmstart mechanic
 * @param checkpointSave    The location where checkpointed data will be stored after failure
 * @param checkpointEvery   If checkpointSave is set, this indicates the frequency of checkpoints (every x iterations)
 */
case class BTMConfig(   var alpha: Double = 0.05,
                        var beta: Double = 0.01,
                        var npartitions: Int = 240,
                        var niterations: Int = 100,
                        var ntopics: Int = 10,
                        var nterms: Int = 0,
                        var nsamples: Int = 0,
                        var nwordXtopics:Int = 0,
                        var nbiterms: Option[Long] = None: Option[Long],
                        var wordpath: String = "",
                        var worddictpath: String = "",
                        var stopwordpath: String = "",
                        var akkaWaittime: Int = 300,
                        var checkpointRead: String = "",
                        var checkpointSave: String = "",
                        var checkpointEvery: Int = 1,
                        var datasetStorageLevel: StorageLevel = StorageLevel.DISK_ONLY,
                        var localoutdir: String = "",
                        var psConfigfile: String = "") extends Serializable {




    def setAlpha(α: Double) = this.alpha = alpha
    def setBeta(β: Double) = this.alpha = beta
    //def setτ(τ: Int) = this.τ = τ
    def setNPartitions(npartitions: Int) = this.npartitions = npartitions
    def setNIterations(niterations: Int) = this.niterations = niterations
    def setNTopics(ntopics: Int) = this.ntopics = ntopics
    def setNTerms(nterms: Int) = this.nterms = nterms
    def setNSamples(nsamples: Int) = this.nsamples = nsamples
    def setNwordXtopics(nwordXtopics:Int) = this.nwordXtopics = nwordXtopics
    def setNBiterms(nbiterms: Option[Long]) = this.nbiterms = nbiterms
    def setAkkaWaittime(akkaWaittime: Int) = this.akkaWaittime = akkaWaittime
    def setWordpath(wordpath: String) = this.wordpath = wordpath
    def setWorddictpath(worddictpath: String) = this.worddictpath = worddictpath
    def setStopwordpath(stopwordpath: String) = this.stopwordpath = stopwordpath
    def setCheckpointRead(checkpointRead: String) = this.checkpointRead = checkpointRead
    def setCheckpointSave(checkpointSave: String) = this.checkpointSave = checkpointSave
    def setCheckpointEvery(checkpointEvery: Int) = this.checkpointEvery = checkpointEvery
    def setDatasetStorageLevel(datasetStorageLevel: StorageLevel) = this.datasetStorageLevel = datasetStorageLevel
    def setLocaloutdir(localoutdir: String) = this.localoutdir = localoutdir
    def setPsConfigfile(psConfigfile: String) = this.psConfigfile = psConfigfile


    override def toString: String = {
        s"""LDAConfig {
           |    alpha = $alpha
           |    beta = $beta
           |    # partitions   = $npartitions
           |    # iterations   = $niterations
           |    # topics       = $ntopics
           |    # words        = $nterms
           |    words * topics = $nwordXtopics
           |    # biterms      = $nbiterms
           |    akka wait time = $akkaWaittime
           |    word path      = $wordpath
           |    worddict path  = $worddictpath
           |    stopword path  = $stopwordpath
           |    checkpointRead = $checkpointRead
           |    checkpointSave = $checkpointSave
           |    checkpointEvery = $checkpointEvery
           |    PS config file  = $psConfigfile
           |    local out dir   = $localoutdir
           |}
           """.stripMargin
    }
}
