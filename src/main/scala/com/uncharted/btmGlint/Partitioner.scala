package com.uncharted.btmGlint

import com.uncharted.btmBasic.{Biterm, BTMConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


object Partitioner {

    def repartition(gibbsSamples: RDD[Biterm], config: BTMConfig) = {
        // Repartition (if possible through coalesce for performance reasons)
        println("Repartitioning and persisting biterm Gibbs samples...")
        // val repartitionedGibbsSamples = if (gibbsSamples.getNumPartitions > config.npartitions) {
        val repartitionedGibbsSamples = if (gibbsSamples.partitions.size > config.npartitions) {
            // gibbsSamples.coalesce(config.npartitions)
            gibbsSamples.coalesce(config.npartitions)
        } else {
            // gibbsSamples.repartition(config.npartitions)
            gibbsSamples.repartition(config.npartitions)
        }
        // Persist samples to memory and disk
        val persistedGibbsSamples = repartitionedGibbsSamples.persist(config.datasetStorageLevel)

        // Trigger empty action to materialize the mapping and persist it
        persistedGibbsSamples.foreachPartition(_ => ())
        println("Using ${persistedGibbsSamples.partitions.size} partitions...")
        persistedGibbsSamples
    }
}