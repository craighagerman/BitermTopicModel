package com.uncharted.btmGlint

import com.uncharted.btmBasic.{Biterm, BTMConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/*
 * Copyright © 2013-2015 Uncharted Software Inc.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * Created by chagerman on 2016-10-24.
 */




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