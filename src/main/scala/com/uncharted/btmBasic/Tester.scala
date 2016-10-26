package com.uncharted.btmBasic

/*
spark-shell  --master yarn-client  --executor-cores 2  --num-executors 5  --executor-memory 10G --driver-memory 2G --jars /home/chagerman/target/bitermTopicModel-1.0-SNAPSHOT.jar

spark-shell --master local[4] --driver-memory 2G  --jars target/bitermTopicModel-1.0-SNAPSHOT.jar

import com.uncharted.btmBasic._
import com.uncharted.btmGlint._
import com.uncharted.btmGlint._
import glintUtil._


 */

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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


class Tester {

    val islocal = true

    def test(sc: SparkContext) = {
        val inpath = if (islocal) "/Users/chagerman/Projects/Topic_Modeling/Input/2016-02-clean.distinct.en/*" else "/user/chagerman/word2vec/isil-keywords-clean-distinct-dedupped.en/part-0010*"

        val basedir = if (islocal) "/Users/chagerman/Projects/Topic_Modeling/Input/"  else "/home/chagerman/Topic_Modeling/BTM/Input/"

        val btmConfig = new BTMConfig()
        btmConfig.setNTopics(10)
        btmConfig.setWordpath(basedir + "en_words.m.50.tsv")
        btmConfig.setWorddictpath(basedir + "en_word_dict.m.50.tsv")
        btmConfig.setStopwordpath(basedir +  "stopwords_all_en.v2.txt")
        btmConfig.setNIterations(5)




        println(btmConfig)

        val (words, word_dict, stopwords) = Vocab.loadWords(btmConfig)
        val textRdd = sc.textFile(inpath).map(_.toLowerCase)
        val bitermRdd = TextTransform.transform(textRdd, btmConfig, sc.broadcast(word_dict), sc.broadcast(stopwords) ).cache
        val nbiterms= bitermRdd.count
        btmConfig.setNBiterms(Option(nbiterms))
        val biterms = bitermRdd.distinct.collect

        val model = BTMModel(btmConfig)
        val s = new Solver(sc, model)
        s.initialize(biterms)
        s.fit(biterms)

        val theta = model.calcTheta(nbiterms)
        val phi = model.calcPhi()
        val topics = model.report_topics(theta, phi, words, 20)

    }


}
