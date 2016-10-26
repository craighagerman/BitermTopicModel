package com.uncharted.btmBasic

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.util.Random

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


//class TextTransform(model: BTMModel) extends Serializable{
object TextTransform extends Serializable{

    private val mt = "@[\\w_]+\\b".r
    private val url = "\\b(http[:\\.\\/\\w]+)\\b".r
    private val nums = "\\b([0-9]+)\\b".r
    private val apos = "'+".r
    private val ps = "[\\p{S}\\p{Pd}\\p{Ps}\\p{Pe}\\p{Pi}\\p{Pf}\\p{Pc}!@%&*:;',.?/\\\"…]+".r
    private val ps2 = "[\\uFF1A-\\uFF20\\uFF3B-\\uFF3E\\uFF40\\uFF5B-\\uFF64\\\\]".r
    private val srt = "\\b(\\w\\w?)\\b".r
    private val single_hashtag = " ?#+ ".r
    private val nl = "[\n\r\t]+".r
    private val unicodeOutliers = ("([^\\u0000-\\uFFEF]|[❤])").r
    private val dots = "[\\u00B7\\u2024\\u2219\\u25D8\\u25E6\\u30FB\\uFF65]".r


    /** Transform an RDD of text into an RDD of biterms
     *
     * @param rdd
     * @param word_dict_Brd
     * @param stopwords_Brd
     * @return
     */
    def transform(rdd: RDD[String], config: BTMConfig, word_dict_Brd: Broadcast[Map[String, Int]], stopwords_Brd: Broadcast[Set[String]] ) = {
        extractBitermsFromRDDRandomK(rdd, config, word_dict_Brd, stopwords_Brd)

    }


//    def extractBitermsFromRDDRandomK(rdd: RDD[String], word_dict: Map[String, Int], stopwords: Set[String]) = {
    def extractBitermsFromRDDRandomK(rdd: RDD[String], config: BTMConfig, word_dict_Brd: Broadcast[Map[String, Int]], stopwords_Brd: Broadcast[Set[String]] ) = {
        val word_dict = word_dict_Brd.value
        val stopwords = stopwords_Brd.value
        val tokens = rdd.map(t => cleanText(t).split("\\s+"))
        val wids = tokens.map(t => getWordIds(t, word_dict, stopwords))
        val biterms = wids.map(w => getBiterms(w).toArray).flatMap(x => x).map(b => Biterm(b, Random.nextInt(config.ntopics)))
        biterms
    }

    /**
     * Receive an array of tokens (cleaned words). Remove stopwords, out-of-vocabulary words
     * Return an array of word-ids corresponding to each word
     */
    def getWordIds(tokens: Array[String], word_dict: Map[String, Int], stopwords: Set[String]) = {
        val wids = tokens.filter(w => !(stopwords contains w))              // ignore words in stopwords
                        .map(word => word_dict.getOrElse(word, -1))         // get the word_id associated with word
                        .filter(x => x > -1)                                // ignore out-of-vocabulary words           // ToDo: should handled OOV words - save & output a list with counts?
        wids
    }



    def getBiterms(d:Array[Int]):Iterator[(Int, Int)] = {
        d.toSeq.combinations(2).map { case Seq(w1, w2) =>
            if (w1 < w2) (w1, w2) else (w2, w1)
        }
    }

    def cleanText(text: String) = {
        val no_emoji = unicodeOutliers.replaceAllIn(text, " ")
        val norm_hashtags = "[\\#]+".r.replaceAllIn(no_emoji, "#")
        val no_nl = nl.replaceAllIn(norm_hashtags, " ")
        val no_url = url.replaceAllIn(no_nl, "")
        val no_mt = mt.replaceAllIn(no_url, " ")
        val no_nums = nums.replaceAllIn(no_mt, " ")
        val no_apos = apos.replaceAllIn(no_nums, " ")
        val no_ps = ps.replaceAllIn(no_apos, " ")
        val no_ps2 = ps2.replaceAllIn(no_ps, " ")
        val no_dots = dots.replaceAllIn(no_ps2, " ")
        val no_short = srt.replaceAllIn(no_dots, " ")
        val no_hashtags = single_hashtag.replaceAllIn(no_short, " ").toLowerCase
        val cleaned = "[\\p{Zs}]+".r.replaceAllIn(no_hashtags, " ").toLowerCase
        cleaned.trim
    }


}
