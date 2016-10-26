package com.uncharted.btmBasic

import scala.io.Source

object Vocab {

    def loadWords(config: BTMConfig) = {
        val words = Source.fromFile(config.wordpath).getLines.toArray
        val word_dict = Source.fromFile(config.worddictpath).getLines.map(_.split("\t")).toArray.map(x => (x(0), x(1).toInt)).toMap
        val stopwords = Source.fromFile(config.stopwordpath).getLines.map(_.toLowerCase).toSet
        (words, word_dict, stopwords)
    }

    // ToDo: add in code to create (words, word_dict) from an input text RDD and min count heuristic

}
