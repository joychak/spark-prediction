package com.datalogs.utils

import org.apache.spark.rdd.RDD
import org.sameersingh.scalaplot.{MemXYSeries, XYChart, XYData}
import org.sameersingh.scalaplot.jfreegraph.JFGraphPlotter

/**
  * Created by joychak on 1/9/17.
  */
object PlotUtils {
    def plot(roc: RDD[(Double, Double)], auROC: Double, label: String): Unit = {

      val xs = roc.map(_._1).collect()
      val ys = roc.map(_._2).collect()

      val series = new MemXYSeries(xs, ys)
      val data = new XYData(series)
      val chart = new XYChart(label, data)
      val plotter = new JFGraphPlotter(chart)
      plotter.gui()
    }
}
