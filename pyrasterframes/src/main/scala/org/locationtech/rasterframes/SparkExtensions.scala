package org.locationtech.rasterframes

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.rf.{ExtractRasterUDFs, RasterEvalPython, RasterEvalPythonExec}

case class RasterUDFStrategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    println("RasterUdfStrategy.apply() invoked")
    plan match {
      case RasterEvalPython(udfs, output, child) =>
        RasterEvalPythonExec(udfs, output, planLater(child)) :: Nil
    }
  }
}

case class ExtractRasterUDFRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    println("ExtractRasterUdfRule.apply() invoked")
    ExtractRasterUDFs.apply(plan)
  }
}


class SparkExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    // Rule to convert user defined raster functions to plan nodes
    println("Registering the shit outta some SparkExtensions")
    e.injectPlannerStrategy(RasterUDFStrategy)
    e.injectOptimizerRule(ExtractRasterUDFRule)
  }
}
