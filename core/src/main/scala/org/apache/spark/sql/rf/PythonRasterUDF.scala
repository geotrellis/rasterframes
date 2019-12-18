package org.apache.spark.sql.rf

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.api.python.{PythonEvalType, PythonFunction}
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.types.DataType

object PythonRasterUDF {
  private[this] val SCALAR_TYPES = Set(
    PythonEvalType.SQL_BATCHED_UDF,
    PythonEvalType.SQL_SCALAR_PANDAS_UDF
  )

  def isScalarPythonUDF(e: Expression): Boolean = {
    e.isInstanceOf[PythonRasterUDF] && SCALAR_TYPES.contains(e.asInstanceOf[PythonRasterUDF].evalType)
  }

  // def isGroupedAggPandasUDF(e: Expression): Boolean = {
  //   e.isInstanceOf[PythonUDF] &&
  //     e.asInstanceOf[PythonUDF].evalType == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF
  // }

  // // This is currently same as GroupedAggPandasUDF, but we might support new types in the future,
  // // e.g, N -> N transform.
  // def isWindowPandasUDF(e: Expression): Boolean = isGroupedAggPandasUDF(e)
}

case class PythonRasterUDF(
    name: String,
    func: PythonFunction,
    dataType: DataType,
    children: Seq[Expression],
    evalType: Int,
    udfDeterministic: Boolean,
    resultId: ExprId = NamedExpression.newExprId)
  extends Expression with Unevaluable with NonSQLExpression with UserDefinedExpression {

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  override def toString: String = s"$name(${children.mkString(", ")})"

  lazy val resultAttribute: Attribute = AttributeReference(toPrettySQL(this), dataType, nullable)(
    exprId = resultId)

  def toPythonUDF: PythonUDF = PythonUDF(name, func, dataType, children, evalType, udfDeterministic, resultId)

  override def nullable: Boolean = true
}
