/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.locationtech.rasterframes.expressions.generators

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{UnaryExpression, Expression, Generator, GenericInternalRow, ExpectsInputTypes}
import org.apache.spark.sql.types._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.expressions.DynamicExtractors
import org.locationtech.rasterframes.util._
import spire.syntax.cfor.cfor

import geotrellis.raster.BufferedTensor

import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
/**
 * Catalyst expression for converting a tensor column into a vector column, with each tensor pixel-stack
 *  occupying a separate row as an MLLib vector.
 *
 * @since 4/12/17
 */
case class ExplodeTensors(
  sampleFraction: Double , seed: Option[Long], override val child: Expression)
  extends UnaryExpression with Generator with CodegenFallback with ExpectsInputTypes {

  def this(child: Expression) = this(1.0, None, child)
  override def nodeName: String = "rf_explode_tensors"

  override def inputTypes: Seq[DataType] = Seq(BufferedTensorType)

  override def elementSchema: StructType =
    StructType(
      Seq(
        StructField(COLUMN_INDEX_COLUMN.columnName, IntegerType, false),
        StructField(ROW_INDEX_COLUMN.columnName, IntegerType, false),
        StructField("vector", VectorType, false)
      )
    )

  private def sample[T](things: Seq[T]) = {
    // Apply random seed if provided
    seed.foreach(s ⇒ scala.util.Random.setSeed(s))
    scala.util.Random.shuffle(things)
      .take(math.ceil(things.length * sampleFraction).toInt)
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val row = child.eval(input).asInstanceOf[InternalRow]
    val bufTensor =
      if(row != null)
        DynamicExtractors.bufferedTensorExtractor(child.dataType)(row)._1
      else throw new Exception("I don't know.")

    //val bufTensor = child.eval(input).asInstanceOf[BufferedTensor]

    val depth = bufTensor.tensor.shape(0)
    val cols = bufTensor.tensor.shape(1)
    val rows = bufTensor.tensor.shape(2)

    val udt = new VectorUDT()
    val retval = Array.ofDim[InternalRow](cols * rows)
    cfor(0)(_ < cols, _ + 1) { col =>
      cfor(0)(_ < rows, _ + 1) { row =>
        cfor(0)(_ < depth, _ + 1) { band =>
          val rowIndex = row * cols + col
          val index = band * row * col
          val outCols = Array.ofDim[Any](3)
          outCols(0) = col
          outCols(1) = row
          outCols(2) = udt.serialize(bufTensor.tensor.getPixelVector(col, row))
          retval(rowIndex) = new GenericInternalRow(outCols)
          //outCols(index + 2) = if(tile == null) doubleNODATA else tile.getDouble(col, row)
        }
      }
    }
    if(sampleFraction > 0.0 && sampleFraction < 1.0) sample(retval)
    else retval
  }
}

object ExplodeTensors {
  def apply(col: Column): Column = {
    ExplodeTensors(1.0, None, col)
  }

  def apply(sampleFraction: Double, seed: Option[Long], col: Column): Column = {
    val exploder = new ExplodeTensors(sampleFraction, seed, col.expr)
    // Hack to grab the first two non-cell columns, containing the column and row indexes
    new Column(exploder)
  }
}
