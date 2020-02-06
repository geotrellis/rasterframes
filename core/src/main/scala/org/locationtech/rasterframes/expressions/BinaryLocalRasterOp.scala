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

package org.locationtech.rasterframes.expressions

import com.typesafe.scalalogging.Logger
import geotrellis.raster.{ArrowTensor, Tile}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.BinaryExpression
import org.apache.spark.sql.rf.{TileUDT, TensorUDT}
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.tensors.RFTensor
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.slf4j.LoggerFactory

/** Operation combining two tiles or a tile and a scalar into a new tile. */
trait BinaryLocalRasterOp extends BinaryExpression {

  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))


  override def dataType: DataType = left.dataType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tensorOrTileExtractor.isDefinedAt(left.dataType)) {
      TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a raster type.")
    }
    else if (!tensorTileOrNumberExtractor.isDefinedAt(right.dataType)) {
      TypeCheckFailure(s"Input type '${right.dataType}' does not conform to a compatible type.")
    }
    else TypeCheckSuccess
  }

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    implicit val tileSer = TileUDT.tileSerializer
    implicit val tensorSer = TensorUDT.tensorSerializer
    val (result, isTensor, context) = (tensorOrTileExtractor(left.dataType)(row(input1)), tensorTileOrNumberExtractor(right.dataType)(input2)) match {
      case (TensorArg(leftTensor, leftCtx), TensorArg(rightTensor, rightCtx)) ⇒
        if (leftCtx.isEmpty && rightCtx.isDefined)
          logger.warn(
            s"Right-hand parameter '${right}' provided an extent and CRS, but the left-hand parameter " +
              s"'${left}' didn't have any. Because the left-hand side defines output type, the right-hand context will be lost.")

        if(leftCtx.isDefined && rightCtx.isDefined && leftCtx != rightCtx)
          logger.warn(s"Both '${left}' and '${right}' provided an extent and CRS, but they are different. Left-hand side will be used.")

        (op(leftTensor, rightTensor), true, leftCtx)
      case (TensorArg(leftTensor, leftCtx), TileArg(rightTile, rightCtx)) ⇒
        if (leftCtx.isEmpty && rightCtx.isDefined)
          logger.warn(
            s"Right-hand parameter '${right}' provided an extent and CRS, but the left-hand parameter " +
              s"'${left}' didn't have any. Because the left-hand side defines output type, the right-hand context will be lost.")

        if(leftCtx.isDefined && rightCtx.isDefined && leftCtx != rightCtx)
          logger.warn(s"Both '${left}' and '${right}' provided an extent and CRS, but they are different. Left-hand side will be used.")

        (op(leftTensor, rightTile), true, leftCtx)
      case (TensorArg(leftTensor, leftCtx), DoubleArg(d)) ⇒ (op(leftTensor, d), true, leftCtx)
      case (TensorArg(leftTensor, leftCtx), IntegerArg(i)) ⇒ (op(leftTensor, i.toDouble), true, leftCtx)
      case (TileArg(leftTile, leftCtx), TileArg(rightTile, rightCtx)) =>
         if (leftCtx.isEmpty && rightCtx.isDefined)
           logger.warn(
             s"Right-hand parameter '${right}' provided an extent and CRS, but the left-hand parameter " +
               s"'${left}' didn't have any. Because the left-hand side defines output type, the right-hand context will be lost.")

         if(leftCtx.isDefined && rightCtx.isDefined && leftCtx != rightCtx)
           logger.warn(s"Both '${left}' and '${right}' provided an extent and CRS, but they are different. Left-hand side will be used.")

         (op(leftTile, rightTile), false, leftCtx)
      case (TileArg(leftTile, leftCtx), DoubleArg(d)) => (op(fpTile(leftTile), d), false, leftCtx)
      case (TileArg(leftTile, leftCtx), IntegerArg(i)) => (op(leftTile, i), false, leftCtx)
      case arg ⇒ throw new UnsupportedOperationException(s"Tried to combine $arg.  Higher rank argument must be on the left-hand side.")
    }

    (context, isTensor) match {
      case (Some(ctx), true) ⇒ result.asInstanceOf[RFTensor].toInternalRow
      case (Some(ctx), false) ⇒ ctx.toProjectRasterTile(result.asInstanceOf[Tile]).toInternalRow
      case (None, true) ⇒ result.asInstanceOf[RFTensor].toInternalRow
      case (None, false) ⇒ result.asInstanceOf[Tile].toInternalRow
    }
  }

  protected def op(left: ArrowTensor, right: ArrowTensor): ArrowTensor = throw new UnsupportedOperationException("Tensor × Tensor operation implementation not defined")
  protected def op(left: ArrowTensor, right: Tile): ArrowTensor = throw new UnsupportedOperationException("Tensor × Tile operation implementation not defined")
  protected def op(left: ArrowTensor, right: Double): ArrowTensor = throw new UnsupportedOperationException("Tensor × Scalar operation implementation not defined")
  // protected def op(left: Tensor, right: Int): Tensor // omitted since tensors are floating point only for now
  protected def op(left: Tile, right: Tile): Tile
  protected def op(left: Tile, right: Double): Tile
  protected def op(left: Tile, right: Int): Tile
}
