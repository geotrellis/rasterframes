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

package org.locationtech.rasterframes.expressions.transformers

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.types.{DataType, StructField, StructType, ArrayType}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.rf.TensorUDT._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.row
import org.locationtech.rasterframes.ref.RasterRef
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

import geotrellis.raster._
import org.slf4j.LoggerFactory

/**
 * Realizes a RasterRef into a Tile.
 *
 * @since 11/2/18
 */
case class RasterRefStackToTensor(child: Expression) extends UnaryExpression
  with CodegenFallback with ExpectsInputTypes {

  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def nodeName: String = "raster_ref_stack_to_tensor"

  override def inputTypes = Seq(StructType(
    Array(StructField("tensor", ArrayType(schemaOf[RasterRef]), true))
  ))

  override def dataType: DataType = schemaOf[ArrowTensor]

  override protected def nullSafeEval(input: Any): Any = {
    implicit val ser = TensorUDT.tensorSerializer
    input match {
      case values: ArrayData =>
        val arrays = values.array.map { r =>
          val rref = row(r).to[RasterRef]
          rref.tile.toArrayDouble
        }
        arrays
      case other => sys.error(s"Cannot deserialize $other")
    }
  }
}

object RasterRefStackToTensor {
  def apply(rr: Column): TypedColumn[Any, ProjectedRasterTile] =
    new Column(RasterRefStackToTensor(rr.expr)).as[ProjectedRasterTile]
}
