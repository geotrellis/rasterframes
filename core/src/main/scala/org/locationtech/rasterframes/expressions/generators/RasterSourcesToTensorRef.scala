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

import geotrellis.raster.GridBounds
import geotrellis.vector.Extent
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{DataType, StructField, StructType, ArrayType}
import org.locationtech.rasterframes.encoders.CatalystSerializer
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.generators.RasterSourceToRasterRefs.bandNames
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.ref.{RasterRef, RasterSource, TensorRef}
import org.locationtech.rasterframes.util._
import org.locationtech.rasterframes.RasterSourceType

import scala.collection.mutable._
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Accepts RasterSource and generates one or more RasterRef instances representing
 *
 * @since 9/6/18
 */
// BUFFER HERE
case class RasterSourcesToTensorRefs(child: Expression, subtileDims: Option[TileDimensions] = None) extends UnaryExpression
  with Generator with CodegenFallback with ExpectsInputTypes {
    import TensorRef._

  override def nodeName: String = "rf_raster_sources_to_tensor_ref"

  override def inputTypes: Seq[DataType] = Seq(ArrayType(RasterSourceType))

  override def dataType: DataType = ArrayType(schemaOf[TensorRef])

  override def elementSchema: StructType =
    StructType(Seq(StructField("tensor", schemaOf[TensorRef], true)))

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    // try {
      val data = child.eval(input).asInstanceOf[ArrayData]

      val rss: Array[(RasterSource, Int)] = {
        val result = new Array[(RasterSource, Int)](data.numElements)
        data.foreach(RasterSourceType, (i, e) => {
          result(i) = (RasterSourceType.deserialize(e), 0)
        })
        result
      }
      println(s"Got arr=$rss")

      val (sampleRS, _) = rss.head

      val maybeSubs = subtileDims.map { dims =>
        val subGB = sampleRS.layoutBounds(dims)
        subGB.map(gb => (gb, sampleRS.rasterExtent.extentFor(gb, clamp = true)))
      }

      val trefs = maybeSubs.map { subs =>
        subs.map { case (gb, extent) => TensorRef(rss, Some(extent), Some(gb)) }
      }.getOrElse(Seq(TensorRef(rss, None, None)))

      println(s"TensorRefs: $trefs")

      println(s"${trefs.map(_.toInternalRow)}")
      trefs.map{ tref => InternalRow(tref.toInternalRow) }
    // }
    // catch {
    //   case _: Throwable => ???
    //   // case NonFatal(ex) ⇒
    //   //   val description = "Error fetching data for one of: " +
    //   //     Try(children.map(c => RasterSourceType.deserialize(c.eval(input))))
    //   //       .toOption.toSeq.flatten.mkString(", ")
    //   //   throw new java.lang.IllegalArgumentException(description, ex)
    // }
  }
}

object RasterSourcesToTensorRefs {
  def apply(brs: Column): TypedColumn[Any, TensorRef] = apply(None, brs)
  def apply(subtileDims: Option[TileDimensions], brs: Column): TypedColumn[Any, TensorRef] =
    new Column(new RasterSourcesToTensorRefs(brs.expr, subtileDims)).as[TensorRef]
}
