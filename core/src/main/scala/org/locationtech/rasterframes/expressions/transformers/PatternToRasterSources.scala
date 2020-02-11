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

import java.net.URI

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, ArrayType, StructType, StructField}
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.rasterframes.RasterSourceType
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.locationtech.rasterframes.ref.RasterSource
import org.locationtech.rasterframes.encoders.CatalystSerializer
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.ref.TensorRef._
import org.locationtech.rasterframes.ref._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

import org.slf4j.LoggerFactory

import java.net.URI

/**
 *
 * @since 5/4/18
 */
case class PatternToRasterSources(override val child: Expression, bands: Option[Seq[Int]], expandPatterns: Boolean)
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback {
    import PatternToRasterSources._
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))


  override def nodeName: String = "rf_pattern_to_raster_sources"

  override def inputTypes = Seq(StringType)

  override def dataType: DataType = ArrayType(schemaOf[RasterSourceWithBand])

  override protected def nullSafeEval(input: Any): Any =  {
    val pattern = input.asInstanceOf[UTF8String].toString
    val sources: Seq[(String, Int)] =
      if (expandPatterns) {
        bands                             // do printf-style replacement
          .map(_.map(pattern.format(_)))  // ... on all supplied bands
          .getOrElse(Seq(0).map(pattern.format(_))) // ... or default to zero
          .map((_, 0)) // Always read 0 band
      } else {
        bands
          .getOrElse(Seq(0))
          .map((pattern, _))
      }
    val expanded = sources.map({ case (str, bnd) => RasterSourceWithBand(RasterSource(URI.create(str)), bnd) })

    new GenericArrayData(expanded.map(_.toInternalRow))
  }
}

object PatternToRasterSources {
  case class RasterSourceWithBand(source: RasterSource, band: Int)

  implicit val bandedRSSerializer: CatalystSerializer[RasterSourceWithBand] =
    new CatalystSerializer[RasterSourceWithBand] {
      import org.apache.spark.sql.rf.RasterSourceUDT._

      override val schema: StructType =
        StructType(Seq(
          StructField("source", RasterSourceType, false),
          StructField("band", IntegerType, false)
        ))

      override def to[R](t: RasterSourceWithBand, io: CatalystIO[R]): R = io.create(
        io.to(t.source),
        t.band
)

      override def from[R](row: R, io: CatalystIO[R]): RasterSourceWithBand =
        RasterSourceWithBand(io.get[RasterSource](row, 0), io.getInt(row, 1))
    }

  implicit val rsArrayEncoder: Encoder[Array[RasterSource]] =
    ExpressionEncoder[Array[RasterSource]]()

  def apply(rasterURI: Column, bands: Option[Seq[Int]]=None, expandPatterns: Boolean): TypedColumn[Any, Array[RasterSource]] =
    new Column(new PatternToRasterSources(rasterURI.expr, bands, expandPatterns)).as[Array[RasterSource]]
}
