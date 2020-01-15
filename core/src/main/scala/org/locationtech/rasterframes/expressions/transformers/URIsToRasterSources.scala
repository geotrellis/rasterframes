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
import org.apache.spark.sql.types.{DataType, StringType, ArrayType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.locationtech.rasterframes.RasterSourceType
import org.locationtech.rasterframes.ref.RasterSource
import org.slf4j.LoggerFactory

/**
 * Catalyst generator to convert a geotiff download URLs into a series of rows
 * containing references to the internal tiles and associated extents.
 *
 * @since 5/4/18
 */
case class URIsToRasterSources(override val child: Expression)
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))


  override def nodeName: String = "rf_uris_to_raster_sources"

  override def dataType: DataType = ArrayType(RasterSourceType, true)

  override def inputTypes = Seq(ArrayType(StringType, true))

  override protected def nullSafeEval(input: Any): Any =  {
    //val uriString = input.asInstanceOf[Array[String]].map(_.toString)
    input match {
      case values: ArrayData =>
        new GenericArrayData(values.toArray[UTF8String](StringType).map { uriString =>
          val uri = URI.create(uriString.toString)
          val ref = RasterSource(uri)
          RasterSourceType.serialize(ref)
        })
      case other => sys.error(s"Cannot deserialize $other")
    }
  }
}

object URIsToRasterSources {
  def apply(rasterURI: Column): TypedColumn[Any, Array[RasterSource]] =
    new Column(new URIsToRasterSources(rasterURI.expr)).as[Array[RasterSource]]
}
