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

package org.locationtech.rasterframes.model

import org.locationtech.rasterframes.encoders.CatalystSerializer._
import geotrellis.raster.{CellType, Tile, ArrowTensor}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{StructField, StructType, IntegerType}
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}

/** Encapsulates all information about a tile aside from actual cell values. */
case class TensorDataContext(depth: Int, rows: Int, cols: Int, bufferPixels: Int)
object TensorDataContext {

  /** Extracts the TensorDataContext from a Tile. */
  def apply(t: RFTensor): TensorDataContext = {
    require(t.depth <= Short.MaxValue, s"RasterFrames doesn't support tiles of size ${t.depth}")
    require(t.cols <= Short.MaxValue, s"RasterFrames doesn't support tiles of size ${t.cols}")
    require(t.rows <= Short.MaxValue, s"RasterFrames doesn't support tiles of size ${t.rows}")
    TensorDataContext(t.shape(0), t.shape(1), t.shape(2), bufferPixels)
  }

  implicit val serializer: CatalystSerializer[TensorDataContext] = new CatalystSerializer[TensorDataContext] {
    override val schema: StructType =  StructType(Seq(
      StructField("depth", IntegerType, false),
      StructField("cols", IntegerType, false),
      StructField("rows", IntegerType, false),
      StructField("bufferPixels", IntegerType, false)
    ))

    override protected def to[R](t: TensorDataContext, io: CatalystIO[R]): R = io.create(
      t.depth,
      t.rows,
      t.cols,
      t.bufferPixels
    )
    override protected def from[R](t: R, io: CatalystIO[R]): TensorDataContext = TensorDataContext(
      io.getInt(t, 0),
      io.getInt(t, 1),
      io.getInt(t, 2),
      io.getInt(t, 3)
    )
  }

  implicit def encoder: ExpressionEncoder[TensorDataContext] = CatalystSerializerEncoder[TensorDataContext]()
}
