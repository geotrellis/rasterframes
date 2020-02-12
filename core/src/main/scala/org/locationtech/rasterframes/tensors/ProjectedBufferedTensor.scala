/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package org.locationtech.rasterframes.tensors

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.{CellType, ProjectedRaster, Tile}
import geotrellis.vector.{Extent, ProjectedExtent}
import geotrellis.raster.BufferedTensor
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.rf.{TensorUDT, BufferedTensorUDT}
import org.locationtech.rasterframes.BufferedTensorType
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import org.locationtech.rasterframes.model.{TensorData, TensorDataContext}
import org.locationtech.rasterframes.ref.ProjectedRasterLike
import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile
import org.locationtech.rasterframes.encoders.StandardEncoders._

/**
 * A Tile that's also like a ProjectedRaster, with delayed evaluation support.
 *
 * @since 9/5/18
 */
case class ProjectedBufferedTensor(tensor: BufferedTensor, extent: Extent, crs: CRS) extends RFTensor {
  def depth = tensor.tensor.depth
  def cols = tensor.tensor.cols
  def rows = tensor.tensor.rows
  def projectedExtent: ProjectedExtent =
    ProjectedExtent(extent, crs)

  def mapTensor(f: BufferedTensor => BufferedTensor): ProjectedBufferedTensor =
    ProjectedBufferedTensor(f(tensor), extent, crs)

  override def toString: String = {
    val e = s"(${extent.xmin}, ${extent.ymin}, ${extent.xmax}, ${extent.ymax})"
    val c = crs.toProj4String
    s"[$tensor, $e, $c]"
  }
}

object ProjectedBufferedTensor {
  import BufferedTensor._
  implicit val serializer: CatalystSerializer[ProjectedBufferedTensor] =
    new CatalystSerializer[ProjectedBufferedTensor] {
      override val schema: StructType = StructType(Seq(
        StructField("tensor_context", schemaOf[TensorDataContext], true),
        StructField("tensor", BufferedTensorType, false))
      )

      override protected def to[R](t: ProjectedBufferedTensor, io: CatalystIO[R]): R = io.create(
        io.to(TensorDataContext(t.extent, t.crs, t.depth, t.rows, t.cols)),
        io.to[TensorData](
          TensorData(Left(t.tensor.tensor.toArrowBytes()))
        )(TensorData.tensorDataSerializer)
      )

      override protected def from[R](t: R, io: CatalystIO[R]): ProjectedBufferedTensor = {
        val ctx = io.get[TensorDataContext](t, 0)
        val tensor = io.get[BufferedTensor](t, 1)(BufferedTensorUDT.bufferedTensorSerializer)
        ProjectedBufferedTensor(tensor, ctx.extent, ctx.crs)
      }
    }

  implicit val pbtEncoder: ExpressionEncoder[ProjectedBufferedTensor] =
    CatalystSerializerEncoder[ProjectedBufferedTensor](true)
}
