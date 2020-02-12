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

import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{StructField, StructType}
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes.tensors.{RFTensor, ProjectedBufferedTensor}

case class TensorDataContext(
  extent: Extent, crs: CRS, depth: Int, cols: Int, rows: Int
) {
  def projectedExtent: ProjectedExtent = ProjectedExtent(extent, crs)
}

object TensorDataContext {
  def apply(pbt: ProjectedBufferedTensor): TensorDataContext =
    TensorDataContext(pbt.extent, pbt.crs, pbt.depth, pbt.cols, pbt.rows)
  def unapply(tensor: RFTensor): Option[(Extent, CRS)] = tensor match {
    case prt: ProjectedRasterTile => Some((prt.extent, prt.crs))
    case _ => None
  }
  implicit val serializer: CatalystSerializer[TensorDataContext] = new CatalystSerializer[TensorDataContext] {
    override val schema: StructType = StructType(Seq(
      StructField("extent", schemaOf[Extent], false),
      StructField("crs", schemaOf[CRS], false)
    ))
    override protected def to[R](t: TensorDataContext, io: CatalystSerializer.CatalystIO[R]): R = io.create(
      io.to(t.extent),
      io.to(t.crs)
    )
    override protected def from[R](t: R, io: CatalystSerializer.CatalystIO[R]): TensorDataContext = TensorDataContext(
      io.get[Extent](t, 0),
      io.get[CRS](t, 1),
      io.getInt(t, 2),
      io.getInt(t, 3),
      io.getInt(t, 4)
    )
  }
  implicit def encoder: ExpressionEncoder[TensorDataContext] = CatalystSerializerEncoder[TensorDataContext]()
}
