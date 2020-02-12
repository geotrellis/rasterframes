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
import org.apache.spark.sql.rf.BufferedTensorUDT
import org.locationtech.rasterframes.BufferedTensorType
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import org.locationtech.rasterframes.model.TensorDataContext
import org.locationtech.rasterframes.ref.ProjectedRasterLike
import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile
import org.locationtech.rasterframes.encoders.StandardEncoders._

/**
 * A Tile that's also like a ProjectedRaster, with delayed evaluation support.
 *
 * @since 9/5/18
 */

trait RFTensor {
  def toArrowBytes(): Array[Byte]
}

object RFTensor {
  def fromArrowMessage(bytes: Array[Byte], context: TensorDataContext): RFTensor = ???
}