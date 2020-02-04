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

package org.locationtech.rasterframes.ref

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridBounds, Tile, ArrowTensor, BufferedTensor, RasterExtent}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.RasterSourceUDT
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, ArrayType}
import org.apache.spark.sql.Encoder
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.CatalystSerializer.{CatalystIO, _}
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import org.locationtech.rasterframes.ref.RasterSource._
import org.locationtech.rasterframes.tensors.ProjectedBufferedTensor


/**
 * A delayed-read projected raster implementation.
 *
 * @since 8/21/18
 */
case class TensorRef(sources: Seq[(RasterSource, Int)], subextent: Option[Extent], subgrid: Option[GridBounds])
  extends ProjectedRasterLike {
  def sample = sources.head._1
  def crs: CRS = sample.crs
  def cols: Int = grid.width
  def rows: Int = grid.height
  def cellType: CellType = sample.cellType
  //def tile: ProjectedRasterTile = RasterRefTile(this)


  protected lazy val grid: GridBounds =
    subgrid.getOrElse(sample.rasterExtent.gridBoundsFor(extent, true))

  lazy val extent: Extent = RasterExtent(sample.extent, sample.cellSize).extentFor(grid)

  lazy val realizedTensor: ArrowTensor = {
    //RasterRef.log.trace(s"Fetching $extent ($grid) from band $bandIndex of $sample")
    val tiles = sources.map({ case (rs, band) =>
      rs.read(grid, Seq(band)).tile.band(0)
    })
    ArrowTensor.stackTiles(tiles)
  }

  def realizedTensor(bufferPixels: Int): ProjectedBufferedTensor = {
    //RasterRef.log.trace(s"Fetching $extent ($grid) from band $bandIndex of $sample")
    val bufferedGrid = grid.buffer(bufferPixels)

    val tiles = sources.map({ case (rs, band) =>
      val tile = rs.read(bufferedGrid, Seq(band)).tile.band(0)

      val rsBounds =
        GridBounds(0, 0, rs.cols - 1, rs.rows - 1)
      val cropBounds =
        TensorRef.bufferedCropBounds(rsBounds, bufferedGrid, bufferPixels)
      val cropOpts =
        geotrellis.raster.crop.Crop.Options(clamp=false)
      val ndBuffered = 
        tile.crop(cropBounds, cropOpts)

      ndBuffered 
    })

    val bufferedTensor =
      BufferedTensor(ArrowTensor.stackTiles(tiles), bufferPixels, bufferPixels, Some(extent))

    ProjectedBufferedTensor(bufferedTensor, extent, crs)
  }
}


object TensorRef extends LazyLogging {
  import RasterSourceUDT._
  private val log = logger

  // This function is here to provide gridbounds for padding (via crop w/ clamp=false)
  def bufferedCropBounds(totalBounds: GridBounds, readBounds: GridBounds, bufferPixels: Int): GridBounds = {
    val rsCols = totalBounds.colMax - totalBounds.colMin
    val rsRows = totalBounds.rowMax - totalBounds.rowMin
    val readCols = readBounds.colMax - readBounds.colMin
    val readRows = readBounds.rowMax - readBounds.rowMin

    val colCropMin =
      if (readBounds.colMin == 0) -bufferPixels else 0
    val colCropMax =
      if (readBounds.colMax == totalBounds.colMax) readCols + bufferPixels else readCols
    val rowCropMin =
      if (readBounds.rowMin == 0) -bufferPixels else 0
    val rowCropMax =
      if (readBounds.rowMax == totalBounds.rowMax) readRows + bufferPixels else readRows

    GridBounds(colCropMin, rowCropMin, colCropMax, rowCropMax)
  }

  implicit val rsBandSerializer: CatalystSerializer[(RasterSource, Int)] =
    new CatalystSerializer[(RasterSource, Int)] {
      override val schema: StructType =
        StructType(Seq(
          StructField("rasterSource", RasterSourceType, false),
          StructField("bandIndex", IntegerType, false)
        ))

    override def to[R](t: (RasterSource, Int), io: CatalystIO[R]): R = io.create(
      io.to(t._1),
      t._2
    )

    override def from[R](row: R, io: CatalystIO[R]): (RasterSource, Int) = (
      io.get[RasterSource](row, 0),
      io.getInt(row, 1)
    )
  }

  implicit val tensorRefSerializer: CatalystSerializer[TensorRef] = new CatalystSerializer[TensorRef] {
    override val schema: StructType = StructType(Seq(
      StructField("sources", ArrayType(schemaOf[(RasterSource, Int)]), false),
      StructField("subextent", schemaOf[Extent], true),
      StructField("subgrid", schemaOf[GridBounds], true)
    ))

    override def to[R](t: TensorRef, io: CatalystIO[R]): R = io.create(
      io.toSeq(t.sources),
      t.subextent.map(io.to[Extent]).orNull,
      t.subgrid.map(io.to[GridBounds]).orNull
    )

    override def from[R](row: R, io: CatalystIO[R]): TensorRef = TensorRef(
      io.getSeq[(RasterSource, Int)](row, 0),
      if (io.isNullAt(row, 1)) None
      else Option(io.get[Extent](row, 1)),
      if (io.isNullAt(row, 2)) None
      else Option(io.get[GridBounds](row, 2))
    )
  }

  implicit def rrEncoder: ExpressionEncoder[TensorRef] = CatalystSerializerEncoder[TensorRef](true)
}
