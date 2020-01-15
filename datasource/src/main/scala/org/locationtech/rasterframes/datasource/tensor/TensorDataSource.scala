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

package org.locationtech.rasterframes.datasource.tensor

import java.net.URI
import java.util.UUID

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.util._
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.locationtech.rasterframes.model.TileDimensions
import shapeless.tag
import shapeless.tag.@@

import scala.util.Try

class TensorDataSource extends DataSourceRegister with RelationProvider {
  import TensorDataSource._
  override def shortName(): String = SHORT_NAME
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val tiling = parameters.tileDims.orElse(Some(NOMINAL_TILE_DIMS))
    val lazyTiles = parameters.lazyTiles
    val spatialIndex = parameters.spatialIndex
    val paths = parameters.paths
    TensorRelation(sqlContext, paths, tiling, lazyTiles, spatialIndex)
  }
}

object TensorDataSource {
  final val SHORT_NAME = "tensor"
  final val TIFF_PATHS_PARAM = "paths"
  final val TILE_DIMS_PARAM = "tile_dimensions"
  final val LAZY_TILES_PARAM = "lazy_tiles"
  final val SPATIAL_INDEX_PARTITIONS_PARAM = "spatial_index_partitions"

  final val DEFAULT_COLUMN_NAME = PROJECTED_RASTER_COLUMN.columnName

  implicit class ParamsDictAccessors(val parameters: Map[String, String]) extends AnyVal {
    def tokenize(csv: String): Seq[String] = csv.split(',').map(_.trim)

    def tileDims: Option[TileDimensions] =
      parameters.get(TILE_DIMS_PARAM)
        .map(tokenize(_).map(_.toInt))
        .map { case Seq(cols, rows) => TileDimensions(cols, rows)}

    def lazyTiles: Boolean = parameters
      .get(LAZY_TILES_PARAM).forall(_.toBoolean)

    def spatialIndex: Option[Int] = parameters
      .get(SPATIAL_INDEX_PARTITIONS_PARAM).flatMap(p => Try(p.toInt).toOption)

    def paths: Seq[String] = parameters
      .get(TIFF_PATHS_PARAM)
      .map(tokenize(_).filter(_.nonEmpty).toSeq)
      .getOrElse(Seq.empty)
  }

  /** Mixin for adding extension methods on DataFrameReader for TensorDataSource-like readers. */
  trait SpatialIndexOptionsSupport[ReaderTag] {
    type _TaggedReader = DataFrameReader @@ ReaderTag
    val reader: _TaggedReader
    def withSpatialIndex(numPartitions: Int = -1): _TaggedReader =
      tag[ReaderTag][DataFrameReader](
        reader.option(TensorDataSource.SPATIAL_INDEX_PARTITIONS_PARAM, numPartitions)
      )
  }

  /** Mixin for adding extension methods on DataFrameReader for TensorDataSource-like readers. */
  trait CatalogReaderOptionsSupport[ReaderTag] {
    type TaggedReader = DataFrameReader @@ ReaderTag
    val reader: TaggedReader

    protected def tmpTableName() = UUID.randomUUID().toString.replace("-", "")

    def withTileDimensions(cols: Int, rows: Int): TaggedReader =
      tag[ReaderTag][DataFrameReader](
        reader.option(TensorDataSource.TILE_DIMS_PARAM, s"$cols,$rows")
      )

    /** Indicate if tile reading should be delayed until cells are fetched. Defaults to `true`. */
    def withLazyTiles(state: Boolean): TaggedReader =
      tag[ReaderTag][DataFrameReader](
        reader.option(TensorDataSource.LAZY_TILES_PARAM, state))

    def from(commaDelimPaths: String): TaggedReader =
      tag[ReaderTag][DataFrameReader](
        reader.option(TensorDataSource.TIFF_PATHS_PARAM, commaDelimPaths)
      )

    def from(paths: Seq[String]): TaggedReader =
      from(paths.mkString(","))

    def from(uris: Seq[URI])(implicit d: DummyImplicit): TaggedReader =
      from(uris.map(_.toASCIIString))
  }
}
