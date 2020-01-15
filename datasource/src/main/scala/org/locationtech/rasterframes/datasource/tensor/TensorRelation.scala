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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, ArrayType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, Column}
import org.locationtech.rasterframes.datasource.raster.RasterSourceDataSource.RasterSourceCatalogRef
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.accessors.{GetCRS, GetExtent}
import org.locationtech.rasterframes.expressions.generators.{RasterSourceToRasterRefs, RasterSourceToTiles}
import org.locationtech.rasterframes.expressions.generators.RasterSourceToRasterRefs.bandNames
import org.locationtech.rasterframes.expressions.transformers._
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes.ref.RasterRef
import org.locationtech.rasterframes.expressions.generators.RasterSourcesToRasterRefStack
import org.locationtech.rasterframes.ref.RasterSource

import geotrellis.raster.GridBounds

import java.net.URI
import collection.JavaConverters._

/**
  * Constructs a Spark Relation over one or more RasterSource paths.
  * @param sqlContext Query context
  * @param catalogTable Specification of raster path sources
  * @param subtileDims how big to tile/subdivide rasters info
  * @param lazyTiles if true, creates a lazy representation of tile instead of fetching contents.
  * @param spatialIndexPartitions Number of spatial index-based partitions to create.
  *                               If Option value > 0, that number of partitions are created after adding a spatial index.
  *                               If Option value <= 0, uses the value of `numShufflePartitions` in SparkContext.
  *                               If None, no spatial index is added and hash partitioning is used.
  */
case class TensorRelation(
  sqlContext: SQLContext,
  catalog: Seq[String],
  subtileDims: Option[TileDimensions],
  lazyTiles: Boolean,
  spatialIndexPartitions: Option[Int]
) extends BaseRelation with TableScan {

  lazy val indexCols: Seq[StructField] =
    if (spatialIndexPartitions.isDefined) Seq(StructField("spatial_index", LongType, false)) else Seq.empty

  protected def defaultNumPartitions: Int =
    sqlContext.sparkSession.sessionState.conf.numShufflePartitions

  override def schema: StructType = {
    StructType(
      Array(StructField("tensor", ArrayType(schemaOf[RasterRef]), true))
    )
  }

  override def buildScan(): RDD[Row] = {
    import sqlContext.implicits._
    val numParts = spatialIndexPartitions.filter(_ > 0).getOrElse(defaultNumPartitions)

    val rsDataFrame = sqlContext.createDataFrame(
      List(Row(catalog)).asJava,
      StructType(Array(
        StructField("raster_urls", ArrayType(StringType))
      ))
    )

    val rss = URIsToRasterSources(rsDataFrame("raster_urls")) as "rss"

    // I need to figure out how to produce the gridbounds and generate 1 row/unique gridbound
    // val layoutBounds: List[GridBounds] = subtileDims.toList.map { dims =>
    //   rss.take(1).layoutBounds(dims)
    // }

    // val zipped =
    //   Stream.continually(rss).zip(layoutBounds).toList

    val df = {
      val refs = RasterSourcesToRasterRefStack(subtileDims, rss) as "ref_stack"

      // RasterSourceToRasterRef is a generator, which means you have to do the Tile conversion
      // in a separate select statement (Query planner doesn't know how many columns ahead of time).
      // val refsToTiles = for {
      //   (refColName, tileColName) <- refColNames.zip(tileColNames)
      // } yield RasterRefToTile(col(refColName)) as tileColName

      // withPaths
      //   .select(paths :+ refs: _*)
      //   .select(paths ++ refsToTiles ++ extras: _*)
      // refsToTiles
      val tensor = RasterRefStackToTensor(refs.asInstanceOf[Column]) as "tensor"
      rsDataFrame.withColumn("tensor", tensor)
    }

    if (spatialIndexPartitions.isDefined) {
      // val sample = col(tileColNames.head)
      val sample = col("raster_reference_stack")
      val indexed = df
        .withColumn("spatial_index", XZ2Indexer(GetExtent(sample), GetCRS(sample)))
        .repartitionByRange(numParts,$"spatial_index")
      indexed.rdd
    }
    else df.rdd
  }
}
