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
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.accessors.{GetCRS, GetExtent}
import org.locationtech.rasterframes.expressions.generators.{RasterSourcesToTensorRefs, RasterSourceToTiles}
import org.locationtech.rasterframes.expressions.generators.RasterSourceToRasterRefs.bandNames
import org.locationtech.rasterframes.expressions.transformers._
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.tensors.ProjectedBufferedTensor

/**
  * Constructs a Spark Relation over one or more RasterSource paths.
  * @param sqlContext Query context
  * @param bandIndexes band indexes to fetch
  * @param subtileDims how big to tile/subdivide rasters info
  * @param lazyTiles if true, creates a lazy representation of tile instead of fetching contents.
  * @param spatialIndexPartitions Number of spatial index-based partitions to create.
  *                               If Option value > 0, that number of partitions are created after adding a spatial index.
  *                               If Option value <= 0, uses the value of `numShufflePartitions` in SparkContext.
  *                               If None, no spatial index is added and hash partitioning is used.
  */
case class TensorRelation(
  sqlContext: SQLContext,
  rsPaths: Seq[String],
  bandIndexes: Option[Seq[Int]],
  expandPatterns: Boolean,
  subtileDims: Option[TileDimensions],
  bufferPixels: Int,
  spatialIndexPartitions: Option[Int]
) extends BaseRelation with TableScan {

  protected def defaultNumPartitions: Int =
    sqlContext.sparkSession.sessionState.conf.numShufflePartitions

  override def schema: StructType = StructType(Seq(
    StructField("tensor_data", schemaOf[ProjectedBufferedTensor], true)
  ))
  // pre buffer
    //StructField("tensor", BufferedTensorType, true)

  import sqlContext.sparkSession.implicits._
  val catalog = rsPaths.toDF("path_pattern")

  override def buildScan(): RDD[Row] = {
    import sqlContext.implicits._
    val numParts = spatialIndexPartitions.filter(_ > 0).getOrElse(defaultNumPartitions)

    val df: DataFrame = {
      val srcs =
        PatternToRasterSources(col("path_pattern"), bandIndexes, expandPatterns) as "raster_source"

      val refs =
        RasterSourcesToTensorRefs(subtileDims, srcs) as "tensor_ref"

      val tens =
        TensorRefToTensor(col("tensor_ref"), bufferPixels) as "tensor_data"

      catalog.select(refs).select(tens)
    }

    if (spatialIndexPartitions.isDefined) {
      val indexed = df
        .withColumn("spatial_index", XZ2Indexer(GetExtent(col("tensor_ref")), GetCRS(col("tensor_ref"))))
        .repartitionByRange(numParts,$"spatial_index")
      indexed.rdd
    }
    else df.rdd
  }
}
