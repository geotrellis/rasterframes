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

import java.nio.ByteBuffer

import org.locationtech.rasterframes.encoders.CatalystSerializer.CatalystIO
import geotrellis.raster._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.rasterframes.model.{TensorData, TensorDataContext}

/**
 * Wrapper around a `Tile` encoded in a Catalyst `InternalRow`, for the purpose
 * of providing compatible semantics over common operations.
 *
 * @since 11/29/17
 */
class InternalRowTensor(val mem: InternalRow) extends RFTensor {
  import InternalRowTensor._

  // TODO: We want to reimplement relevant delegated methods so that they read directly from tungsten storage
  lazy val realizedTile: RFTensor = voxels.toTensor(context)

  private def context: TensorDataContext =
    CatalystIO[InternalRow].get[TensorDataContext](mem, 0)

  private def voxels: TensorData =
    CatalystIO[InternalRow]
      .get[TensorData](mem, 1)

  /** Retrieve the depth from the internal encoding. */
  override def depth: Int = context.depth

  /** Retrieve the number of rows from the internal encoding. */
  override def rows: Int = context.rows

  /** Retrieve the number of columns from the internal encoding. */
  override def cols: Int = context.cols

  /** Get the internally encoded tile data cells. */
  override lazy val toBytes: Array[Byte] = {
    voxels.data.left
      .getOrElse(throw new IllegalStateException(
        "Expected tile cell bytes, but received RasterRef instead: " + voxels.data.right.get)
      )
  }

  // private lazy val toByteBuffer: ByteBuffer = {
  //   val data = toBytes
  //   if(data.length < cols * rows && cellType.name != "bool") {
  //     // Handling constant tiles like this is inefficient and ugly. All the edge
  //     // cases associated with them create too much undue complexity for
  //     // something that's unlikely to be
  //     // used much in production to warrant handling them specially.
  //     // If a more efficient handling is necessary, consider a flag in
  //     // the UDT struct.
  //     ByteBuffer.wrap(toArrayTile().toBytes())
  //   } else ByteBuffer.wrap(data)
  // }

  /** Reads the cell value at the given index as an Int. */
  // def apply(i: Int): Int = cellReader.apply(i)

  /** Reads the cell value at the given index as a Double. */
  // def applyDouble(i: Int): Double = cellReader.applyDouble(i)

  def copy = new InternalRowTensor(mem.copy)
}

object InternalRowTensor {}
