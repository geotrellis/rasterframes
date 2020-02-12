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

import geotrellis.raster.{ArrayTile, ConstantTile, Tile}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.locationtech.rasterframes
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import org.locationtech.rasterframes.ref.TensorRef
import org.locationtech.rasterframes.tensors.RFTensor
import org.locationtech.rasterframes.tiles.ProjectedRasterTile.ConcreteProjectedRasterTile

/** Represents the union of binary cell datas or a reference to the data.*/
case class TensorData(data: Either[Array[Byte], TensorRef]) {
  def isRef: Boolean = data.isRight

  /** Convert TensorData into either a RasterRefTile or an ArrayTile. */
  def toTensor(ctx: TensorDataContext): RFTensor = {
    data.fold(
      bytes => {
        // ArrowTensor.fromArrowMessage(bytes)
        RFTensor.fromArrowMessage(bytes, ctx)
      },
      ref => ref.delayed
    )
  }
}

object TensorData {
  /** Extracts the TensorData from a Tile. */
  def apply(t: RFTensor): TensorData = {
    t match {
      case delay: TensorRef.Delay =>
        TensorData(Right(delay.tr))
      case o =>
        TensorData(Left(o.toArrowBytes))
    }
  }

  implicit def tensorDataSerializer: CatalystSerializer[TensorData] = new CatalystSerializer[TensorData] {
    override val schema: StructType =
      StructType(
        Seq(
          StructField("TensorData", BinaryType, true),
          StructField("ref", schemaOf[TensorRef], true)
        ))
    override protected def to[R](t: TensorData, io: CatalystSerializer.CatalystIO[R]): R = io.create(
      t.data.left.getOrElse(null),
      t.data.right.map(rr => io.to(rr)).right.getOrElse(null)
    )
    override protected def from[R](t: R, io: CatalystSerializer.CatalystIO[R]): TensorData = {
      if (!io.isNullAt(t, 0))
        TensorData(Left(io.getByteArray(t, 0)))
      else if (!io.isNullAt(t, 1))
        TensorData(Right(io.get[TensorRef](t, 1)))
      else throw new IllegalArgumentException("must be eithe cell data or a ref, but not null")
    }
  }

  implicit def encoder: ExpressionEncoder[TensorData] = CatalystSerializerEncoder[TensorData]()
}
