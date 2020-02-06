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

import geotrellis.raster._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.locationtech.rasterframes
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import org.locationtech.rasterframes.ref.{TensorRef, DeferredTensorRef}
import org.locationtech.rasterframes.tensors.RFTensor

/** Represents the union of binary cell datas or a reference to the data.*/
case class Voxels(data: Either[Array[Byte], TensorRef]) {
  def isRef: Boolean = data.isRight

  /** Convert voxels into either a DeferredTensorRef or an ArrowTensor. */
  def toTensor(ctx: TensorDataContext): RFTensor = data.fold(
    bytes => {
      val nakedTensor = ArrowTensor.fromArrowMessage(bytes)
      BufferedTensor(ArrowTensor.fromArrowMessage, ctx.bufferPixels)
    },
    ref => DeferredTensorRef(ref)
  )
}

object Voxels {
  /** Extracts the Voxels from a Tensor. */
  def apply(t: RFTensor): Voxels = {
    t match {
      case arrowTensor: ArrowTensor =>
        Voxels(Left(arrowTensor.toArrowBytes()))
      case ref: DeferredTensorRef =>
        Voxels(Right(ref.deferred))
      case const: ConstantTile =>
        throw new IllegalArgumentException
    }
  }

  implicit def voxelsSerializer: CatalystSerializer[Voxels] = new CatalystSerializer[Voxels] {
    override val schema: StructType =
      StructType(
        Seq(
          StructField("voxels", BinaryType, true),
          StructField("ref", schemaOf[TensorRef], true)
        ))
    override protected def to[R](t: Voxels, io: CatalystSerializer.CatalystIO[R]): R = io.create(
      t.data.left.getOrElse(null),
      t.data.right.map(tr => io.to(tr)).right.getOrElse(null)
    )
    override protected def from[R](t: R, io: CatalystSerializer.CatalystIO[R]): Voxels = {
      if (!io.isNullAt(t, 0))
        Voxels(Left(io.getByteArray(t, 0)))
      else if (!io.isNullAt(t, 1))
        Voxels(Right(io.get[TensorRef](t, 1)))
      else throw new IllegalArgumentException("must be either arrow tensor data or a ref, but not null")
    }
  }

  implicit def encoder: ExpressionEncoder[Voxels] = CatalystSerializerEncoder[Voxels]()
}
