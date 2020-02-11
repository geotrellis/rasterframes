/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Azavea, Inc.
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

package org.apache.spark.sql.rf

import geotrellis.raster._
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, _}
import org.locationtech.rasterframes.TensorType
import org.locationtech.rasterframes.encoders.CatalystSerializer
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.model.{Cells, TileDataContext}
import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile
import org.locationtech.rasterframes.tiles.InternalRowTile

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

@SQLUserDefinedType(udt = classOf[BufferedTensorUDT])
class BufferedTensorUDT extends UserDefinedType[BufferedTensor] {
  import BufferedTensorUDT._
  override def typeName = BufferedTensorUDT.typeName

  override def pyUDT: String = "pyrasterframes.rf_types.BufferedTensorUDT"

  def userClass: Class[BufferedTensor] = classOf[BufferedTensor]

  def sqlType: StructType = schemaOf[BufferedTensor]

  override def serialize(obj: BufferedTensor): InternalRow =
    Option(obj)
      .map(_.toInternalRow)
      .orNull

  override def deserialize(datum: Any): BufferedTensor = {
    // logger.warn(s"Deserializing from $datum")
    Option(datum)
      .collect {
        case ir: InternalRow ⇒ ir.to[BufferedTensor]
      }
      .orNull
  }

  override def acceptsType(dataType: DataType): Boolean = dataType match {
    case _: BufferedTensorUDT ⇒ true
    case _ ⇒ super.acceptsType(dataType)
  }
}

object BufferedTensorUDT  {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  UDTRegistration.register(classOf[BufferedTensor].getName, classOf[BufferedTensorUDT].getName)
  logger.warn(s"Registered BufferedTensor")

  final val typeName: String = "buffered_tensor"

  implicit def bufferedTensorSerializer: CatalystSerializer[BufferedTensor] = new CatalystSerializer[BufferedTensor] {
    import org.apache.spark.sql.rf.TensorUDT._


    override val schema: StructType = StructType(Seq(
      StructField("arrow_tensor", TensorType, false),
      StructField("extent", schemaOf[Extent], true),
      StructField("x_buffer", IntegerType, false),
      StructField("y_buffer", IntegerType, false)
    ))

    override def to[R](t: BufferedTensor, io: CatalystIO[R]): R = {
      io.create (
        io.to(t.tensor),
        t.extent.map(io.to(_)).getOrElse(null),
        t.bufferCols,
        t.bufferRows
      )
    }

    override def from[R](row: R, io: CatalystIO[R]): BufferedTensor = {
      val tensor = io.get[ArrowTensor](row, 0)
      val extent =
        if (io.isNullAt(row, 1))
          None
        else
          Some(io.get[Extent](row, 1))
      val bx = io.getInt(row, 2)
      val by = io.getInt(row, 3)
      new BufferedTensor(tensor, bx, by, extent)
    }
  }
}
