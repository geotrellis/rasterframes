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

package org.locationtech.rasterframes.expressions.transformers

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.locationtech.rasterframes.BufferedTensorType
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.row
import org.locationtech.rasterframes.ref.TensorRef
import org.locationtech.rasterframes.tensors.ProjectedBufferedTensor
import org.locationtech.rasterframes.encoders.StandardEncoders._
import org.locationtech.rasterframes.encoders._

import org.slf4j.LoggerFactory

/**
 */
case class TensorRefToTensor(child: Expression, bufferPixels: Int) extends UnaryExpression
  with CodegenFallback with ExpectsInputTypes {

  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def nodeName: String = "tensor_ref_to_tensor"

  override def inputTypes = Seq(schemaOf[TensorRef])

  override def dataType: DataType = schemaOf[ProjectedBufferedTensor]

  override protected def nullSafeEval(input: Any): Any = {
    implicit val ser = ProjectedBufferedTensor.serializer
    val ref = row(input).to[TensorRef]
    val realized = ref.realizedTensor(bufferPixels)

    realized.toInternalRow
  }
}

object TensorRefToTensor {
  def apply(rr: Column, bufferPixels: Int): TypedColumn[Any, ProjectedBufferedTensor] =
    new Column(TensorRefToTensor(rr.expr, bufferPixels)).as[ProjectedBufferedTensor]
}
