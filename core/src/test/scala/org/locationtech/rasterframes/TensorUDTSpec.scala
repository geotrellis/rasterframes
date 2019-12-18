/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Azavea, Inc.
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

package org.locationtech.rasterframes
import geotrellis.raster.ArrowTensor
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.scalatest.Inspectors

class TensorUDTSpec extends TestEnvironment with TestData with Inspectors {

  spark.version
  val tensorEncoder: ExpressionEncoder[ArrowTensor] = ExpressionEncoder()
  implicit val ser = TensorUDT.tensorSerializer

  describe("TensorUDT") {
    val sizes = Seq(1, 2, 4, 8, 11)

    def forEveryConfig(test: ArrowTensor ⇒ Unit): Unit = {
      forEvery(sizes.combinations(3).toSeq) { case Seq(xCount, yCount, zCount) ⇒
        val arr = for {
          xs <- Array.fill(xCount)(scala.math.random)
          ys <- Array.fill(yCount)(scala.math.random)
          zs <- Array.fill(zCount)(scala.math.random)
        } yield {
          xs * ys * zs
        }
        val tensor = ArrowTensor.fromArray(arr, xCount, yCount, zCount)
          test(tensor)
      }
    }

    it("should (en/de)code tile") {
      forEveryConfig { tensor =>
        val row = tensorEncoder.toRow(tensor)
        assert(!row.isNullAt(0))
        val tileAgain = TensorType.deserialize(row.getStruct(0, TensorType.sqlType.size))
        assert(tileAgain.shape === tensor.shape)
        assert(tileAgain.vector.get(0) === tensor.vector.get(0))
      }
    }

    it("should extract properties") {
      forEveryConfig { tensor =>
        val row = TensorType.serialize(tensor)
        val wrapper = row.to[ArrowTensor]
        assert(wrapper.shape === tensor.shape)
      }
    }

    it("should directly extract cells") {
      forEveryConfig { tensor =>
        val row = TensorType.serialize(tensor)
        val wrapper = row.to[ArrowTensor]
        val Seq(xCount, yCount, zCount) = wrapper.shape
        for {
          x <- 0 until xCount
          y <- 0 until yCount
          z <- 0 until zCount
        } yield {
          assert(wrapper.vector.get(x*y*z) === tensor.vector.get(x*y*z))
        }
      }
    }

    it("should round trip through a dataframe") {
      import spark.implicits._
      forEveryConfig { tensor =>
        val df = Seq(tensor).toDF("tensor")
        assert(df.select($"tensor").as[ArrowTensor].first().shape == tensor.shape)
      }
    }
  }
}
