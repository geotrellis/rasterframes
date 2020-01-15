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

package org.locationtech.rasterframes.datasource

import org.apache.spark.sql.DataFrameReader
import org.locationtech.rasterframes.datasource.tensor.TensorDataSource._
import shapeless.tag
import shapeless.tag.@@

package object tensor {

  trait TensorDataFrameReaderTag
  type TensorDataFrameReader = DataFrameReader @@ TensorDataFrameReaderTag

  /** Adds `raster` format specifier to `DataFrameReader`. */
  implicit class DataFrameReaderHasTensorFormat(val reader: DataFrameReader) {
    def tensor: TensorDataFrameReader =
      tag[TensorDataFrameReaderTag][DataFrameReader](
        reader.format(TensorDataSource.SHORT_NAME))
  }

  /** Adds option methods relevant to TensorDataSource. */
  implicit class TensorDataFrameReaderHasOptions(val reader: TensorDataFrameReader)
    extends CatalogReaderOptionsSupport[TensorDataFrameReaderTag] with
      SpatialIndexOptionsSupport[TensorDataFrameReaderTag]
}
