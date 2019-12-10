package org.locationtech.rasterframes

import geotrellis.raster.ArrowTensor
import org.scalatest._

class ArrowTensorSpec extends FunSpec {
  it("round trips through ipc message") {
    val tensor = ArrowTensor.fromArray(Array(13),1, 1, 1)
    val arr = tensor.toArrowBytes()
    ArrowTensor.fromArrowMessage(arr)
  }
}
