package geotrellis.raster

import geotrellis.vector.Extent
import org.apache.arrow.vector.{Float8Vector, VectorSchemaRoot}
import spire.syntax.cfor._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, IntegerType}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.TensorUDT._

import org.locationtech.rasterframes.encoders.CatalystSerializerEncoder


/**
 * Container used to interpret the underlying cell data as having a buffer
 * region around the tile perimeter.
 *
 * @param tile: The raster data, including buffer
 * @param extent: The extent of the "core" data, not including the buffer cells
 * @param bufferSize: The number of cells around the boundary that are
 * considered as buffer data
 */
case class BufferedTensor(
  val tensor: ArrowTensor,
  val bufferRows: Int,
  val bufferCols: Int,
  val extent: Option[Extent]
) extends RFTensor {

  val cellType = DoubleCellType

  val cols = tensor.cols - bufferCols * 2
  val rows = tensor.rows - bufferRows * 2
  val bands = tensor.shape(0)
  val shape = Seq(bands, rows, cols)
  val rasterExtent = extent.map(RasterExtent(_, cols, rows))

  lazy val bufferedCols = tensor.cols
  lazy val bufferedRows = tensor.rows
  lazy val bufferedExtent = extent.map(_.expandBy(rasterExtent.get.cellwidth * bufferCols,
                                       rasterExtent.get.cellheight * bufferRows))

  def map(fn: Double => Double): BufferedTensor = {
    BufferedTensor(tensor.map(fn), bufferCols, bufferRows, extent)
  }

  def zipWith(other: BufferedTensor)(fn: (Double, Double) => Double): BufferedTensor = {
    if (shape != other.shape)
      throw new IllegalArgumentException(s"Cannot zip tensors of differing sizes.  Got arguments of shape ${shape.mkString("×")} and ${other.shape.mkString("×")}")

    val newBufRows = math.min(bufferRows, other.bufferRows)
    val newBufCols = math.min(bufferCols, other.bufferCols)
    val newRows = rows + 2 * newBufRows
    val newCols = cols + 2 * newBufCols

    val thisRowOfs = bufferRows - newBufRows
    val thisColOfs = bufferCols - newBufCols
    val otherRowOfs = other.bufferRows - newBufRows
    val otherColOfs = other.bufferCols - newBufCols

    val n = newRows * newCols * bands
    val result = new Float8Vector("array", ArrowTensor.allocator)
    result.allocateNew(n)
    result.setValueCount(n)

    var i = 0
    cfor(0)(_ < bands, _ + 1){ band =>
      cfor(0)(_ < newRows, _ + 1){ r =>
        cfor(0)(_ < newCols, _ + 1){ c =>
          val thisIdx = band * (bufferedRows * bufferedCols) + (r + thisRowOfs) * bufferedCols + c + thisColOfs
          val otherIdx = band * (other.bufferedRows * other.bufferedCols) + (r + otherRowOfs) * other.bufferedCols + c + otherColOfs

          if (tensor.vector.isNull(thisIdx) || other.tensor.vector.isNull(otherIdx))
            result.setNull(i)
          else
            result.set(i, fn(tensor.vector.get(thisIdx), other.tensor.vector.get(otherIdx)))

          i += 1
        }
      }
    }

    BufferedTensor(ArrowTensor(result, Seq(bands, newRows, newCols)), newBufRows, newBufCols, extent)
  }

  def zipBands(other: BufferedTensor)(fn: (Double, Double) => Double): BufferedTensor = {
    if ((cols, rows) != (other.cols, other.rows))
      throw new IllegalArgumentException(s"Cannot zip bands from tensors of differing sizes.  Base is ${shape.mkString("×")}, mask is ${other.shape.mkString("×")}")

    val newBufRows = math.min(bufferRows, other.bufferRows)
    val newBufCols = math.min(bufferCols, other.bufferCols)
    val newRows = rows + 2 * newBufRows
    val newCols = cols + 2 * newBufCols

    val thisRowOfs = bufferRows - newBufRows
    val thisColOfs = bufferCols - newBufCols
    val otherRowOfs = other.bufferRows - newBufRows
    val otherColOfs = other.bufferCols - newBufCols

    val n = newRows * newCols * bands
    val result = new Float8Vector("array", ArrowTensor.allocator)
    result.allocateNew(n)
    result.setValueCount(n)

    var i = 0
    cfor(0)(_ < bands, _ + 1){ band =>
      cfor(0)(_ < newRows, _ + 1){ r =>
        cfor(0)(_ < newCols, _ + 1){ c =>
          val thisIdx = band * (bufferedRows * bufferedCols) + (r + thisRowOfs) * bufferedCols + c + thisColOfs
          val otherIdx = (r + otherRowOfs) * other.bufferedCols + c + otherColOfs

          if (tensor.vector.isNull(thisIdx) || other.tensor.vector.isNull(otherIdx))
            result.setNull(i)
          else
            result.set(i, fn(tensor.vector.get(thisIdx), other.tensor.vector.get(otherIdx)))

          i += 1
        }
      }
    }

    BufferedTensor(ArrowTensor(result, Seq(bands, newRows, newCols)), newBufRows, newBufCols, extent)
  }

  override def toString: String = {
    s"BufferedTensor with $bands × $rows × $cols (bands × rows × cols) with rows buffered by ${bufferRows} and cols buffered by ${bufferCols}"
  }

  def show: Unit = {
    var i = 0
    var accum = s"BufferedTensor with $bands × $rows × $cols (bands × rows × cols)\n"
    val formatting = "% 4.2f"
    cfor(0)(_ < bands, _ + 1){ b =>
      cfor(0)(_ < tensor.rows, _ + 1){ r =>
        cfor(0)(_ < tensor.cols, _ + 1){ c =>
          if (r >= bufferRows && r < rows + bufferRows) {
            if (c >= bufferCols && c < bufferCols + cols)
              accum += "\033[1m"
          }
          accum = accum + s"${formatting.format(tensor.vector.get(i))} "
          if (r >= bufferRows && r < rows + bufferRows) {
            if (c >= bufferCols && c < bufferCols + cols)
              accum += "\033[0m"
          }
          i += 1
        }
        accum += "\n"
      }
      accum = accum + "\n"
    }
    accum += s"with rows buffered by ${bufferRows} and cols buffers by ${bufferCols}"
    println(accum)
  }
}


object BufferedTensor {
  import org.apache.spark.sql.rf.BufferedTensorUDT._
  implicit val arrowTensorEncoder: ExpressionEncoder[BufferedTensor] =
    CatalystSerializerEncoder[BufferedTensor](true)

   def apply(tensor: ArrowTensor, br: Int, bc: Int): BufferedTensor = BufferedTensor(tensor, br, bc, None)
}
