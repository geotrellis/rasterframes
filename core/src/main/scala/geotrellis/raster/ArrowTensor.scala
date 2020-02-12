package geotrellis.raster

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels

import com.google.flatbuffers.FlatBufferBuilder
import org.apache.arrow.flatbuf.{Buffer, Tensor, TensorDim, Type}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.{Float8Vector, VectorSchemaRoot}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import org.locationtech.rasterframes.encoders.CatalystSerializerEncoder

import spire.syntax.cfor._

import scala.collection.JavaConverters._

case class ArrowTensor(val vector: Float8Vector, val shape: Seq[Int]) extends CellGrid {
  // TODO: Should we be using ArrowBuf here directly, since Arrow Tensor can not have pages?
  lazy val depth = shape(0)
  lazy val rows = shape(1)
  lazy val cols = shape(2)
  val cellType = DoubleCellType

  // TODO: Figure out how to work this crazy thing
  // def copy(implicit alloc: BufferAllocator) = {
  //   val n = vector.getValueCount
  //   val copied = new Float8Vector("array", alloc)
  //   val tp = vector.makeTransferPair(copied)
  //   tp.copyValueSafe(0, n)
  //   copied.copyFromSafe(0, n, vector)
  //   copied.setValueCount(n)
  //   ArrowTensor(copied, shape)
  // }

  def map(fn: Double => Double): ArrowTensor = {
    val n = vector.getValueCount
    val result = new Float8Vector("array", ArrowTensor.allocator)
    result.allocateNew(n)
    result.setValueCount(n)
    cfor(0)(_ < n, _ + 1) { i =>
      if (vector.isSet(i) == 1)
        result.set(i, fn(vector.get(i)))
    }
    ArrowTensor(result, shape)
  }

  def zipWith(other: ArrowTensor)(fn: (Double, Double) => Double): ArrowTensor = {
    if (other.shape != shape)
      throw new IllegalArgumentException(s"Cannot zip tensors of differing sizes.  Got arguments of shape ${shape.mkString("×")} and ${other.shape.mkString("×")}")

    val n = vector.getValueCount
    val result = new Float8Vector("array", ArrowTensor.allocator)
    result.allocateNew(n)
    result.setValueCount(n)
    cfor(0)(_ < n, _ + 1){ i =>
      if (vector.isNull(i) || other.vector.isNull(i))
        result.setNull(i)
      else
        result.set(i, fn(vector.get(i), other.vector.get(i)))
    }
    ArrowTensor(result, shape)
  }

  def zipBands(other: Tile)(fn: (Double, Double) => Double): ArrowTensor = {
    if (other.rows != rows || other.cols != cols)
      throw new IllegalArgumentException(s"Cannot zip; tile and tensor have incompatible dimension: Tensor shape ${shape.mkString("×")}, tile shape 1×${other.rows}×${other.cols}")

    val n = vector.getValueCount
    val result = new Float8Vector("array", ArrowTensor.allocator)
    result.allocateNew(n)
    result.setValueCount(n)

    var i = 0
    cfor(0)(_ < rows, _ + 1){ r =>
      cfor(0)(_ < cols, _ + 1){ c =>
        if (vector.isNull(i) || isNoData(other.getDouble(c, r)))
          result.setNull(i)
        else
          result.set(i, fn(vector.get(i), other.getDouble(c, r)))
        i += 1
      }
    }
    ArrowTensor(result, shape)
  }

  def getPixelVector(col: Int, row: Int): Vector = {
    val pixelStack =
      for (depth <- 0 until shape(0)) yield {
    //    println("handling depth: ", depth)
        val i = depth * rows * cols + row * cols + col
        if (vector.isNull(i))
          Double.NaN
        else
          vector.get(i)
      }
   //   println("pixel stack", pixelStack.toList)

    Vectors.dense(pixelStack.toArray)
  }

  def sliceBands(bands: Seq[Int]): ArrowTensor = {
    assert(bands.forall{ b => b >= 0 && b < shape(0) }, s"Encountered band outside range 0 to ${shape(0)} in $bands")

    val newSize = rows * cols * bands.length
    val result = new Float8Vector("array", ArrowTensor.allocator)
    result.allocateNew(newSize)
    result.setValueCount(newSize)

    var pos = 0
    for ( b <- bands ) {
      cfor(b * rows * cols)(_ < (b + 1) * rows * cols, _ + 1) { i =>
        if (vector.isNull(i))
          result.setNull(pos)
        else
          result.set(pos, vector.get(i))
        pos += 1
      }
    }

    ArrowTensor(result, Seq(bands.length, rows, cols))
  }

  /** Write Tensor to buffer, return offset of Tensor object in that buffer */
  def writeTensor(bufferBuilder: FlatBufferBuilder): Int = {
    val elementSize = 8

    // note the following relies on the data being Double
    val strides: Array[Long] = {
      shape.tails.toSeq.tail.map(_.product.toLong * 8L).toArray
    }

    val shapeOffset: Int = {
      val rank = shape.length
      val tensorDimOffsets = new Array[Int](rank)
      val nameOffset = new Array[Int](rank)

      for (i <- shape.indices) {
        nameOffset(i) = bufferBuilder.createString("")
        tensorDimOffsets(i) = TensorDim.createTensorDim(bufferBuilder, shape(i), nameOffset(i))
      }

      Tensor.createShapeVector(bufferBuilder, tensorDimOffsets)
    }

    val typeOffset = org.apache.arrow.flatbuf.Int.createInt(bufferBuilder, 32,true)

    val stridesOffset = Tensor.createStridesVector(bufferBuilder, strides)
    Tensor.startTensor(bufferBuilder)
    Tensor.addTypeType(bufferBuilder, Type.FloatingPoint)
    // pa.read_tensor also wants type, ND4j does not write this because it I'm guessing its not written to python
    Tensor.addType(bufferBuilder, typeOffset)
    Tensor.addShape(bufferBuilder, shapeOffset)
    Tensor.addStrides(bufferBuilder, stridesOffset)
    // Buffers offset is relative to memory page, not the IPC message.
    val tensorBodyOffset: Int = 0
    val tensorBodySize: Int = vector.getValueCount * 8
    val dataOffset = Buffer.createBuffer(bufferBuilder, tensorBodyOffset, tensorBodySize)
    Tensor.addData(bufferBuilder, dataOffset)
    Tensor.endTensor(bufferBuilder)
  }

  def toIpcMessage(): ByteBuffer = {
    val bufferBuilder = new FlatBufferBuilder(512)
    val tensorOffset = writeTensor(bufferBuilder)
    val tensorBodySize: Int = vector.getValueCount * 8

    MessageSerializer.serializeMessage(bufferBuilder, org.apache.arrow.flatbuf.MessageHeader.Tensor, tensorOffset, tensorBodySize);
  }

  def toArrowBytes(): Array[Byte] = {
    val bb = toIpcMessage()
    val bout = new ByteArrayOutputStream()
    val wbc = new WriteChannel(Channels.newChannel(bout))
    MessageSerializer.writeMessageBuffer(wbc, bb.remaining(), bb)
    // wbc.align
    wbc.write(vector.getDataBuffer)
    wbc.close()
    bout.toByteArray
  }

  def show: Unit = {
    var i = 0
    var accum = ""
    val formatting = "% 1.2f"
    cfor(0)(_ < shape(0), _ + 1){b =>
      cfor(0)(_ < rows, _ + 1){ r =>
        cfor(0)(_ < cols, _ + 1){ c =>
          accum = accum + s"${formatting.format(vector.get(i))} "
          i += 1
        }
        accum = accum + "\n"
      }
      accum = accum + "\n"
    }
    print(accum)
  }

}

object ArrowTensor {
  import org.apache.spark.sql.rf.TensorUDT._
  implicit val arrowTensorEncoder: ExpressionEncoder[ArrowTensor] =
    CatalystSerializerEncoder[ArrowTensor](true)

  val allocator = new RootAllocator(Long.MaxValue)

  val schema: Schema = {
    val fieldArr = new Field(
      "array",
      new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null, null),
      Nil.asJava)

    new Schema(List(fieldArr).asJava)
  }

  def fromArray(arr: Array[Double], shape: Int*): ArrowTensor = {
    val root = VectorSchemaRoot.create(schema, allocator)
    val vec = new Float8Vector("array", allocator)
    vec.allocateNew(arr.length)
    for (i <- arr.indices) vec.set(i, arr(i))
    vec.setValueCount(arr.length)
    root.setRowCount(arr.length)
    new ArrowTensor(vec, shape.toArray)
  }

  def fill(v: Double, shape: Int*): ArrowTensor = {
    val vec = new Float8Vector("array", allocator)
    val shp = shape.toSeq
    val n = shp.product
    vec.allocateNew(n)
    vec.setValueCount(n)
    cfor(0)(_ < n, _ + 1){ i =>
      vec.set(i, v)
    }
    ArrowTensor(vec, shp)
  }

  def fromArrowMessage(bytes: Array[Byte]): ArrowTensor = {
    val is = new ByteArrayInputStream(bytes)
    val channel = Channels.newChannel(is)
    val readChannel = new ReadChannel(channel)
    val msg = MessageSerializer.readMessage(readChannel)
    //println("msg BB: " + msg.getMessageBuffer)

    // TODO: use tensor information to build the right kind of tensor
    val tensor = new Tensor()
    msg.getMessage.header(tensor)

    val root = VectorSchemaRoot.create(schema, allocator)
    val vec = new Float8Vector("array", allocator)
    def shape = {
      for (i <- 0 until tensor.shapeLength()) yield tensor.shape(i).size().toInt
    }.toArray

    val tensorSize = shape.product

    val arrowBuf = MessageSerializer.readMessageBody(readChannel, msg.getMessageBodyLength.toInt, allocator)
    vec.setValueCount(tensorSize)
    // TODO: find a way to reference this buffer directly, this is obviously horrible
    for (i <- 0 until tensorSize) vec.set(i, Float8Vector.get(arrowBuf, i))

    new ArrowTensor(vec, shape)
  }

  def stackTiles(others: Seq[Tile]): ArrowTensor = {
    val rowSet = others.map(_.rows).toSet
    val colSet = others.map(_.cols).toSet
    assert(others.size > 0, "Cannot stack an empty set of tiles!")
    assert(rowSet.size == 1 && colSet.size == 1,
      "All tiles must have equal number of rows and columns!")

    val rows = rowSet.head
    val cols = colSet.head
    val bands = others.length
    val newSize = rows * cols * bands

    val result = new Float8Vector("array", allocator)
    result.allocateNew(newSize)
    result.setValueCount(newSize)

    var pos = 0
    for (other <- others) {
      cfor(0)(_ < other.rows, _ + 1) { r =>
        cfor(0)(_ < other.cols, _ + 1) { c =>
          if (isNoData(other.get(c, r)))
            result.setNull(pos)
          else
            result.set(pos, other.get(c, r))
          pos += 1
        }
      }
    }

    ArrowTensor(result, Seq(bands, rows, cols))
  }

  def stackTensors(others: Seq[ArrowTensor]): ArrowTensor = {
    assert(others.size > 0, "Cannot stack an empty set of tensors!")
    assert(others.map(_.rows).toSet.size == 1 && others.map(_.cols).toSet.size == 1,
           "All tensors must have equal number of rows and columns!")

    val rows = others.head.rows
    val cols = others.head.cols
    val bands = others.map(_.shape(0)).sum
    val newSize = rows * cols * bands

    val result = new Float8Vector("array", allocator)
    result.allocateNew(newSize)
    result.setValueCount(newSize)

    var pos = 0
    for (other <- others) {
      cfor(0)(_ < other.vector.getValueCount, _ + 1) { i =>
        if (other.vector.isNull(i))
          result.setNull(pos)
        else
          result.set(pos, other.vector.get(i))
        pos += 1
      }
    }

    ArrowTensor(result, Seq(bands, rows, cols))
  }
}
