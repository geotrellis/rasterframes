package org.locationtech.rasterframes.encoders

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.DeserializerBuildHelper.{addToPath, deserializerForWithNullSafetyAndUpcast, expressionWithNullSafety}
import org.apache.spark.sql.catalyst.ScalaReflection.{Schema, cleanUpReflectionObjects, dataTypeFor, encodeFieldNameToIdentifier, getClassFromType, getClassNameFromType, localTypeOf, schemaFor}
import org.apache.spark.sql.catalyst.SerializerBuildHelper.{createSerializerForBoolean, createSerializerForByte, createSerializerForDouble, createSerializerForFloat, createSerializerForInteger, createSerializerForJavaBigDecimal, createSerializerForJavaBigInteger, createSerializerForJavaInstant, createSerializerForJavaLocalDate, createSerializerForLong, createSerializerForObject, createSerializerForScalaBigDecimal, createSerializerForScalaBigInt, createSerializerForShort, createSerializerForSqlDate, createSerializerForSqlTimestamp, createSerializerForString}
import org.apache.spark.sql.catalyst.{WalkedTypePath, expressions}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, IsNull, KnownNotNull}
import org.apache.spark.sql.types.{DataType, ObjectType}
import org.locationtech.rasterframes.encoders.Person.{baseType, deserializerFor, deserializerForPerson, isSubtype, serializerFor, serializerForPerson}

import javax.lang.model.SourceVersion
import org.apache.spark.sql.catalyst.ScalaReflection.universe.{AnnotatedType, Type, typeOf, typeTag}
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.locationtech.rasterframes.encoders.ImplicitEncoder.FieldSerializer

import scala.reflect.{ClassTag, classTag}

trait ImplicitEncoder[T] {
  import ImplicitEncoder._
  def dataType: DataType
  def nullable: Boolean
  def schema: Schema = Schema(dataType, nullable)

  def fields : Seq[FieldSerializer]

  // this could be NewInstance of StaticInvoke
  def constructor(arguments: Seq[Expression]): Expression
}

object ImplicitEncoder {
  def apply[T](implicit ev: ImplicitEncoder[T]): ImplicitEncoder[T] = ev

  /** Info needed to create serializer at some point in the expression tree */
  case class FieldSerializer(
    fieldName: String,
    fieldType: Type,
    dataType: DataType,
    sqlType: DataType,
    makeSerializer: Expression => Expression,
    makeDeserializer: (Expression, WalkedTypePath)=> Expression,
    nullable: Boolean = false
  ) {
    require(!SourceVersion.isKeyword(fieldName) && SourceVersion.isIdentifier(encodeFieldNameToIdentifier(fieldName)),
      s"`$fieldName` is not a valid identifier of Java and cannot be used as field name\n" )
  }

  import scala.reflect.runtime.universe._
  def createSerializerForColumn[T: ImplicitEncoder: ClassTag: TypeTag]: Expression = {
    val tpe = typeOf[T]
    val clsName = getClassNameFromType(tpe)
    val walkedTypePath = new WalkedTypePath().recordRoot(clsName)

    // The input object to `ExpressionEncoder` is located at first column of an row.
    val isPrimitive = tpe.typeSymbol.asClass.isPrimitive
    val inputObject = BoundReference(0, dataTypeFor(typeTag[T]), nullable = !isPrimitive)

    createSerializerFor[T](inputObject)
  }

  def createSerializerFor[T: ImplicitEncoder: ClassTag](inputObject: Expression): Expression = {
    val fields = ImplicitEncoder[T].fields.map { field =>
      val fieldValue = Invoke(KnownNotNull(inputObject), field.fieldName, field.dataType,
        returnNullable = !field.fieldType.typeSymbol.asClass.isPrimitive)
      val clsName = getClassNameFromType(field.fieldType)
      (field.fieldName, field.makeSerializer(fieldValue))
    }
    createSerializerForObject(inputObject, fields)
  }

  def createDeserializerForColumn[T: ImplicitEncoder: ClassTag: TypeTag]: Expression = cleanUpReflectionObjects {
    val tpe = typeOf[Person]
    val clsName = getClassNameFromType(tpe)
    val walkedTypePath = new WalkedTypePath().recordRoot(clsName)
    val Schema(dataType, nullable) = ImplicitEncoder[T].schema
    // Assumes we are deserializing the first column of a row.
    deserializerForWithNullSafetyAndUpcast(GetColumnByOrdinal(0, dataType), dataType,
      nullable = nullable, walkedTypePath,
      (casted, typePath) => createDeserializerFor(casted, typePath))
  }

  def createDeserializerFor[T](
    path: Expression,
    walkedTypePath: WalkedTypePath
  )(implicit
    ev: ImplicitEncoder[T],
    cls: ClassTag[T]
  ): Expression = cleanUpReflectionObjects {
    val arguments: Seq[Expression] = ev.fields.map { field =>
      val clsName = getClassNameFromType(field.fieldType)
      val newTypePath = walkedTypePath.recordField(clsName, field.fieldName)
      val newPath = field.makeDeserializer(
        addToPath(path, field.fieldName, field.sqlType, newTypePath),
        newTypePath)

      expressionWithNullSafety(
        newPath,
        nullable = ev.nullable,
        newTypePath)
    }


    expressions.If(
      IsNull(path),
      expressions.Literal.create(null, ObjectType(classTag[T].runtimeClass)),
      ImplicitEncoder[T].constructor(arguments)
    )
  }


  private def serializerForPrimitive(inputObject: Expression, tpe: `Type`): Expression = cleanUpReflectionObjects {
    baseType(tpe) match {
      case _ if !inputObject.dataType.isInstanceOf[ObjectType] =>
        inputObject
      case t if isSubtype(t, localTypeOf[String]) =>
        createSerializerForString(inputObject)
      case t if isSubtype(t, localTypeOf[java.time.Instant]) =>
        createSerializerForJavaInstant(inputObject)
      case t if isSubtype(t, localTypeOf[java.sql.Timestamp]) =>
        createSerializerForSqlTimestamp(inputObject)
      case t if isSubtype(t, localTypeOf[java.time.LocalDate]) =>
        createSerializerForJavaLocalDate(inputObject)
      case t if isSubtype(t, localTypeOf[java.sql.Date]) =>
        createSerializerForSqlDate(inputObject)
      case t if isSubtype(t, localTypeOf[BigDecimal]) =>
        createSerializerForScalaBigDecimal(inputObject)
      case t if isSubtype(t, localTypeOf[java.math.BigDecimal]) =>
        createSerializerForJavaBigDecimal(inputObject)
      case t if isSubtype(t, localTypeOf[java.math.BigInteger]) =>
        createSerializerForJavaBigInteger(inputObject)
      case t if isSubtype(t, localTypeOf[scala.math.BigInt]) =>
        createSerializerForScalaBigInt(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Integer]) =>
        createSerializerForInteger(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Long]) =>
        createSerializerForLong(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Double]) =>
        createSerializerForDouble(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Float]) =>
        createSerializerForFloat(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Short]) =>
        createSerializerForShort(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Byte]) =>
        createSerializerForByte(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Boolean]) =>
        createSerializerForBoolean(inputObject)
    }
  }


  private def baseType(tpe: `Type`): `Type` = {
    tpe.dealias match {
      case annotatedType: AnnotatedType => annotatedType.underlying
      case other => other
    }
  }

  object ScalaSubtypeLock
  private def isSubtype(tpe1: `Type`, tpe2: `Type`): Boolean = {
    ScalaSubtypeLock.synchronized {
      tpe1 <:< tpe2
    }
  }
}
