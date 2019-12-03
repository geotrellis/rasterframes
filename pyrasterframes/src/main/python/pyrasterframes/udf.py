import sys
import functools
import inspect

if sys.version < "3":
    from itertools import imap as map

if sys.version >= '3':
    basestring = str

from pyspark import SparkContext
from pyspark.rdd import PythonEvalType
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.types import ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType, UserDefinedType, _parse_datatype_string
from pyspark.sql.udf import _wrap_function


def _get_argspec(f):
    """
    Get argspec of a function. Supports both Python 2 and Python 3.
    """
    if sys.version_info[0] < 3:
        argspec = inspect.getargspec(f)
    else:
        # `getargspec` is deprecated since python3.0 (incompatible with function annotations).
        # See SPARK-23569.
        argspec = inspect.getfullargspec(f)
    return argspec


def require_minimum_pyarrow_version():
    """ Raise ImportError if minimum version of pyarrow is not installed
    """
    # TODO(HyukjinKwon): Relocate and deduplicate the version specification.
    minimum_pyarrow_version = "0.15.0"

    from distutils.version import LooseVersion
    try:
        import pyarrow
        have_arrow = True
    except ImportError:
        have_arrow = False
    if not have_arrow:
        raise ImportError("PyArrow >= %s must be installed; however, "
                          "it was not found." % minimum_pyarrow_version)
    if LooseVersion(pyarrow.__version__) < LooseVersion(minimum_pyarrow_version):
        raise ImportError("PyArrow >= %s must be installed; however, "
                          "your version was %s." % (minimum_pyarrow_version, pyarrow.__version__))


def _create_udf(f, returnType, evalType):

    if evalType in (PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                    PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
                    PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF):

        require_minimum_pyarrow_version()

        argspec = _get_argspec(f)

        if evalType == PythonEvalType.SQL_SCALAR_PANDAS_UDF and len(argspec.args) == 0 and \
                argspec.varargs is None:
            raise ValueError(
                "Invalid function: 0-arg pandas_udfs are not supported. "
                "Instead, create a 1-arg pandas_udf and ignore the arg in your function."
            )

        if evalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF \
                and len(argspec.args) not in (1, 2):
            raise ValueError(
                "Invalid function: pandas_udfs with function type GROUPED_MAP "
                "must take either one argument (data) or two arguments (key, data).")

    # Set the name of the UserDefinedFunction object to be the name of function f
    udf_obj = UserDefinedRasterFunction(
        f, returnType=returnType, name=None, evalType=evalType, deterministic=True)
    return udf_obj._wrapped()


def to_arrow_type(dt):
    """ Convert Spark data type to pyarrow type
    """
    from distutils.version import LooseVersion
    import pyarrow as pa
    if type(dt) == BooleanType:
        arrow_type = pa.bool_()
    elif type(dt) == ByteType:
        arrow_type = pa.int8()
    elif type(dt) == ShortType:
        arrow_type = pa.int16()
    elif type(dt) == IntegerType:
        arrow_type = pa.int32()
    elif type(dt) == LongType:
        arrow_type = pa.int64()
    elif type(dt) == FloatType:
        arrow_type = pa.float32()
    elif type(dt) == DoubleType:
        arrow_type = pa.float64()
    elif type(dt) == DecimalType:
        arrow_type = pa.decimal128(dt.precision, dt.scale)
    elif type(dt) == StringType:
        arrow_type = pa.string()
    elif type(dt) == BinaryType:
        # TODO: remove version check once minimum pyarrow version is 0.10.0
        if LooseVersion(pa.__version__) < LooseVersion("0.10.0"):
            raise TypeError("Unsupported type in conversion to Arrow: " + str(dt) +
                            "\nPlease install pyarrow >= 0.10.0 for BinaryType support.")
        arrow_type = pa.binary()
    elif type(dt) == DateType:
        arrow_type = pa.date32()
    elif type(dt) == TimestampType:
        # Timestamps should be in UTC, JVM Arrow timestamps require a timezone to be read
        arrow_type = pa.timestamp('us', tz='UTC')
    elif type(dt) == ArrayType:
        if type(dt.elementType) == TimestampType:
            raise TypeError("Unsupported type in conversion to Arrow: " + str(dt))
        arrow_type = pa.list_(to_arrow_type(dt.elementType))
    else:
        raise TypeError("Unsupported type in conversion to Arrow: " + str(dt))
    return arrow_type


def to_arrow_schema(schema):
    """ Convert a schema from Spark to Arrow
    """
    import pyarrow as pa
    fields = [pa.field(field.name, to_arrow_type(field.dataType), nullable=field.nullable)
              for field in schema]
    return pa.schema(fields)


class UserDefinedRasterFunction(object):
    """
    User defined function in Python

    .. versionadded:: 1.3
    """
    def __init__(self, func,
                 returnType=StringType(),
                 name=None,
                 evalType=PythonEvalType.SQL_BATCHED_UDF,
                 deterministic=True):
        if not callable(func):
            raise TypeError(
                "Invalid function: not a function or callable (__call__ is not defined): "
                "{0}".format(type(func)))

        if not isinstance(returnType, (DataType, str)):
            raise TypeError(
                "Invalid returnType: returnType should be DataType or str "
                "but is {}".format(returnType))

        if not isinstance(evalType, int):
            raise TypeError(
                "Invalid evalType: evalType should be an int but is {}".format(evalType))

        self.func = func
        self._returnType = returnType
        # Stores UserDefinedPythonFunctions jobj, once initialized
        self._returnType_placeholder = None
        self._judf_placeholder = None
        self._name = name or (
            func.__name__ if hasattr(func, '__name__')
            else func.__class__.__name__)
        self.evalType = evalType
        self.deterministic = deterministic

    @property
    def returnType(self):
        # This makes sure this is called after SparkContext is initialized.
        # ``_parse_datatype_string`` accesses to JVM for parsing a DDL formatted string.
        if self._returnType_placeholder is None:
            if isinstance(self._returnType, DataType):
                self._returnType_placeholder = self._returnType
            else:
                self._returnType_placeholder = _parse_datatype_string(self._returnType)

        if self.evalType == PythonEvalType.SQL_SCALAR_PANDAS_UDF:
            try:
                to_arrow_type(self._returnType_placeholder)
            except TypeError:
                raise NotImplementedError(
                    "Invalid returnType with scalar Pandas UDFs: %s is "
                    "not supported" % str(self._returnType_placeholder))
        elif self.evalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF:
            if isinstance(self._returnType_placeholder, StructType):
                try:
                    to_arrow_schema(self._returnType_placeholder)
                except TypeError:
                    raise NotImplementedError(
                        "Invalid returnType with grouped map Pandas UDFs: "
                        "%s is not supported" % str(self._returnType_placeholder))
            else:
                raise TypeError("Invalid returnType for grouped map Pandas "
                                "UDFs: returnType must be a StructType.")
        elif self.evalType == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF:
            try:
                to_arrow_type(self._returnType_placeholder)
            except TypeError:
                raise NotImplementedError(
                    "Invalid returnType with grouped aggregate Pandas UDFs: "
                    "%s is not supported" % str(self._returnType_placeholder))

        return self._returnType_placeholder

    @property
    def _judf(self):
        # It is possible that concurrent access, to newly created UDF,
        # will initialize multiple UserDefinedPythonFunctions.
        # This is unlikely, doesn't affect correctness,
        # and should have a minimal performance impact.
        if self._judf_placeholder is None:
            self._judf_placeholder = self._create_judf()
        return self._judf_placeholder

    def _create_judf(self):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext

        wrapped_func = _wrap_function(sc, self.func, self.returnType)
        jdt = spark._jsparkSession.parseDataType(self.returnType.json())
        judf = sc._jvm.org.apache.spark.sql.execution.python.UserDefinedPythonFunction(
            self._name, wrapped_func, jdt, self.evalType, self.deterministic)
        return judf

    def __call__(self, *cols):
        judf = self._judf
        sc = SparkContext._active_spark_context
        return Column(judf.apply(_to_seq(sc, cols, _to_java_column)))

    # This function is for improving the online help system in the interactive interpreter.
    # For example, the built-in help / pydoc.help. It wraps the UDF with the docstring and
    # argument annotation. (See: SPARK-19161)
    def _wrapped(self):
        """
        Wrap this udf with a function and attach docstring from func
        """

        # It is possible for a callable instance without __name__ attribute or/and
        # __module__ attribute to be wrapped here. For example, functools.partial. In this case,
        # we should avoid wrapping the attributes from the wrapped function to the wrapper
        # function. So, we take out these attribute names from the default names to set and
        # then manually assign it after being wrapped.
        assignments = tuple(
            a for a in functools.WRAPPER_ASSIGNMENTS if a != '__name__' and a != '__module__')

        @functools.wraps(self.func, assigned=assignments)
        def wrapper(*args):
            return self(*args)

        wrapper.__name__ = self._name
        wrapper.__module__ = (self.func.__module__ if hasattr(self.func, '__module__')
                              else self.func.__class__.__module__)

        wrapper.func = self.func
        wrapper.returnType = self.returnType
        wrapper.evalType = self.evalType
        wrapper.deterministic = self.deterministic
        wrapper.asNondeterministic = functools.wraps(
            self.asNondeterministic)(lambda: self.asNondeterministic()._wrapped())
        return wrapper

    def asNondeterministic(self):
        """
        Updates UserDefinedFunction to nondeterministic.

        .. versionadded:: 2.3
        """
        # Here, we explicitly clean the cache to create a JVM UDF instance
        # with 'deterministic' updated. See SPARK-23233.
        self._judf_placeholder = None
        self.deterministic = False
        return self


def raster_udf(f=None, returnType=None, functionType=None):
    """
    Creates a vectorized user defined function (UDF).

    :param f: user-defined function. A python function if used as a standalone function
    :param returnType: the return type of the user-defined function. The value can be either a
        :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
    :param functionType: an enum value in :class:`pyspark.sql.functions.PandasUDFType`.
                         Default: SCALAR.

    .. note:: Experimental

    The function type of the UDF can be one of the following:

    1. SCALAR

       A scalar UDF defines a transformation: One or more `pandas.Series` -> A `pandas.Series`.
       The length of the returned `pandas.Series` must be of the same as the input `pandas.Series`.

       :class:`MapType`, :class:`StructType` are currently not supported as output types.

       Scalar UDFs are used with :meth:`pyspark.sql.DataFrame.withColumn` and
       :meth:`pyspark.sql.DataFrame.select`.

       >>> from pyspark.sql.functions import raster_udf, PandasUDFType
       >>> from pyspark.sql.types import IntegerType, StringType
       >>> slen = raster_udf(lambda s: s.str.len(), IntegerType())  # doctest: +SKIP
       >>> @raster_udf(StringType())  # doctest: +SKIP
       ... def to_upper(s):
       ...     return s.str.upper()
       ...
       >>> @raster_udf("integer", PandasUDFType.SCALAR)  # doctest: +SKIP
       ... def add_one(x):
       ...     return x + 1
       ...
       >>> df = spark.createDataFrame([(1, "John Doe", 21)],
       ...                            ("id", "name", "age"))  # doctest: +SKIP
       >>> df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age")) \\
       ...     .show()  # doctest: +SKIP
       +----------+--------------+------------+
       |slen(name)|to_upper(name)|add_one(age)|
       +----------+--------------+------------+
       |         8|      JOHN DOE|          22|
       +----------+--------------+------------+

       .. note:: The length of `pandas.Series` within a scalar UDF is not that of the whole input
           column, but is the length of an internal batch used for each call to the function.
           Therefore, this can be used, for example, to ensure the length of each returned
           `pandas.Series`, and can not be used as the column length.

    2. GROUPED_MAP

       A grouped map UDF defines transformation: A `pandas.DataFrame` -> A `pandas.DataFrame`
       The returnType should be a :class:`StructType` describing the schema of the returned
       `pandas.DataFrame`. The column labels of the returned `pandas.DataFrame` must either match
       the field names in the defined returnType schema if specified as strings, or match the
       field data types by position if not strings, e.g. integer indices.
       The length of the returned `pandas.DataFrame` can be arbitrary.

       Grouped map UDFs are used with :meth:`pyspark.sql.GroupedData.apply`.

       >>> from pyspark.sql.functions import raster_udf, PandasUDFType
       >>> df = spark.createDataFrame(
       ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
       ...     ("id", "v"))  # doctest: +SKIP
       >>> @raster_udf("id long, v double", PandasUDFType.GROUPED_MAP)  # doctest: +SKIP
       ... def normalize(pdf):
       ...     v = pdf.v
       ...     return pdf.assign(v=(v - v.mean()) / v.std())
       >>> df.groupby("id").apply(normalize).show()  # doctest: +SKIP
       +---+-------------------+
       | id|                  v|
       +---+-------------------+
       |  1|-0.7071067811865475|
       |  1| 0.7071067811865475|
       |  2|-0.8320502943378437|
       |  2|-0.2773500981126146|
       |  2| 1.1094003924504583|
       +---+-------------------+

       Alternatively, the user can define a function that takes two arguments.
       In this case, the grouping key(s) will be passed as the first argument and the data will
       be passed as the second argument. The grouping key(s) will be passed as a tuple of numpy
       data types, e.g., `numpy.int32` and `numpy.float64`. The data will still be passed in
       as a `pandas.DataFrame` containing all columns from the original Spark DataFrame.
       This is useful when the user does not want to hardcode grouping key(s) in the function.

       >>> import pandas as pd  # doctest: +SKIP
       >>> from pyspark.sql.functions import raster_udf, PandasUDFType
       >>> df = spark.createDataFrame(
       ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
       ...     ("id", "v"))  # doctest: +SKIP
       >>> @raster_udf("id long, v double", PandasUDFType.GROUPED_MAP)  # doctest: +SKIP
       ... def mean_udf(key, pdf):
       ...     # key is a tuple of one numpy.int64, which is the value
       ...     # of 'id' for the current group
       ...     return pd.DataFrame([key + (pdf.v.mean(),)])
       >>> df.groupby('id').apply(mean_udf).show()  # doctest: +SKIP
       +---+---+
       | id|  v|
       +---+---+
       |  1|1.5|
       |  2|6.0|
       +---+---+
       >>> @raster_udf(
       ...    "id long, `ceil(v / 2)` long, v double",
       ...    PandasUDFType.GROUPED_MAP)  # doctest: +SKIP
       >>> def sum_udf(key, pdf):
       ...     # key is a tuple of two numpy.int64s, which is the values
       ...     # of 'id' and 'ceil(df.v / 2)' for the current group
       ...     return pd.DataFrame([key + (pdf.v.sum(),)])
       >>> df.groupby(df.id, ceil(df.v / 2)).apply(sum_udf).show()  # doctest: +SKIP
       +---+-----------+----+
       | id|ceil(v / 2)|   v|
       +---+-----------+----+
       |  2|          5|10.0|
       |  1|          1| 3.0|
       |  2|          3| 5.0|
       |  2|          2| 3.0|
       +---+-----------+----+

       .. note:: If returning a new `pandas.DataFrame` constructed with a dictionary, it is
           recommended to explicitly index the columns by name to ensure the positions are correct,
           or alternatively use an `OrderedDict`.
           For example, `pd.DataFrame({'id': ids, 'a': data}, columns=['id', 'a'])` or
           `pd.DataFrame(OrderedDict([('id', ids), ('a', data)]))`.

       .. seealso:: :meth:`pyspark.sql.GroupedData.apply`

    3. GROUPED_AGG

       A grouped aggregate UDF defines a transformation: One or more `pandas.Series` -> A scalar
       The `returnType` should be a primitive data type, e.g., :class:`DoubleType`.
       The returned scalar can be either a python primitive type, e.g., `int` or `float`
       or a numpy data type, e.g., `numpy.int64` or `numpy.float64`.

       :class:`MapType` and :class:`StructType` are currently not supported as output types.

       Group aggregate UDFs are used with :meth:`pyspark.sql.GroupedData.agg` and
       :class:`pyspark.sql.Window`

       This example shows using grouped aggregated UDFs with groupby:

       >>> from pyspark.sql.functions import raster_udf, PandasUDFType
       >>> df = spark.createDataFrame(
       ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
       ...     ("id", "v"))
       >>> @raster_udf("double", PandasUDFType.GROUPED_AGG)  # doctest: +SKIP
       ... def mean_udf(v):
       ...     return v.mean()
       >>> df.groupby("id").agg(mean_udf(df['v'])).show()  # doctest: +SKIP
       +---+-----------+
       | id|mean_udf(v)|
       +---+-----------+
       |  1|        1.5|
       |  2|        6.0|
       +---+-----------+

       This example shows using grouped aggregated UDFs as window functions. Note that only
       unbounded window frame is supported at the moment:

       >>> from pyspark.sql.functions import raster_udf, PandasUDFType
       >>> from pyspark.sql import Window
       >>> df = spark.createDataFrame(
       ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
       ...     ("id", "v"))
       >>> @raster_udf("double", PandasUDFType.GROUPED_AGG)  # doctest: +SKIP
       ... def mean_udf(v):
       ...     return v.mean()
       >>> w = Window \\
       ...     .partitionBy('id') \\
       ...     .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
       >>> df.withColumn('mean_v', mean_udf(df['v']).over(w)).show()  # doctest: +SKIP
       +---+----+------+
       | id|   v|mean_v|
       +---+----+------+
       |  1| 1.0|   1.5|
       |  1| 2.0|   1.5|
       |  2| 3.0|   6.0|
       |  2| 5.0|   6.0|
       |  2|10.0|   6.0|
       +---+----+------+

       .. seealso:: :meth:`pyspark.sql.GroupedData.agg` and :class:`pyspark.sql.Window`

    .. note:: The user-defined functions are considered deterministic by default. Due to
        optimization, duplicate invocations may be eliminated or the function may even be invoked
        more times than it is present in the query. If your function is not deterministic, call
        `asNondeterministic` on the user defined function. E.g.:

    >>> @raster_udf('double', PandasUDFType.SCALAR)  # doctest: +SKIP
    ... def random(v):
    ...     import numpy as np
    ...     import pandas as pd
    ...     return pd.Series(np.random.randn(len(v))
    >>> random = random.asNondeterministic()  # doctest: +SKIP

    .. note:: The user-defined functions do not support conditional expressions or short circuiting
        in boolean expressions and it ends up with being executed all internally. If the functions
        can fail on special rows, the workaround is to incorporate the condition into the functions.

    .. note:: The user-defined functions do not take keyword arguments on the calling side.
    """
    # decorator @raster_udf(returnType, functionType)
    is_decorator = f is None or isinstance(f, (str, DataType))

    if is_decorator:
        # If DataType has been passed as a positional argument
        # for decorator use it as a returnType
        return_type = f or returnType

        if functionType is not None:
            # @raster_udf(dataType, functionType=functionType)
            # @raster_udf(returnType=dataType, functionType=functionType)
            eval_type = functionType
        elif returnType is not None and isinstance(returnType, int):
            # @raster_udf(dataType, functionType)
            eval_type = returnType
        else:
            # @raster_udf(dataType) or @raster_udf(returnType=dataType)
            eval_type = PythonEvalType.SQL_SCALAR_PANDAS_UDF
    else:
        return_type = returnType

        if functionType is not None:
            eval_type = functionType
        else:
            eval_type = PythonEvalType.SQL_SCALAR_PANDAS_UDF

    if return_type is None:
        raise ValueError("Invalid returnType: returnType can not be None")

    if eval_type not in [PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                         PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
                         PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF]:
        raise ValueError("Invalid functionType: "
                         "functionType must be one the values from PandasUDFType")

    if is_decorator:
        return functools.partial(_create_udf, returnType=return_type, evalType=eval_type)
    else:
        return _create_udf(f=f, returnType=return_type, evalType=eval_type)
