from __future__ import print_function
from pyspark.streaming import StreamingContext
from pyspark import SparkContext , SparkConf
from pyspark.sql.snappy import SnappySession
from pyspark.sql.types import StructType , StructField
from py4j.java_gateway import java_import, JavaObject
from pyspark.streaming.util import TransformFunction, TransformFunctionSerializer
from pyspark.serializers import NoOpSerializer, UTF8Deserializer, CloudPickleSerializer
from pyspark.streaming.dstream import DStream
from pyspark.streaming.snappy.snappydstream import  SchemaDStream


class SnappyStreamingContext(StreamingContext):
    """
    Main entry point for Snappy Spark Streaming functionality. A SnappyStreamingContext
    represents the connection to a Snappy cluster, and can be used to create
    L{DStream} various input sources. It can be from an existing L{SparkContext}.
    After creating and transforming DStreams, the streaming computation can
    be started and stopped using `context.start()` and `context.stop()`,
    respectively. `context.awaitTermination()` allows the current thread
    to wait for the termination of the context by `stop()` or by an exception.
    """
    _transformerSerializer = None

    def __init__(self, sparkContext, batchDuration=None, jssc=None):
        """
        Create a new StreamingContext.

        @param sparkContext: L{SparkContext} object.
        @param batchDuration: the time interval (in seconds) at which streaming
                              data will be divided into batches
        """

        self._sc = sparkContext
        self._jvm = self._sc._jvm
        self._jssc = jssc or self._initialize_context(self._sc, batchDuration)
        self._snappySession = SnappySession(sparkContext)
        # self._snappycontext = SnappyContext(sparkContext, snappySession)

    @classmethod
    def _ensure_initialized(cls):
        SparkContext._ensure_initialized()
        gw = SparkContext._gateway

        java_import(gw.jvm, "org.apache.spark.streaming.*")
        java_import(gw.jvm, "org.apache.spark.streaming.api.*")
        java_import(gw.jvm, "org.apache.spark.streaming.api.java.*")
        java_import(gw.jvm, "org.apache.spark.streaming.api.python.*")

        # start callback server
        # getattr will fallback to JVM, so we cannot test by hasattr()
        if "_callback_server" not in gw.__dict__ or gw._callback_server is None:
            gw.callback_server_parameters.eager_load = True
            gw.callback_server_parameters.daemonize = True
            gw.callback_server_parameters.daemonize_connections = True
            gw.callback_server_parameters.port = 0
            gw.start_callback_server(gw.callback_server_parameters)
            cbport = gw._callback_server.server_socket.getsockname()[1]
            gw._callback_server.port = cbport
            # gateway with real port
            gw._python_proxy_port = gw._callback_server.port
            # get the GatewayServer object in JVM by ID
            jgws = JavaObject("GATEWAY_SERVER", gw._gateway_client)
            # update the port of CallbackClient with real port
            jgws.resetCallbackClient(jgws.getCallbackClient().getAddress(), gw._python_proxy_port)

        # register serializer for TransformFunction
        # it happens before creating SparkContext when loading from checkpointing
        cls._transformerSerializer = TransformFunctionSerializer(
            SparkContext._active_spark_context, CloudPickleSerializer(), gw)


    def _initialize_context(self, sc, duration):
        self._ensure_initialized()
        return self._jvm.JavaSnappyStreamingContext(sc._jsc, self._jduration(duration))

    @classmethod
    def getOrCreate(cls, checkpointPath, setupFunc):
        """
        Either recreate a SnappyStreamingContext from checkpoint data or create a new SnappyStreamingContext.
        If checkpoint data exists in the provided `checkpointPath`, then SnappyStreamingContext will be
        recreated from the checkpoint data. If the data does not exist, then the provided setupFunc
        will be used to create a new context.

        @param checkpointPath: Checkpoint directory used in an earlier streaming program
        @param setupFunc:      Function to create a new context and setup DStreams
        """
        cls._ensure_initialized()
        gw = SparkContext._gateway

        # Check whether valid checkpoint information exists in the given path
        ssc_option = gw.jvm.SnappyStreamingContextPythonHelper().tryRecoverFromCheckpoint(checkpointPath)
        if ssc_option.isEmpty():
            ssc = setupFunc()
            ssc.checkpoint(checkpointPath)
            return ssc

        jssc = gw.jvm.JavaSnappyStreamingContext(ssc_option.get())

        # If there is already an active instance of Python SparkContext use it, or create a new one
        if not SparkContext._active_spark_context:
            jsc = jssc.sparkContext()
            conf = SparkConf(_jconf=jsc.getConf())
            SparkContext(conf=conf, gateway=gw, jsc=jsc)

        sc = SparkContext._active_spark_context

        # update ctx in serializer
        cls._transformerSerializer.ctx = sc
        return SnappyStreamingContext(sc, None, jssc)

    @classmethod
    def getActive(cls):
        """
        Return either the currently active SnappyStreamingContext (i.e., if there is a context started
        but not stopped) or None.
        """
        activePythonContext = cls._activeContext
        if activePythonContext is not None:
            # Verify that the current running Java StreamingContext is active and is the same one
            # backing the supposedly active Python context
            activePythonContextJavaId = activePythonContext._jssc.ssc().hashCode()
            activeJvmContextOption = activePythonContext._jvm.SnappyStreamingContext.getActive()

            if activeJvmContextOption.isEmpty():
                cls._activeContext = None
            elif activeJvmContextOption.get().hashCode() != activePythonContextJavaId:
                cls._activeContext = None
                raise Exception("JVM's active JavaStreamingContext is not the JavaStreamingContext "
                                "backing the action Python StreamingContext. This is unexpected.")
        return cls._activeContext

    def start(self):
        """
        Start the execution of the streams.
        """
        self._jssc.start()
        SnappyStreamingContext._activeContext = self


    def sql(self ,  sqlText):
        """Returns a :class:`DataFrame` representing the result of the given query.
        :return: :class:`DataFrame`
        """
        return self._snappySession.sql(sqlText)

    def union(self, *dstreams):
        """
        Create a unified DStream from multiple DStreams of the same
        type and same slide duration.
        """
        if not dstreams:
            raise ValueError("should have at least one DStream to union")
        if len(dstreams) == 1:
            return dstreams[0]
        if len(set(s._jrdd_deserializer for s in dstreams)) > 1:
            raise ValueError("All DStreams should have same serializer")
        if len(set(s._slideDuration for s in dstreams)) > 1:
            raise ValueError("All DStreams should have same slide duration")
        first = dstreams[0]
        jrest = [d._jdstream for d in dstreams[1:]]
        return DStream(self._jssc.union(first._jdstream, jrest), self, first._jrdd_deserializer)

    def createSchemaDStream(self, dstream , schema):
        """
        Creates a [[SchemaDStream]] from an DStream of Product"
        """
        if not isinstance(schema, StructType):
            raise TypeError("schema should be StructType, but got %s" % type(schema))
        if not isinstance(dstream, DStream):
            raise TypeError("dstream should be DStream, but got %s" % type(dstream))
        return SchemaDStream(dstream._jdstream, self, dstream._jrdd_deserializer, schema)