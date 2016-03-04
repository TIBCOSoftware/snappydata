// scalastyle:ignore
package io.snappydata.aggr

import com.miguno.kafka.avro.{AvroDecoder, AvroEncoder}
import kafka.utils.VerifiableProperties

class ImpressionLogAvroEncoder(props: VerifiableProperties = null)
    extends AvroEncoder[ImpressionLog](props, ImpressionLog.getClassSchema)

class ImpressionLogAvroDecoder(props: VerifiableProperties = null)
    extends AvroDecoder[ImpressionLog](props, ImpressionLog.getClassSchema)

