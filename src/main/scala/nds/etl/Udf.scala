package nds.etl

import java.nio.ByteBuffer
import java.security.MessageDigest

import org.apache.spark.sql.functions._


object Udf {

  val uCellId = udf((rncId: Int, cellId: Int) => (rncId << 16) + cellId)

  val callId = udf((imsi: String, srnti: Int) => {
    val digest = MessageDigest.getInstance("MD5").digest((imsi + srnti).getBytes)
    ByteBuffer.wrap(digest).getLong()
  })

  val toEpochTime = udf((epochMs: Long) => { (epochMs / 1000).toInt })
}
