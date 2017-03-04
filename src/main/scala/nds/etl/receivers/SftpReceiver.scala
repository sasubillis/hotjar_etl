package nds.etl.receivers

/*
import java.io.InputStream
import nds.etl.parsers.TrafficaRncEvent

import collection.JavaConverters._

import com.jcraft.jsch.{Channel, Session, ChannelSftp, JSch}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


class SftpReceiver[T](
  user: String,
  password: String,
  host: String,
  operator: String,
  dataSource: String,
  parser: InputStream => Iterator[T],
  storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER_2) extends Receiver[T](storageLevel) {


  object SftpFileStatus extends Enumeration {
    val Invalid, NoPermission, New, InProcessing, Processed = Value
  }


  case class SftpFileEntry(
    sftpUser: String,
    sftpHost: String,
    operator: String,
    dataSource: String,
    filePath: String,
    size: Long,
    mtime: Int,
    status: SftpFileStatus.Value,
    rows: Int
  )


  def onStart() {
    //logError(s"STARTING SFTP RECEIVER: ${Thread.currentThread().getId}")
    new Thread("SFTP Receiver") {
      override def run() { receive() }
    }.start()
  }


  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }


  private def receive() {
    var session: Session = null
    var channel: Channel = null
    JSch.setConfig("StrictHostKeyChecking", "no")

    try {
      val jsch = new JSch
      session = jsch.getSession(user, host)
      session.setPassword(password)
      session.connect(10000)

      channel = session.openChannel("sftp")
      channel.connect()

      val sftpChannel = channel.asInstanceOf[ChannelSftp]

      while (!isStopped) {
        val files = sftpChannel.ls(s"./$dataSource")
          .asInstanceOf[java.util.Vector[ChannelSftp#LsEntry]]
          .asScala
          .map(s =>
            SftpFileEntry(
              host,
              user,
              operator,
              dataSource,
              s"$dataSource/${s.getFilename}",
              s.getAttrs.getSize,
              s.getAttrs.getMTime,
              SftpFileStatus.New,
              0
            )
          )
          .filterNot(f => f.filePath.endsWith(".") || f.filePath.endsWith(".."))

        files.foreach(f => {
          // logError(s"PROCESSING FILE: ${f.filePath}")


          if (f.filePath.endsWith(".csv.gz")) {
            parser(sftpChannel.get(f.filePath))
              .grouped(10000)
              .map(_.iterator)
              .foreach(store)
          }
        })

        stop("Ready with the files")
        Thread.sleep(10000)
      }

    } catch {
      case t: Throwable =>
        channel.disconnect
        session.disconnect
        restart("RESTARTING SFTP RECEIVER")
    } finally {
      // logError("STOPPING SFTP RECEIVER")
      channel.disconnect
      session.disconnect
    }
  }
}
*/
