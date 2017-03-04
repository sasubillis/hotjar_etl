package nds.etl

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.security.Security

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.openpgp._


/**
  * NOTE: Bouncycastle version needs to match the version used by Hadoop distribution.
  * See: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/dependency-analysis.html
  *
  * Implementation based on org.bouncycastle.openpgp.examplesKeyBasedFileProcessor
  */
object PgpUtils {

  Security.addProvider(new BouncyCastleProvider())


  def decrypt(inputStream: InputStream, privateKeyStr: String, password: String): InputStream = {
    var lines: Array[AnyRef] = Array()

    var pgpFactory: PGPObjectFactory = new PGPObjectFactory(PGPUtil.getDecoderStream(inputStream))


    // the first object might be a PGP marker packet.
    val encryptedDataObjects = (pgpFactory.nextObject() match {
      case obj: PGPEncryptedDataList => obj.asInstanceOf[PGPEncryptedDataList]
      case _                         => pgpFactory.nextObject().asInstanceOf[PGPEncryptedDataList]
    }).getEncryptedDataObjects


    // find the secret key
    var privateKey: PGPPrivateKey = null
    var encryptedData: PGPPublicKeyEncryptedData = null
    val pgpSec: PGPSecretKeyRingCollection = new PGPSecretKeyRingCollection(
      PGPUtil.getDecoderStream(new ByteArrayInputStream(privateKeyStr.getBytes(StandardCharsets.UTF_8))))

    while (privateKey == null && encryptedDataObjects.hasNext) {
      encryptedData = encryptedDataObjects.next().asInstanceOf[PGPPublicKeyEncryptedData]
      privateKey = findSecretKey(pgpSec, encryptedData.getKeyID, password.toCharArray)
    }

    if (privateKey == null) {
      throw new IllegalArgumentException("Private key for message not found.")
    }

    pgpFactory = new PGPObjectFactory(encryptedData.getDataStream(privateKey, "BC"))
    var message = pgpFactory.nextObject

    if (message.isInstanceOf[PGPCompressedData]) {
      pgpFactory = new PGPObjectFactory(message.asInstanceOf[PGPCompressedData].getDataStream)
      message = pgpFactory.nextObject
    }

    if (message.isInstanceOf[PGPOnePassSignatureList]) {
      // pass: pgpFactory.nextObject.asInstanceOf[PGPOnePassSignatureList].get(0)
      message = pgpFactory.nextObject
    }

    message match {
      case message: PGPLiteralData => {
        message.asInstanceOf[PGPLiteralData].getInputStream
      }
      case _ => throw new PGPException("message is not a simple encrypted file - type unknown.")
    }
  }


  def findSecretKey(pgpSec: PGPSecretKeyRingCollection, keyID: Long, pass: Array[Char]): PGPPrivateKey = {
    val pgpSecKey: PGPSecretKey = pgpSec.getSecretKey(keyID)
    if (pgpSecKey == null) return null

    pgpSecKey.extractPrivateKey(pass, "BC")
  }

}
