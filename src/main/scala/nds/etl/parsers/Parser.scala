package nds.etl.parsers

import java.io.InputStream


trait Parser[T] {
  def parse(inputStream: InputStream): Iterator[T]
}
