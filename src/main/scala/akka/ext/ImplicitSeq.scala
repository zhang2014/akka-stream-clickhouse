package akka.ext

case class ImplicitSeq[T](seq: Seq[T])
{
  def getOrElse(index: Int, default: => T): T = seq.isDefinedAt(index) match {
    case true => seq(index)
    case false => default
  }
}
