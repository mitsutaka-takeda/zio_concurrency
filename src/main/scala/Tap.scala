import scalaz.zio.{Ref, UIO, ZIO}


final case class Stats(totalNumberOfTry: Long, numberOfErrors: Long) {
  def success: Stats = Stats(totalNumberOfTry + 1, numberOfErrors)

  def reject = Stats(totalNumberOfTry + 1, numberOfErrors)

  def fail: Stats = Stats(totalNumberOfTry + 1,numberOfErrors + 1)

  def errorRate: Percentage = if(totalNumberOfTry != 0) Percentage(numberOfErrors / (1.0*totalNumberOfTry)) else Percentage(0)
}

object Stats {
  val empty: Stats = Stats(0, 0)
}

final case class Percentage(value: Double) extends AnyVal

object Percentage {
  val ordering: Ordering[Percentage] = Ordering.by((p: Percentage) => p.value)
}


/**
  * A `Tap` adjusts the flow of tasks through
  * an external service in response to observed
  * failures in the service, always trying to
  * maximize flow while attempting to meet the
  * user-defined upper bound on failures.
  */
trait Tap[-E1, +E2] {
  /**
    * Sends the task through the tap. The
    * returned task may fail immediately with a
    * default error depending on the service
    * being guarded by the tap.
    */
  def apply[R, E >: E2 <: E1, A](
                                  effect: ZIO[R, E, A]): ZIO[R, E, A]
}

object Tap {

  /**
    * Creates a tap that aims for the specified
    * maximum error rate, using the specified
    * function to qualify errors (unqualified
    * errors are not treated as failures for
    * purposes of the tap), and the specified
    * default error used for rejecting tasks
    * submitted to the tap.
    */
  def make[E1, E2](
                    errBound  : Percentage,
                    qualified : E1 => Boolean,
                    rejected  : => E2): UIO[Tap[E1, E2]] =
    for {
      ref<- Ref.make(Stats.empty)
      t <- UIO(
        new Tap[E1, E2] {
          override def apply[R, E >: E2 <: E1, A](effect: ZIO[R, E, A]): ZIO[R, E, A] = for {
            eR <- ref.get.map(_.errorRate)
            result <- if(Percentage.ordering.gt(eR, errBound)) {
              ref.update(_.reject).flatMap(_ => ZIO.fail(rejected))
            } else {
              effect.tapBoth(
                e => if (qualified(e)) ref.update(_.fail) else UIO(ref),
                _ => ref.update(_.success)
              )
            }
          } yield result
        })
    } yield t
}
