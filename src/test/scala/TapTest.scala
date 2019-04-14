import org.scalatest.{Matchers, WordSpecLike}
import scalaz.zio.{DefaultRuntime, UIO, ZIO}

class PerfectService {
  def request(): ZIO[Any, Nothing, Unit] = UIO(())
}

object ServiceFailure extends Throwable

class EvilService {
  def request(): ZIO[Any, ServiceFailure.type, Unit] = ZIO.fail(ServiceFailure)
}

object RejectByTap extends Throwable

class TapTest extends WordSpecLike with DefaultRuntime with Matchers {

  "Tap" when {
    "service working properly" should {
      "let all requests through" in {

        unsafeRun(for {
          tap <- Tap.make(Percentage(0.01), (_: Throwable) => true, RejectByTap)
          service <- UIO(new PerfectService)
          results <- ZIO.foreach(Iterable.range(1, 100)) {
            _ =>
              tap(service.request()).fold(Left(_), Right(_))
          }
        } yield {
          results.forall(_.isRight)
        }) shouldBe false

      }
    }
  }

  "Tap" when {
    "service is not working" should {
      "reject a request after detecting a failure" in {

        val results = unsafeRun(for {
          tap <- Tap.make(Percentage(2), (_: Throwable) => true, RejectByTap)
          service <- UIO(new EvilService)
          results <- ZIO.foreach(Iterable.range(1, 100)) {
            _ =>
              tap(service.request()).fold(Left(_), Right(_))
          }
        } yield results)

        results.head.left.get shouldBe a[ServiceFailure.type]
        println(results.length)
        results.tail.nonEmpty shouldBe true
        results.tail.forall{
          case Left(RejectByTap) => true
          case _ => false } shouldBe true
      }
    }
  }
}
