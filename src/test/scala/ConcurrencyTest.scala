import java.util.concurrent._

import org.scalatest.{Matchers, WordSpecLike}
import scalaz.zio.blocking._
import scalaz.zio.clock.{Clock, _}
import scalaz.zio.duration.Duration
import scalaz.zio.internal.Executor
import scalaz.zio.{DefaultRuntime, UIO, ZIO}

import scala.concurrent.ExecutionContext

final case class QueryResult()

final case class MockDbAccess(dur: Duration) {
  def query: ZIO[Clock, Nothing, QueryResult] = for {
    _ <- sleep(dur)
  } yield QueryResult()
}

object MockDbAccess {
  val slowDBAccess: UIO[MockDbAccess] = UIO.effectTotal(new MockDbAccess(Duration(2000, TimeUnit.MICROSECONDS)))
}

final case class Response()

final case class MockApiService(dur: Duration) {
  def request: ZIO[Clock, Nothing, Response] = for {
    _ <- sleep(dur)
  } yield Response()
}

object MockApiService {
  val slowApiService: UIO[MockApiService] = UIO.effectTotal(new MockApiService(Duration(1000, TimeUnit.MILLISECONDS)))
  val fastApiService: UIO[MockApiService] = UIO.effectTotal(new MockApiService(Duration(100, TimeUnit.MILLISECONDS)))
}

class ConcurrencyTest extends WordSpecLike with Matchers with DefaultRuntime {

  import MockApiService._
  import MockDbAccess._

  "race" should {
    "return faster result of two effects" in {
      unsafeRun(for {
        slow <- slowApiService
        fast <- fastApiService
        slowOrFast <- slow.request.race(fast.request)
      } yield slowOrFast) shouldBe a[Response]
    }
  }

  "raceEither" should {
    "return the faster result among two effects" in {
      unsafeRun(for {
        slow <- slowDBAccess
        fast <- fastApiService
        slowOrFast <- slow.query.raceEither(fast.request)
      } yield slowOrFast) shouldBe a[Right[_, _]]
    }
  }


  "zip" should {
    "return both results" in {
      unsafeRun(for {
        slow <- slowApiService
        fast <- fastApiService
        both <- slow.request.zip(fast.request)
      } yield both) shouldBe a[(_, _)]
    }
  }

  "zip/zipPar" should {
    "execute effects in sequence/parallel" in {
      val serviceIO = UIO(MockApiService(Duration(100, TimeUnit.MILLISECONDS)))
      val deadline = UIO(()).delay(Duration(120, TimeUnit.MILLISECONDS))

      unsafeRun(
        for {
          service <- serviceIO
          serviceCallTwiceInSequence = service.request.zip(service.request)
          result <- serviceCallTwiceInSequence.raceEither(deadline)
        } yield result
      ) shouldBe a[Right[_, _]] // service call twice in sequence should be executed in about 200 msec.

      unsafeRun(
        for {
          service <- serviceIO
          serviceCallTwiceInParallel = service.request.zipPar(service.request)
          result <- serviceCallTwiceInParallel.raceEither(deadline)
        } yield result
      ) shouldBe a[Left[_, _]] // service call twice in parallel should be executed in about 100 msec.
    }
  }


  "lock" should {
    "lock an effect on specified execution context" in {
      class MyExecutionContext extends ExecutionContext {
        val executor: ExecutorService = Executors.newSingleThreadExecutor()

        override def execute(runnable: Runnable): Unit = executor.submit(runnable)

        override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()
      }

      val thisThreadId = Thread.currentThread().getId

      unsafeRun(
        for {
          ec <- UIO.succeed(new MyExecutionContext)
          executedIn <- UIO.effectTotal({
            Thread.currentThread().getId
          })
            .lock(Executor.fromExecutionContext(Int.MaxValue) {
              ec
            })
        } yield executedIn
      ) should not be thisThreadId
    }
  }

  "blocking" should {
    "execute an effect on the blocking thread pool" in {
      unsafeRun(for {
        slept <- UIO(()).delay(Duration(10, TimeUnit.MILLISECONDS))
          .raceEither(blocking(ZIO.effect(java.lang.Thread.sleep(10000L)))) // java.lang.Thread.sleepはブロッキング
      } yield slept) shouldBe a[Left[_, _]]
    }
  }

  "interruptible" should {
    "make a blocking task interruptible" in {
      unsafeRun(for {
        slept <- UIO(()).delay(Duration(10, TimeUnit.MILLISECONDS))
          .raceEither(interruptible(java.lang.Thread.sleep(10000L)))
      } yield slept) shouldBe a[Left[_, _]]
    }
  }
}
