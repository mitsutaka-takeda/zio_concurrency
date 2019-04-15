import java.util.concurrent._

import org.scalatest.{Matchers, WordSpecLike}
import scalaz.zio.clock.{Clock, _}
import scalaz.zio.duration.Duration
import scalaz.zio.internal.Executor
import scalaz.zio.{DefaultRuntime, UIO, ZIO}

import scala.concurrent.ExecutionContext

final case class DbRecord()

case class Db() {
  def blockingIo: ZIO[Clock, Nothing, DbRecord] = for {
    _ <- sleep(Duration(1, TimeUnit.SECONDS))
  } yield DbRecord()
}

final case class Response()

final case class SlowApiService() {
  def request: ZIO[Clock, Nothing, Response] = for {
    _ <- sleep(Duration(1000, TimeUnit.MILLISECONDS))
  } yield Response()
}

final case class FastApiService() {
  def request: ZIO[Clock, Nothing, Response] = for {
    _ <- sleep(Duration(100, TimeUnit.MILLISECONDS))
  } yield Response()
}

final case class ControlledApiService(dur: Duration) {
  def request: ZIO[Clock, Nothing, Response] = for {
    _ <- sleep(dur)
  } yield Response()
}

class ConcurrencyTest extends WordSpecLike with Matchers with DefaultRuntime {

  "raceEither" should {
    "return fastest result among two effects" in {
      unsafeRun(for {
        slow <- UIO(new SlowApiService)
        fast <- UIO(new FastApiService)
        slowOrFast <- slow.request.raceEither(fast.request)
      } yield slowOrFast) shouldBe a[Right[_, _]]
    }
  }

  "race" should {
    "interrupt slower effect" in {
      unsafeRun(for {
        forever <- UIO(ControlledApiService(Duration(Long.MaxValue, TimeUnit.MILLISECONDS)))
        fast <- UIO(ControlledApiService(Duration(100, TimeUnit.MILLISECONDS)))
        foreverOrFast <- forever.request.raceEither(fast.request)
      } yield foreverOrFast) shouldBe a[Right[_, _]]
    }
  }

  "zip" should {
    "return both results" in {
      unsafeRun(for {
        slow <- UIO(new SlowApiService)
        fast <- UIO(new FastApiService)
        both <- slow.request.zip(fast.request)
      } yield both) shouldBe a[(_, _)]
    }
  }

  "zip/zipPar" should {
    "execute effects in sequence/parallel" in {
      val s1IO = UIO(ControlledApiService(Duration(100, TimeUnit.MILLISECONDS)))
      val s2IO = UIO(ControlledApiService(Duration(120, TimeUnit.MILLISECONDS)))

      unsafeRun(
        for {
          s1 <- s1IO
          s2 <- s2IO
          s1TwiceInSequence = s1.request.zip(s1.request)
          result <- s1TwiceInSequence.raceEither(s2.request)
        } yield result
      ) shouldBe a[Right[_, _]] // s1 twice in sequence should be executed in about 200 msec.

      unsafeRun(
        for {
          s1 <- s1IO
          s2 <- s2IO
          s1TwiceInParallel = s1.request.zipPar(s1.request)
          result <- s1TwiceInParallel.raceEither(s2.request)
        } yield result
      ) shouldBe a[Left[_, _]] // s1 twice in parallel should be executed in about 100 msec.
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

  "interruptible" should {
    "make a blocking task interruptible" in {
      import scalaz.zio.blocking._
      unsafeRun(for {
        slept <- UIO(()).delay(Duration(10, TimeUnit.MILLISECONDS))
          .raceEither(blocking(ZIO.effect(java.lang.Thread.sleep(10000L))))
      } yield slept) shouldBe a[Left[_, _]]

      unsafeRun(for {
        slept <- UIO(()).delay(Duration(10, TimeUnit.MILLISECONDS))
          .raceEither(interruptible(java.lang.Thread.sleep(10000L)))
      } yield slept) shouldBe a[Left[_, _]]
    }
  }
}
