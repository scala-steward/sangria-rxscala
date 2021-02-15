package sangria.streaming

import scala.language.higherKinds

import rx.lang.scala.Observable

import scala.concurrent.{ExecutionContext, Future, Promise}

object rxscala {
  class ObservableSubscriptionStream(implicit ec: ExecutionContext)
      extends SubscriptionStream[Observable] {
    override def supported[T[_]](other: SubscriptionStream[T]): Boolean =
      other.isInstanceOf[ObservableSubscriptionStream]

    override def map[A, B](source: Observable[A])(fn: A => B): Observable[B] = source.map(fn)

    override def singleFuture[T](value: Future[T]): Observable[T] =
      Observable.from(value)

    override def single[T](value: T): Observable[T] = Observable.just(value)

    override def mapFuture[A, B](source: Observable[A])(fn: A => Future[B]): Observable[B] =
      source.flatMap(a => Observable.from(fn(a)))

    override def first[T](s: Observable[T]): Future[T] = {
      val promise = Promise[T]()

      s.take(1)
        .subscribe(
          t => promise.success(t),
          e => promise.failure(e),
          () =>
            if (!promise.isCompleted)
              promise.failure(
                new IllegalStateException(
                  "Promise was not completed - observable haven't produced any elements."))
        )

      promise.future
    }

    override def failed[T](e: Throwable): Observable[Nothing] = Observable.error(e)

    override def onComplete[Ctx, Res](result: Observable[Res])(op: => Unit): Observable[Res] =
      result.doAfterTerminate(op)

    override def flatMapFuture[Ctx, Res, T](future: Future[T])(
        resultFn: T => Observable[Res]): Observable[Res] =
      Observable.from(future).flatMap(resultFn)

    override def merge[T](streams: Vector[Observable[T]]): Observable[T] =
      if (streams.size > 1)
        streams.tail.foldLeft(streams.head) { case (acc, e) => acc.merge(e) }
      else if (streams.nonEmpty)
        streams.head
      else
        throw new IllegalStateException("No streams produced!")

    override def recover[T](stream: Observable[T])(fn: Throwable => T): Observable[T] =
      stream.onErrorReturn(e => fn(e))
  }

  implicit def observableSubscriptionStream(implicit
      ec: ExecutionContext): SubscriptionStream[Observable] =
    new ObservableSubscriptionStream
}
