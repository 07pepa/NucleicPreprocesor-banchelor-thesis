package sequence.inport;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ForkJoinPool;

/**
 * not redacted LOL
 */
@Slf4j
class SequenceImportPool {
    private static final Thread.UncaughtExceptionHandler errorHandler = (thread, e) -> log.error("Thread spawned in sequence import pool failed\n" +
                    "Thread details: " + thread.toString() + " id:" + thread.getId() + "\n"
            , e);
    final static ForkJoinPool importPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
            ForkJoinPool.defaultForkJoinWorkerThreadFactory, errorHandler, true);


}
