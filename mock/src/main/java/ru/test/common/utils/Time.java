//package ru.test.common.utils;
//
//import java.time.Duration;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//import java.util.function.Supplier;
//
//public interface Time {
//
//    Time SYSTEM = new SystemTime();
//
//    long milliseconds();
//
//    default long hiResClockMs() {
//        return TimeUnit.NANOSECONDS.toMillis(nanoseconds());
//    }
//
//    long nanoseconds();
//
//    void sleep(long ms);
//
//    void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) throws InterruptedException;
//
//    default Timer timer(long timeoutMs) {
//        return new Timer(this, timeoutMs);
//    }
//
//    default Timer timer(Duration timeout) {
//        return timer(timeout.toMillis());
//    }
//
//    default <T> T waitForFuture(
//            CompletableFuture<T> future,
//            long deadlineNs
//    ) throws TimeoutException, InterruptedException, ExecutionException {
//        TimeoutException timeoutException = null;
//        while (true) {
//            long nowNs = nanoseconds();
//            if (deadlineNs <= nowNs) {
//                throw (timeoutException == null) ? new TimeoutException() : timeoutException;
//            }
//            long deltaNs = deadlineNs - nowNs;
//            try {
//                return future.get(deltaNs, TimeUnit.NANOSECONDS);
//            } catch (TimeoutException t) {
//                timeoutException = t;
//            }
//        }
//    }
//}
