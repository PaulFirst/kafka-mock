//package ru.test.common.utils;
//
//import java.util.function.Supplier;
//
//public class SystemTime implements Time {
//
//    @Override
//    public long milliseconds() {
//        return System.currentTimeMillis();
//    }
//
//    @Override
//    public long nanoseconds() {
//        return System.nanoTime();
//    }
//
//    @Override
//    public void sleep(long ms) {
//        Utils.sleep(ms);
//    }
//
//    @Override
//    public void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) throws InterruptedException {
//        synchronized (obj) {
//            while (true) {
//                if (condition.get())
//                    return;
//
//                long currentTimeMs = milliseconds();
//                if (currentTimeMs >= deadlineMs)
//                    throw new TimeoutException("Condition not satisfied before deadline");
//
//                obj.wait(deadlineMs - currentTimeMs);
//            }
//        }
//    }
//
//}
//
