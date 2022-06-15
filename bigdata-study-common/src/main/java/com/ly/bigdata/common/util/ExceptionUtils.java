package com.ly.bigdata.common.util;

import static com.ly.bigdata.common.util.Preconditions.checkNotNull;

public final class ExceptionUtils {
    /**
     * Adds a new exception as a {@link Throwable#addSuppressed(Throwable) suppressed exception} to
     * a prior exception, or returns the new exception, if no prior exception exists.
     *
     * <pre>{@code
     * public void closeAllThings() throws Exception {
     *     Exception ex = null;
     *     try {
     *         component.shutdown();
     *     } catch (Exception e) {
     *         ex = firstOrSuppressed(e, ex);
     *     }
     *     try {
     *         anotherComponent.stop();
     *     } catch (Exception e) {
     *         ex = firstOrSuppressed(e, ex);
     *     }
     *     try {
     *         lastComponent.shutdown();
     *     } catch (Exception e) {
     *         ex = firstOrSuppressed(e, ex);
     *     }
     *
     *     if (ex != null) {
     *         throw ex;
     *     }
     * }
     * }</pre>
     *
     * @param newException The newly occurred exception
     * @param previous The previously occurred exception, possibly null.
     * @return The new exception, if no previous exception exists, or the previous exception with
     *     the new exception in the list of suppressed exceptions.
     */
    public static <T extends Throwable> T firstOrSuppressed(T newException, T previous) {
        checkNotNull(newException, "newException");

        if (previous == null || previous == newException) {
            return newException;
        } else {
            previous.addSuppressed(newException);
            return previous;
        }
    }
}
