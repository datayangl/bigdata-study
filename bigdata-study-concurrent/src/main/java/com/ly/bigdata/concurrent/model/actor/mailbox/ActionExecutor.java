package com.ly.bigdata.concurrent.model.actor.mailbox;

import java.util.concurrent.Callable;

/**
 * 兼容执行 {@link Runnable}, or {@link Callable}
 */
public interface ActionExecutor {
    void run(Runnable runnable) throws Exception;

    <R> R call(Callable<R> callable) throws Exception;

    ActionExecutor IMMEDIATE =
            new ActionExecutor() {
                @Override
                public void run(Runnable runnable) throws Exception {
                    runnable.run();
                }

                @Override
                public <R> R call(Callable<R> callable) throws Exception {
                    return callable.call();
                }
            };
}
