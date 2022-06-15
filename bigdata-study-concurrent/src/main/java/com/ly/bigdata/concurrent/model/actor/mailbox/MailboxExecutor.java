package com.ly.bigdata.concurrent.model.actor.mailbox;

public interface MailboxExecutor {
    void yield() throws InterruptedException,RuntimeException;
    boolean tryYield() throws RuntimeException;

}
