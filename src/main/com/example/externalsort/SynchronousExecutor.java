package com.example.externalsort;

import org.apache.commons.lang.Validate;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * The class is intended to execute a {@link RecursiveAction} instance synchronously.
 * This functionality must be already implemented somewhere, but unfortunately I haven't been able to find it.
 *
 * @author Ruslan Sverchkov
 */
@ThreadSafe
public class SynchronousExecutor {

    private final ForkJoinPool pool;

    /**
     * Constructs a SynchronousExecutor instance.
     *
     * @param pool a fork join pool to execute tasks
     * @throws IllegalArgumentException if pool is null
     */
    public SynchronousExecutor(ForkJoinPool pool) {
        Validate.notNull(pool);
        this.pool = pool;
    }

    /**
     * The method is intended to execute a {@link RecursiveAction} instance synchronously.
     *
     * @param action an action to execute
     * @throws IllegalArgumentException if action is null
     * @throws Throwable                if any error occurred in the action itself
     *                                  (rethrown {@link RecursiveAction#getException()})
     */
    public void execute(RecursiveAction action) throws Throwable {
        Validate.notNull(action);
        pool.execute(action);
        while (!action.isDone()) {
            Thread.yield();
        }
        if (action.getException() != null) {
            throw action.getException();
        }
    }

}