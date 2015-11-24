package com.example.externalsort;

import com.example.externalsort.aggregator.BufferAggregator;
import com.example.externalsort.aggregator.MappedBufferAggregator;
import com.example.externalsort.aggregator.MappedBufferAggregatorFactory;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ForkJoinPool;

/**
 * The class is intended to sort the specified file using the specified number of threads. This is the application's
 * entry point. Any messages to user are simply put in {@link System#out}, in my opinion this is
 * quite reasonable for such an application. Exceptions occurred during processing are eventually also printed in
 * the console which is also reasonable since there is nothing else to do with them anyway.
 * There are no attempts to implement i18n since there is no such requirement, even though messages is source code
 * look not so pretty.
 *
 * @author Ruslan Sverchkov
 */
public class ExternalSort {

    private static final int MAX_BYTES_TO_MAP = Integer.MAX_VALUE - Integer.MAX_VALUE % BufferAggregator.INT_SIZE_IN_BYTES;
    private static final String WELCOME = "Program usage: java -jar external_sort.jar <file path> <threads number>";

    /**
     * The method is intended to sort the specified file using the specified number of threads.
     *
     * @param args file path and threads number
     * @throws Throwable if any error occurred during processing
     */
    public static void main(String... args) throws Throwable {
        if (args.length != 2) {
            System.out.println(WELCOME);
            return;
        }
        ExternalSort sort = new ExternalSort();
        ConstructionResult<File> file = sort.getFile(args[0]);
        ConstructionResult<Integer> threadsNumber = sort.getThreadsNumber(args[1]);
        Collection<String> errors = new ArrayList<>();
        errors.addAll(file.getErrors());
        errors.addAll(threadsNumber.getErrors());
        if (!errors.isEmpty()) {
            for (String error : errors) {
                System.out.println(error);
            }
            return;
        }
        SynchronousExecutor executor = new SynchronousExecutor(new ForkJoinPool(threadsNumber.getObject()));
        MappedBufferAggregatorFactory factory = new MappedBufferAggregatorFactory(MAX_BYTES_TO_MAP);
        MappedBufferAggregator aggregator = factory.get(file.getObject());
        executor.execute(new SortTask(aggregator, 0, aggregator.getLength() - 1));
        aggregator.force();
    }

    /**
     * The method is intended to construct a file using the specified string.
     * Validation rules:
     * * file exists
     * * file length is positive
     * * file length is a multiple of {@link BufferAggregator#INT_SIZE_IN_BYTES}
     *
     * @param filePath a file path
     * @return a file construction result, never returns null
     */
    protected ConstructionResult<File> getFile(String filePath) {
        if (StringUtils.isEmpty(filePath)) {
            return new ConstructionResult<>(ImmutableList.of("File path is required"));
        }
        File file = new File(filePath);
        if (file.length() == 0) {
            return new ConstructionResult<>(ImmutableList.of("File is empty or does not exist"));
        }
        if (file.length() % BufferAggregator.INT_SIZE_IN_BYTES != 0) {
            return new ConstructionResult<>(ImmutableList.of("File size must be a multiple of "
                    + BufferAggregator.INT_SIZE_IN_BYTES));
        }
        return new ConstructionResult<>(file);
    }

    /**
     * The method is intended to construct a threads number value using the specified string.
     * Validation rule: the value must be a positive integer.
     *
     * @param threadsNumberString a string representation of threads number
     * @return a threads number value construction result, never returns null
     */
    protected ConstructionResult<Integer> getThreadsNumber(String threadsNumberString) {
        if (StringUtils.isEmpty(threadsNumberString)) {
            return new ConstructionResult<>(ImmutableList.of("Threads number is required"));
        }
        try {
            // could use apache commons validator but didn't like to pull the whole dependency
            // just because of one line of code, even though this is an exception-handled workflow
            Integer threadsNumber = Integer.parseInt(threadsNumberString);
            if (threadsNumber <= 0) {
                return new ConstructionResult<>(ImmutableList.of("Threads number must be positive"));
            }
            return new ConstructionResult<>(threadsNumber);
        } catch (NumberFormatException e) {
            return new ConstructionResult<>(ImmutableList.of("Threads number must be an integer"));
        }
    }

}