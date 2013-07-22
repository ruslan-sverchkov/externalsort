package com.example.externalsort;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.Validate;

import javax.annotation.concurrent.Immutable;
import java.util.Collection;

/**
 * The class is intended to hold an object construction information, which is:
 * - an object itself if construction has been successful
 * - an errors collection if construction has been failed
 *
 * @author Ruslan Sverchkov
 */
@Immutable
public class ConstructionResult<T> {

    private final T object;
    private final Collection<String> errors;

    /**
     * Constructs a ConstructionResult instance. The errors collection is empty.
     *
     * @param object a successfully constructed object
     * @throws IllegalArgumentException if result is null
     */
    public ConstructionResult(T object) {
        Validate.notNull(object);
        this.object = object;
        this.errors = ImmutableList.of();
    }

    /**
     * Constructs a ConstructionResult instance. The result is null.
     *
     * @param errors an errors list
     * @throws IllegalArgumentException if:
     *                                  * errors list is null
     *                                  * errors list is empty
     *                                  * errors list contains nulls
     */
    public ConstructionResult(Collection<String> errors) {
        Validate.noNullElements(errors);
        Validate.notEmpty(errors);
        this.object = null;
        this.errors = ImmutableList.copyOf(errors);
    }

    /**
     * The object getter.
     *
     * @return the object, never returns null
     * @throws IllegalStateException if there are construction errors
     */
    public T getObject() {
        if (!errors.isEmpty()) {
            throw new IllegalStateException("Construction has been failed, examine the errors collection");
        }
        return object;
    }

    /**
     * Construction errors getter.
     *
     * @return the construction errors list, which is:
     *         * not null
     *         * not empty
     *         * does not contain nulls
     *         Pay attention that the returned list is immutable and an attempt to change it will lead to exception.
     */
    public Collection<String> getErrors() {
        return errors;
    }

}