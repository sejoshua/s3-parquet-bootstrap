package org.example.filter.impl;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.example.filter.LineFilter;

/**
 * @author Joshua Xing
 */
@Slf4j
public class WordFilter implements LineFilter {
    private final String filterWord;

    public WordFilter(@NonNull String filterWord) {
        if (StringUtils.isEmpty(filterWord)) {
            throw new IllegalArgumentException("Cannot initialize a filter with null or empty string");
        }
        this.filterWord = filterWord;
    }

    @Override
    public boolean test(String s) {
        log.debug("Testing word [{}].", s);
        return !StringUtils.isEmpty(s) && s.contains(filterWord);
    }
}
