package org.example.filter.impl;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WordFilterTest {
    private static final String ERROR_MESSAGE = "Cannot initialize a filter with null or empty string";

    @Test
    public void testBadInitialization() {
        assertThrows(IllegalArgumentException.class, () -> new WordFilter(StringUtils.EMPTY), ERROR_MESSAGE);
    }

    @Test
    public void testTest() {
        WordFilter filter = new WordFilter("ellipsis");

        assertTrue(filter.test("ellipsis"));
        assertTrue(filter.test("this is a test line ending with the expected word: ellipsis."));
        assertTrue(filter.test("ellipsis: this is a test line starting with the expected word."));
        assertTrue(filter.test("this is a test line ellipsis containing the expected word."));

        assertFalse(filter.test(null));
        assertFalse(filter.test(StringUtils.EMPTY));
        assertFalse(filter.test("this is a line without the expected word."));
        assertFalse(filter.test(".!@#%$^1489481"));
    }
}