package org.example.io.impl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Joshua Xing
 */
class DefaultHeaderProviderTest {
    private static final String ERROR_MSG_TEMPLATE = "Expecting at least 1 column, but got %d";

    @Test
    public void testBadInitialization() {
        assertThrows(IllegalArgumentException.class,
                () -> new DefaultHeaderProvider(-1), String.format(ERROR_MSG_TEMPLATE, -1));
        assertThrows(IllegalArgumentException.class,
                () -> new DefaultHeaderProvider(0), String.format(ERROR_MSG_TEMPLATE, 0));
    }

    @Test
    public void getHeaderTest() {
        getHeaderTest(1);
        getHeaderTest(10);
        getHeaderTest(100);
    }

    private void getHeaderTest(int columnCount) {
        DefaultHeaderProvider provider = new DefaultHeaderProvider(columnCount);
        String[] header = provider.getHeader();
        assertEquals(columnCount, header.length);
        for (int i = 0; i < columnCount; ++i) {
            assertEquals("col_" + (i + 1), header[i]);
        }
    }
}