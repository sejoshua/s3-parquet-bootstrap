package org.example.io.impl;

/**
 * @author Joshua Xing
 */
public class DefaultHeaderProvider {
    private static final String COLUMN_NAME_TEMPLATE = "col_%d";

    private final int columnCount;

    public DefaultHeaderProvider(int columnCount) {
        if (columnCount < 1) {
            throw new IllegalArgumentException("Expecting at least 1 column, but got " + columnCount);
        }
        this.columnCount = columnCount;
    }

    public String[] getHeader() {
        String[] header = new String[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            header[i] = String.format(COLUMN_NAME_TEMPLATE, i + 1);
        }
        return header;
    }
}
