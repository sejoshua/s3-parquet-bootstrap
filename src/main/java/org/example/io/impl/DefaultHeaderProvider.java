package org.example.io.impl;

import lombok.AllArgsConstructor;

/**
 * @author Joshua Xing
 */
@AllArgsConstructor
public class DefaultHeaderProvider {
    private static final String COLUMN_NAME_TEMPLATE = "col_%d";

    private final int columnCount;

    public String[] getHeader() {
        String[] header = new String[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            header[i] = String.format(COLUMN_NAME_TEMPLATE, i + 1);
        }
        return header;
    }
}
