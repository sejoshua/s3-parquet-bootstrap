package org.example.exception;

import java.io.IOException;

/**
 * @author Joshua Xing
 */
public class ParquetExportException extends IOException {
    public ParquetExportException(String message) {
        super(message);
    }

    public ParquetExportException(String message, Throwable cause) {
        super(message, cause);
    }
}
