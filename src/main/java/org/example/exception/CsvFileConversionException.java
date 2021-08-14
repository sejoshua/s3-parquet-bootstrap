package org.example.exception;

/**
 * @author Joshua Xing
 */
public class CsvFileConversionException extends Exception {
    public CsvFileConversionException(String message) {
        super(message);
    }

    public CsvFileConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
