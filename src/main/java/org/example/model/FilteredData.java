package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

/**
 * @author Joshua Xing
 */
@AllArgsConstructor
@Getter
public class FilteredData {
    private final String originalFileName;
    private final String[] header;
    private final List<String[]> filteredLines;
}
