package org.example.io;

import java.io.File;
import java.io.InputStream;
import java.util.List;

/**
 * @author Joshua Xing
 */
public interface FileClient {
    List<String> list(String path);

    void uploadFile(String path, String content);

    void uploadFile(String path, File file);

    InputStream getFileInputStream(String path);
}
