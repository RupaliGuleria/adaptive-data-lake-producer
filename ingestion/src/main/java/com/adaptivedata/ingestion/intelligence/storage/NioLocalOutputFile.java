package com.adaptivedata.ingestion.intelligence.storage;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Parquet {@link OutputFile} backed by a local {@link Path}. Parquet-avro 1.13.1 ships
 * no built-in local implementation; this fills that gap so we can write to a temp file
 * without pulling in the deprecated Hadoop-{@code Path}-based builder.
 */
final class NioLocalOutputFile implements OutputFile {

    private final Path path;

    NioLocalOutputFile(Path path) {
        this.path = path;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return wrap(Files.newOutputStream(path,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return wrap(Files.newOutputStream(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING));
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    @Override
    public String getPath() {
        return path.toString();
    }

    private static PositionOutputStream wrap(OutputStream out) {
        return new PositionOutputStream() {
            private long pos = 0;

            @Override
            public long getPos() {
                return pos;
            }

            @Override
            public void write(int b) throws IOException {
                out.write(b);
                pos++;
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
                pos += len;
            }

            @Override
            public void flush() throws IOException {
                out.flush();
            }

            @Override
            public void close() throws IOException {
                out.close();
            }
        };
    }
}
