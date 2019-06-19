package sequence.data;

import sequence.Sequence;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.LineIterator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static java.text.MessageFormat.format;

/**
 * Created by Jan Kolomazník on 27.6.17. optimized by josef havranek not redacted
 */
@Slf4j
@Component
public class SequenceDataRepository {
    private static final String FASTA = "fasta";
    private static final String INTERNAL_FORMAT = "sequence-buffer";
    private static final int SHARED_CPU_CAHCE = 3000000;//this is suboptimal ... it would be beter to query for cache.. i am writing solution for this
    private static final int MAX_LINE_LENGTH = SHARED_CPU_CAHCE / Runtime.getRuntime().availableProcessors();

    private Path storageDir;

    @Value("${sequence.dir}")
    public void setStorageDir(String storageDir) {
        this.storageDir = Paths.get(storageDir);
        // Create if not exist
        if (Files.notExists(this.storageDir)) {
            try {
                Files.createDirectories(this.storageDir);
            } catch (IOException e) {
                throw new UnsupportedOperationException(format("Create sequence {0} dir failed.", storageDir), e);
            }
        }
    }

    // precessing to make  lines long enough to be worth processing in parallel
    // but not that long that cpus would be running out of cache constantly
    // this method is heavyly optimized josef havranek
    //this is where multifasta detection
    //and line length optimalization hapens
    public Path saveRawBuffer(UUID bufferId, String format, InputStream buffer) {
        Path path = getPath(bufferId, format);
        assert Files.notExists(path) : "Sequence buffer data-file can't by overridden.";
        //light pre processing
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path.toFile()), StandardCharsets.UTF_8))) {
            try (LineIterator raw = new LineIterator(new InputStreamReader(buffer))) {
                String line;
                long currentLength = 0;
                if (format.equals(FASTA)) {
                    boolean previousWasFASTA = false;
                    boolean isMultiFASTA = false;
                    while (raw.hasNext()) {
                        line = raw.next();
                        //found fasta comment that must be on separate line... reset counter put it on separate line
                        if (line.startsWith(">") || line.startsWith(";")) {
                            currentLength = 0;
                            if (!previousWasFASTA) {
                                previousWasFASTA = true;
                                if (!isMultiFASTA) {
                                    isMultiFASTA = true;
                                } else {
                                    throw new UnsupportedOperationException("Illegal input detected!\n" +
                                            "Multi-FASTA format is not supported");
                                }
                            } else {
                                //agregating fasta lines on one line to make extraction in stream stateless
                                // look here
                                //https://docs.oracle.com/javase/tutorial/collections/streams/parallelism.html#stateful_lambda_expressions
                                writer.write("\0");
                            }

                            writer.write(line);
                        } else {
                            if (previousWasFASTA) {
                                previousWasFASTA = false;
                                writer.write("\n");
                            }
                            currentLength += line.length();
                            if (currentLength >= MAX_LINE_LENGTH) {
                                writer.write("\n");
                                currentLength = 0;
                            }
                            writer.write(line);
                        }
                    }
                } else {// plain format 
                    while (raw.hasNext()) {
                        line = raw.next();
                        currentLength += line.length();
                        if (currentLength >= MAX_LINE_LENGTH) {
                            writer.write("\n");
                            currentLength = 0;
                        }
                        writer.write(line);
                    }
                }
            }
            return path;
        } catch (Exception e) {
            try {
                Files.delete(path);
            } catch (IOException ex) {
                ex = new IOException("Exeption during emergency deletion of partially written uploaded file " +
                        "exception encountered", ex);
                e.addSuppressed(ex);
            }
            throw new UnsupportedOperationException(e);
        } finally {
            try {
                buffer.close();
            } catch (IOException e) {
                log.warn("can't close stream of uploaded data.", e);
            }
        }
    }

    public Stream<String> loadRawBufferToStream(Sequence sequence, String format) {
        try {
            Path file = getPath(sequence.getBufferId(), format);
            return Files.lines(file);
        } catch (IOException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    public void deleteRawBuffer(UUID bufferId, String format) {
        Path file = getPath(bufferId, format);
        try {
            Files.delete(file);
        } catch (IOException e) {
            log.warn(format("Delete sequence data with format: {0} failed.", format), e);
        }
    }
    
    // this method is heavyly optimized Josef Havranek
    public Path save(UUID bufferId, Iterator<ByteBuffer> buffers) {
        Path file = getPath(bufferId, INTERNAL_FORMAT);
        assert Files.notExists(file) : "Sequence buffer data-file can't by overridden.";
        try (FileChannel output = new FileOutputStream(file.toFile()).getChannel()) {
            ByteBuffer current;
            while (buffers.hasNext()) {
                current = buffers.next().asReadOnlyBuffer();//making sure we have independent counters on buffer
                current.position(0);
                output.write(current);
            }
            //try with resources closes file itself
            return file;
        } catch (IOException e) {
            try {
                //delete unfinished file
                Files.delete(file);
            } catch (IOException deleteFail) {
                e.addSuppressed(new IOException("Exception In exception during emergency cleanup of incompletely written procesed file", deleteFail));
            }
            throw new UnsupportedOperationException("Exception during saving file", e);
        }
    }

    private Path getPath(UUID bufferId, String format) {
        String suffix = (Objects.equals(format, INTERNAL_FORMAT))
                ? ""
                : "." + format.toLowerCase();
        return storageDir.resolve(bufferId + suffix);
    }
}
