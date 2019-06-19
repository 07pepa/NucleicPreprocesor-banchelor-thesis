package sequence.inport;

import cz.mendelu.dnaAnalyser.sequence.Nucleic;
import cz.mendelu.dnaAnalyser.sequence.Sequence;
import cz.mendelu.dnaAnalyser.sequence.SequenceRepository;
import cz.mendelu.dnaAnalyser.sequence.data.SequenceDataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @author Josef Havránek
 * not redacted
 */
@Service
public class RawDataProcessor {

    @Autowired
    private SequenceRepository sequenceRepository;

    @Autowired
    private SequenceDataRepository sequenceDataRepository;

    // beware some algorithms are binary
    // .... before debugging look how utf-8 works https://www.youtube.com/watch?v=MijmeoH9LT4

    //those constants are used as byte filter to make chars uppercase.
    final private static int BYTE_FILTER;//< is int because java can do direct bitwise operation on int and longs
    //following are just copy of previous on all relevant octets
    final private static int SHORT_FILTER;
    final private static int INT_FILTER;
    final private static long LONG_FILTER;
    final private static Charset UTF_8 = StandardCharsets.UTF_8;

    static {

        //(a starts at 97 (01100001) A is 01000001 you need to flip 3rd highest byte to change from upper to lower
        //that could be done with 95 (01011111) but that would only work with old school 7bit ascii
        //our data are in utf-8 ...we need to preserve highest order bit.

        //1<<7 puts one to highest bit and fixes problem with utf-8;
        BYTE_FILTER = (95 | 1 << 7);
        long workValue = (95 | 1 << 7); //casting would mess with long...so i create it from scratch

        SHORT_FILTER = BYTE_FILTER << 8 | BYTE_FILTER;//fills first 8 bits with copy of data and fills lower 8 what we already have
        workValue = workValue << 8 | workValue;

        INT_FILTER = SHORT_FILTER << 16 | SHORT_FILTER;// fills first 16 bits with copy of data and fill lower 16 bits with short
        workValue = workValue << 16 | workValue;


        workValue = workValue << 32 | workValue;// and grand finale... this fill first 32 bits with copy and also fill lower 32
        LONG_FILTER = workValue;
    }

    /**
     * Normalizes raw char to internal format...must be a letter
     *
     * @param in must be in ASCII or UTF-8
     * @return normalized char
     */
    public static byte toInternal(byte in) {
        return (byte) (in & BYTE_FILTER);
    }


    /**
     * Less memory intensive and faster than default java implementation of translating characters to uppercase
     *
     * @param buffer buffer of characters in utf-8
     * @return internal format of nucleic string as buffer (correctly translated are only byte chars everything else
     * is mangled but it never match nucleic.)
     */
    static private ByteBuffer nucleicStringToUppercase(ByteBuffer buffer) {
        final int bufferSize = buffer.capacity();
        buffer.position(0);
        final int lastValidIndex = bufferSize - 1;
        final int lastLongIndex = bufferSize - 7;

        //this takes care about odd byte on end if there is any
        buffer.put(lastValidIndex, (byte) (buffer.get(lastValidIndex) & BYTE_FILTER));

        // this processes most data
        while (buffer.position() < lastLongIndex)
            buffer.putLong(buffer.getLong(buffer.position()) & LONG_FILTER);

        //this processes leftovers
        if (buffer.position() < lastValidIndex) {
            if (buffer.position() < bufferSize - 3) {
                buffer.putInt(buffer.getInt(buffer.position()) & INT_FILTER);
                if (buffer.position() >= lastValidIndex)
                    return buffer;
            }
            buffer.putShort((short) (buffer.getShort(buffer.position()) & SHORT_FILTER));
        }
        return buffer;
    }

    /**
     * Retrieve data from sequence repo runs transformation on it save it and delete raw buffer from repo.
     *
     * @param s              non null sequence
     * @param format         format of sequence
     * @param rawTransformer method that transforms data to internal format
     */
    void batchProcessor(Sequence s, String format, Function<Stream<String>, Stream<ByteBuffer>> rawTransformer) {
        SequenceImportPool.importPool.invoke(
                batchProcessorHelper(s, sequenceDataRepository.loadRawBufferToStream(s, format), rawTransformer));
        sequenceDataRepository.deleteRawBuffer(s.getBufferId(), format);
    }

    /**
     * Takes your stream string input runs transformations on it and saves it.
     * <p>
     * Note:
     * Reason for this to be Fork join task that is spawns a ton of other threads (and if they are "forked" from here
     * they use same pool) ... this is only way to keep stream to spawn new system threads and use pre made pool
     *
     * @param s              sequence
     * @param input          valid stream of strings of lines. (is fully managed internally and does not have to be closed)
     * @param rawTransformer method that transforms data to internal format
     * @return returns ForkJoinTask to be executed in pool
     */
    ForkJoinTask<Boolean> batchProcessorHelper(Sequence s, Stream<String> input, Function<Stream<String>, Stream<ByteBuffer>> rawTransformer) {
        return ForkJoinTask.adapt(() -> {
            try (Stream<ByteBuffer> internalFormatStream = rawTransformer.apply(input.parallel())) {
                AtomicInteger length = new AtomicInteger(0);
                NucleicCounterService counter = new NucleicCounterService();
                Iterator<ByteBuffer> iterator = internalFormatStream
                        /*locking to avoid changes*/
                        .map(ByteBuffer::asReadOnlyBuffer)
                        /*initiate nucleic counting*/
                        .peek(toCount ->
                                {
                                    length.addAndGet(toCount.limit());
                                    counter.countBufferAsync(toCount);
                                }
                        ).iterator();

                //saving buffers (execution of stream happens here)
                sequenceDataRepository.save(s.getBufferId(), iterator);
                s.setLength(length.get());
                s.setNucleicCounts(counter.getCounts());
                sequenceRepository.save(s);
                return true;
            }
        });
    }

    /**
     * transforms data to inner format
     *
     * @param input        stream containing lines of fasta Strings
     * @param FASTAComment non null string buffer.... need to be regexed \0 replaces new lines because perf. reson
     * @return transformed stream of buffers in internal format to save or compute upon
     */

    private static Stream<ByteBuffer> transformFASTAToInternalExecutor(Stream<String> input, StringBuffer FASTAComment) {
        Stream<String> plain = input.filter(line -> {
            if ((line.startsWith(">") || line.startsWith(";"))) {
                FASTAComment.append(line).append("\n");
                return false;
            }
            return true;
        });
        return RawDataProcessor.transformPlain(plain);
    }

    /**
     * convenience wrapper of transformFASTAToInternalExecutor
     *
     * @param FASTAComment non null string buffer.... need to be regexed \0 replaces new lines because perf. reson
     */
    public static Function<Stream<String>, Stream<ByteBuffer>> transformFASTAToInternal(final StringBuffer FASTAComment) {
        return in -> transformFASTAToInternalExecutor(in, FASTAComment);
    }

    /**
     * Transforms data to inner format
     *
     * @param in stream containing lines of nucleic Strings (not containing FASTA coments)
     * @return returns transformed buffers to save in internal format or compute upon
     */
    static Stream<ByteBuffer> transformPlain(Stream<String> in) {
        return in.map(line -> ByteBuffer.wrap(line.getBytes(UTF_8))).filter(buff -> buff.capacity() > 0)
                .map(RawDataProcessor::nucleicStringToUppercase).map(RawDataProcessor::trimNonNucleic);
    }

    /**
     * Trims non nucleic chars from your buffer
     *
     * @param in non null buffer to trim
     * @return returns clean buffer (if is input clean it returns original buffer)
     */
    private static ByteBuffer trimNonNucleic(ByteBuffer in) {
        in.position(0);

        while (in.hasRemaining()) {
            if (Nucleic.isNotNuclidInInernal(in.get())) {
                in.position(in.position() - 1);//leaving position at dirty place
                break;
            }
        }
        if (!in.hasRemaining())
            return in;
        else
            return fixDirtyBuffer(in);
    }

    /**
     * fixes dirty buffer
     *
     * @param in buffer where first dirty position is set
     * @return returns clean buffer
     */
    private static ByteBuffer fixDirtyBuffer(ByteBuffer in) {
        ByteArrayOutputStream clean = new ByteArrayOutputStream();
        int unclean = in.position();
        in.position(0);

        //copy already known clean
        while (in.position() < unclean)
            clean.write(in.get());
        byte current;
        in.position(unclean + 1);

        //check leftovers if they are ok (and copy them to clean)
        while (in.hasRemaining()) {
            current = in.get();
            if (!Nucleic.isNotNuclidInInernal(current))
                clean.write(current);
        }
        return ByteBuffer.wrap(clean.toByteArray());
    }
}
