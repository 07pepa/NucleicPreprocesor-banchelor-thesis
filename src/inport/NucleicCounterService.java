package sequence.inport;

import sequence.Nucleic;

import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Josef Havránek
 * not redacted
 */

class NucleicCounterService {
    final private AtomicInteger runningTasks;
    final private EnumMap<Nucleic, AtomicInteger> counts;

    /**
     * Constructor that creates instance of counter
     */
    NucleicCounterService() {
        runningTasks = new AtomicInteger(0);
        counts = new EnumMap<>(Nucleic.class);
        for (Nucleic one : Nucleic.values())
            counts.put(one, new AtomicInteger(0));
    }

    /**
     * Count buffer in parallel thread pool in nonblocking way
     * Is thread save
     *
     * @param toCount -- non null buffer to count nucleotides must be read only (not changed by other thread)
     */
    void countBufferAsync(ByteBuffer toCount) {
        runningTasks.incrementAndGet();
        SequenceImportPool.importPool.execute(() -> {
                    toCount.position(0);
                    if (toCount.capacity() < 200) {
                        while (toCount.hasRemaining()) {
                            //this is thread save since map itself is not modified
                            counts.get(Nucleic.getFromInternalFormat(toCount.get())).incrementAndGet();
                        }
                    } else {
                        final HashMap<Nucleic, AtomicInteger> local = new HashMap<>();
                        Nucleic current;
                        while (toCount.hasRemaining()) {
                            current = Nucleic.getFromInternalFormat(toCount.get());
                            if (local.containsKey(current))
                                local.get(current).incrementAndGet();
                            else {
                                local.put(current, new AtomicInteger(1));
                            }
                        }
                        for (Nucleic key : local.keySet()) {
                            //this is thread save since map itself is not modified
                            counts.get(key).addAndGet(local.get(key).get());
                        }

                    }
                    runningTasks.decrementAndGet();
                }
        );
    }


    /**
     * method used to retrieve nucleic counts
     *
     * @return valid enum map with nucleic counts
     */
    EnumMap<Nucleic, Integer> getCounts() {
        final EnumMap<Nucleic, Integer> result = new EnumMap<>(Nucleic.class);
        while (runningTasks.get() > 0) {//wait for all to finish (is like spinLock)
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new InternalServerException("waiting for Nucleic counting task interrupted");
            }
        }
        final Iterator<Map.Entry<Nucleic, AtomicInteger>> iterator = counts.entrySet().iterator();
        Map.Entry<Nucleic, AtomicInteger> item;
        int nucleicCount;
        while (iterator.hasNext()) {
            item = iterator.next();
            nucleicCount = item.getValue().get();
            if (nucleicCount > 0)
                result.put(item.getKey(), nucleicCount);
        }
        return result;
    }

}
