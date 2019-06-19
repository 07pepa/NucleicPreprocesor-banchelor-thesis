package sequence;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
/**
 * writen by jan kolomaznik
 * optimized by Josef Havránek
 * not redacted
 */
@Slf4j
@Service
@Scope("singleton")
public class NCBISequenceService {


    private static final String urlFormat = "%s?db=%s&id=%s&retmode=%s&rettype=%s";

    private static final String METHOD = "GET";

    @Value("${sequence.ncbi.restriction-timing}")
    private long restrictionTiming;

    @Value("${sequence.ncbi.url}")
    private String url;

    @Value("${sequence.ncbi.db}")
    private String db;

    @Value("${sequence.ncbi.retmode}")
    private String retmode;

    @Value("${sequence.ncbi.rettype}")
    private String rettype;

    private boolean sslEnabled = false;

    /**
     * Last time successful download occurred
     */
    private static AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());

    private static Lock downloadLock = new ReentrantLock();//to allow only one download at time

    private void waitRestrictionTiming() {
        long now = System.currentTimeMillis();
        if ((now - lastTime.get()) < restrictionTiming) {
            try {
                log.info("The time limit is applied, delay time is {} milliseconds.", restrictionTiming);
                Thread.sleep(restrictionTiming);
                waitRestrictionTiming();
            } catch (InterruptedException e) {
                throw new InternalServerException("Obtaining source has been interrupted.");
            }
        }
    }

    //writen by josef havranek inspired by jan kolomaznik
    private void downloadCloser(InputStream is, String ncbiID) {
        try {
            is.close();
        } catch (IOException e) {
            log.warn("can't close InputStream for NCBIid: " + ncbiID);
        }
        lastTime.set(System.currentTimeMillis());
        downloadLock.unlock();
    }

    //Returned stream must be closed afterwards at all cost
    /**the only method optimized by josef haranek */
    public Stream<String> downloadFASTA(String ncbiId) {
        try {
            downloadLock.lock();
            waitRestrictionTiming();
            String urlAddress = String.format(urlFormat, url, db, ncbiId, retmode, rettype);
            final InputStream is = getResource(urlAddress);
            return new BufferedReader(new InputStreamReader(is)).lines().onClose(() -> downloadCloser(is, ncbiId));
        } catch (Exception e) {
            downloadLock.unlock();//lock must be released if we did not returned stream
            throw e;
        }
    }


    private InputStream getResource(String urlAddress) {
        try {
            log.info("Start get resource from {}.", urlAddress);
            if (!sslEnabled) setSslEnabled();
            URL url = new URL(urlAddress);
            HttpsURLConnection urlConnection = (HttpsURLConnection) url.openConnection();
            urlConnection.setRequestMethod(METHOD);
            urlConnection.connect();

            if (urlConnection.getResponseCode() != HttpURLConnection.HTTP_OK) {
                //redirect
                if (urlConnection.getResponseCode() != HttpURLConnection.HTTP_MOVED_PERM || urlConnection.getResponseCode() != HttpURLConnection.HTTP_MOVED_TEMP) {
                    String newUrl = urlConnection.getHeaderField("Location");
                    log.warn("Resource moved to {}.", newUrl);
                    return this.getResource(newUrl);
                } else {
                    log.error("Get resource failed: {} ({}).", urlConnection.getResponseCode(), urlConnection.getResponseMessage());
                    throw new InternalServerException(String.format("Get resource failed. Server response code %d\n", urlConnection.getResponseCode()));
                }
            }

            log.info("Connection open, start download");
            return urlConnection.getInputStream();
        } catch (Exception e) {
            log.error("Get resource failed: {}.", e.getMessage());
            throw new InternalServerException(String.format("Get resource from url %s failed.", urlAddress), e);
        }
    }

    private void setSslEnabled() throws NoSuchAlgorithmException, KeyManagementException {
        log.info("Create a trust manager that does not validate certificate chains.");
        final TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
            @Override
            public void checkClientTrusted(final X509Certificate[] chain, final String authType) {
            }

            @Override
            public void checkServerTrusted(final X509Certificate[] chain, final String authType) {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        }};
        log.info("Install the all-trusting trust manager.");
        final SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, null);

        log.info("Create an ssl socket factory with our all-trusting manager.");
        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
        HttpsURLConnection.setDefaultHostnameVerifier((urlHostName, session) -> true);
        sslEnabled = true;
    }
}
