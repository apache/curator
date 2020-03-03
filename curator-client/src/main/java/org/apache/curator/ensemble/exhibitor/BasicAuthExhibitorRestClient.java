package org.apache.curator.ensemble.exhibitor;

import com.google.common.io.CharStreams;
import org.apache.curator.utils.CloseableUtils;
import sun.misc.BASE64Encoder;

import javax.net.ssl.*;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class BasicAuthExhibitorRestClient  implements ExhibitorRestClient
{
    private final boolean useSsl;
    private final boolean validateSsl;
    private final String userInfo;

    public BasicAuthExhibitorRestClient(boolean useSsl, boolean validateSsl, String userInfo)
    {
        this.useSsl = useSsl;
        this.validateSsl = validateSsl;
        this.userInfo = userInfo;
    }

    @Override
    public String getRaw(String hostname, int port, String uriPath, String mimeType) throws Exception
    {
        URI uri = new URI(useSsl ? "https" : "http", null, hostname, port, uriPath, null, null);
        HttpURLConnection connection = (HttpURLConnection)uri.toURL().openConnection();
        if (useSsl && !validateSsl) {
            X509TrustManager trustAllCert = new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {}

                public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {}

                public X509Certificate[] getAcceptedIssuers() { return null; }
            };
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, new TrustManager[]{trustAllCert}, new java.security.SecureRandom());
            ((HttpsURLConnection)connection).setSSLSocketFactory(sc.getSocketFactory());
            ((HttpsURLConnection)connection).setHostnameVerifier(new HostnameVerifier(){
                public boolean verify(String host,  SSLSession session){
                    return true;
                }
            });
        }
        connection.addRequestProperty("Accept", mimeType);
        connection.addRequestProperty("Authorization", "Basic " + new BASE64Encoder().encode(userInfo.getBytes()));

        Reader in = new InputStreamReader(new BufferedInputStream(connection.getInputStream()));
        try
        {
            return CharStreams.toString(in);
        }
        finally
        {
            CloseableUtils.closeQuietly(in);
        }
    }
}