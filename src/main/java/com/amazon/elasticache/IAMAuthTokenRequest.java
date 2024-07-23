package com.amazon.elasticache;

import java.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpFullRequest.Builder;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;

public class IAMAuthTokenRequest {
    private static final SdkHttpMethod REQUEST_METHOD = SdkHttpMethod.GET;
    private static final String REQUEST_PROTOCOL = "http://";
    private static final String PARAM_ACTION = "Action";
    private static final String PARAM_USER = "User";
    private static final String ACTION_NAME = "connect";
    private static final String SERVICE_NAME = "elasticache";
    private static final Duration TOKEN_EXPIRY_DURATION_SECONDS = Duration.ofSeconds(900);
    private static final String PARAM_RESOURCE_TYPE = "ResourceType";
    private static final String RESOURCE_TYPE_SERVERLESS_CACHE = "ServerlessCache";

    private final String userId;
    private final String replicationGroupId;
    private final String cacheName;
    private final boolean isServerless;
    private final String region;

    public IAMAuthTokenRequest(String userId, String replicationGroupId, String region) {
        this.userId = userId;
        this.replicationGroupId = replicationGroupId;
        this.cacheName = null;
        this.region = region;
        this.isServerless = false;
    }

    public IAMAuthTokenRequest(String userId, String cacheName, String region, boolean isServerless) {
        this.userId = userId;
        this.cacheName = cacheName;
        this.region = region;
        this.isServerless = isServerless;
        this.replicationGroupId = null;
    }


    public String toSignedRequestUri(AwsCredentials credentials) throws URISyntaxException {
        SdkHttpFullRequest request = getSignableRequest();

        // Sign the canonical request
        request = sign(request, credentials);

        // Return the signed URI
        return new URIBuilder(request.getUri())
            .addParameters(toNamedValuePair(request.rawQueryParameters()))
            .build()
            .toString()
            .replace(REQUEST_PROTOCOL, "");
    }

    private SdkHttpFullRequest getSignableRequest() {
        Builder builder = SdkHttpFullRequest.builder()
            .method(REQUEST_METHOD)
            .uri(getRequestUri())
            .appendRawQueryParameter(PARAM_ACTION, ACTION_NAME)
            .appendRawQueryParameter(PARAM_USER, userId);
        if (isServerless) {
            builder.appendRawQueryParameter(PARAM_RESOURCE_TYPE, RESOURCE_TYPE_SERVERLESS_CACHE);
        }
        return builder.build();
    }

    private URI getRequestUri() {
        if (replicationGroupId == null) {
            return URI.create(String.format("%s%s/", REQUEST_PROTOCOL, cacheName));
        } else {
            return URI.create(String.format("%s%s/", REQUEST_PROTOCOL, replicationGroupId));
        }
    }

    private SdkHttpFullRequest sign(SdkHttpFullRequest request, AwsCredentials credentials) {
        Instant expiryInstant = Instant.now().plus(TOKEN_EXPIRY_DURATION_SECONDS);
        Aws4Signer signer = Aws4Signer.create();
        Aws4PresignerParams signerParams = Aws4PresignerParams.builder()
            .signingRegion(Region.of(region))
            .awsCredentials(credentials)
            .signingName(SERVICE_NAME)
            .expirationTime(expiryInstant)
            .build();
        return signer.presign(request, signerParams);
    }

    private static List<NameValuePair> toNamedValuePair(Map<String, List<String>> in) {
        return in.entrySet().stream()
            .map(e -> new BasicNameValuePair(e.getKey(), e.getValue().get(0)))
            .collect(Collectors.toList());
    }
}