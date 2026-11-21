/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * stardelt — the projected-token catalog client.
 */
package io.stardelt.iceberg;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;

/**
 * A catalog identity read from a file on every request: a workload's projected ServiceAccount
 * token (audience {@code lakekeeper}), which the kubelet rewrites before it expires (#492, #769).
 *
 * <p>Configured with {@code rest.auth.type=io.stardelt.iceberg.TokenFileAuthManager} and
 * {@code rest.auth.token-file=/var/run/secrets/stardelt-catalog/token}. The file is read inside
 * {@link Session#authenticate}, never at session build, so a rotation needs no restart and a
 * job may outlive the token it started with. A static token could not: #769 measured it refused
 * at {@code exp + 70 s}.
 */
public final class TokenFileAuthManager implements AuthManager {

  /** The catalog property that names the token file. */
  public static final String TOKEN_FILE = "rest.auth.token-file";

  /** Required by {@code AuthManagers.loadAuthManager}; the properties arrive in {@link #catalogSession}. */
  public TokenFileAuthManager(String ignored) {}

  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    String file = properties == null ? null : properties.get(TOKEN_FILE);
    if (file == null || file.isBlank()) {
      throw new IllegalArgumentException(
          "TokenFileAuthManager needs " + TOKEN_FILE + " (the projected token's path)");
    }
    return new Session(Path.of(file));
  }

  @Override
  public void close() {}

  private static final class Session implements AuthSession {
    private final Path file;

    private Session(Path file) {
      this.file = file;
    }

    @Override
    public HTTPRequest authenticate(HTTPRequest request) {
      HTTPHeaders headers =
          request
              .headers()
              .putIfAbsent(HTTPHeaders.of(Map.of("Authorization", "Bearer " + token())));
      return headers.equals(request.headers())
          ? request
          : ImmutableHTTPRequest.builder().from(request).headers(headers).build();
    }

    /** The token as the kubelet last wrote it: read per request, never cached in a field. */
    private String token() {
      try {
        String token = Files.readString(file, StandardCharsets.UTF_8).trim();
        if (token.isEmpty()) {
          throw new IllegalStateException("the catalog token file " + file + " is empty");
        }
        return token;
      } catch (IOException e) {
        throw new IllegalStateException("cannot read the catalog token file " + file, e);
      }
    }

    @Override
    public void close() {}
  }
}
