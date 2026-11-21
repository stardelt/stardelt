/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * stardelt — the build-time check of the projected-token catalog client.
 */
package io.stardelt.iceberg;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.auth.AuthSession;

/** The build-time check of {@link TokenFileAuthManager}: the header follows the file. */
public final class TokenFileAuthManagerCheck {
  public static void main(String[] args) throws Exception {
    Path file = Files.createTempFile("catalog-token", ".jwt");
    Files.writeString(file, "token-one\n", StandardCharsets.UTF_8);
    AuthSession session =
        new TokenFileAuthManager("ignored")
            .catalogSession(null, Map.of(TokenFileAuthManager.TOKEN_FILE, file.toString()));

    String first = header(session);
    Files.writeString(file, "token-two\n", StandardCharsets.UTF_8);
    String second = header(session);
    Files.delete(file);
    if (!"Bearer token-one".equals(first) || !"Bearer token-two".equals(second)) {
      throw new IllegalStateException("token file not read per request: " + first + " / " + second);
    }
    System.out.println("stardelt-auth: reads-per-request=yes first=token-one second=token-two");
  }

  private static String header(AuthSession session) throws Exception {
    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .method(HTTPRequest.HTTPMethod.GET)
            .baseUri(new URI("http://localhost:1/catalog"))
            .path("v1/config")
            .build();
    // HTTPHeaders.firstEntry returns Optional<HTTPHeader>; HTTPHeader#value() is the token.
    return session.authenticate(request).headers().firstEntry("Authorization")
        .map(header -> header.value())
        .orElseThrow(() -> new IllegalStateException("no Authorization header"));
  }
}
