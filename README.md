# Robustools

Robustools is a set of small libraries that bring more robustness to your JVM-based application.

* FaultTolerantCache is a cache that returns cached entry longer than TTL when cache reloading fails.

* RetryingExecutor executes a Callable or Runnable with retrying. Retrying interval is exponential back-off.

* LeakyBucket implements the leaky bucket algorithm that is useful for throughput control and API rate limiting.

Robustools doesn't have runtime library dependencies.

## Build configuration

### Gradle

Add following line to `repositories { ... }` block in your `build.gradle`:

```
repositories {
  maven { url 'https://maven.pkg.github.com/frsyuki' }  // Robustools by frsyuki
}
```

Then add following dependency line to `dependencies { ... }` block:

```
dependencies {
  compile 'robustools:robustools:1.0'
}
```

### Maven

Add following entity to `<repositories>` tag in your `pom.xml`:

```
<repository>
  <id>github</id>
  <name>Robustools by frsyuki</name>
  <url>https://maven.pkg.github.com/frsyuki</url>
</repository>
```

Then add following entity to `<dependencies> tag:

```
<dependencies>
  <dependency>
    <groupId>robustools</groupId>
    <artifactId>robustools</artifactId>
    <version>1.0</version>
  </dependency>
</dependencies>
```

## Development

### Build

```
$ ./gradlew --info check build
```

### Release

```
$ edit ChangeLog
$ edit build.gradle  #=> increment version
$ ./gradlew clean check
$ ./gradlew publish
```

