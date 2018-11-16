[![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![Released Version][maven-img]][maven]

# OpenTracing Redisson Instrumentation
OpenTracing instrumentation for Redisson.

## Installation

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-redisson</artifactId>
    <version>VERSION</version>
</dependency>
```

## Usage

```java
// Instantiate tracer
Tracer tracer = ...

// Optionally register tracer with GlobalTracer
GlobalTracer.register(tracer);

// Create Redisson config object
Config = ...

// Create Redisson instance
RedissonClient redissonClient = Redisson.create(config);

// Decorate RedissonClient with TracingRedissonClient
RedissonClient tracingRedissonClient =  new TracingRedissonClient(redissonClient, tracer);

// Get object you need using TracingRedissonClient
RMap<MyKey, MyValue> map = tracingRedissonClient.getMap("myMap");
```


## License

[Apache 2.0 License](./LICENSE).

[ci-img]: https://travis-ci.org/opentracing-contrib/java-redisson.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-redisson
[cov-img]: https://coveralls.io/repos/github/opentracing-contrib/java-redisson/badge.svg?branch=master
[cov]: https://coveralls.io/github/opentracing-contrib/java-redisson?branch=master
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-redisson.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-redisson

