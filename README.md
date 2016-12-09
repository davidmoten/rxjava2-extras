# rxjava2-extras
<a href="https://travis-ci.org/davidmoten/rxjava2-extras"><img src="https://travis-ci.org/davidmoten/rxjava2-extras.svg"/></a><br/>
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava2-extras/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava2-extras)<br/>
[![codecov](https://codecov.io/gh/davidmoten/rxjava2-extras/branch/master/graph/badge.svg)](https://codecov.io/gh/davidmoten/rxjava2-extras)<br/>

Utilities for use with RxJava 2

Bit by bit, features from [rxjava-extras](https://github.com/davidmoten/rxjava-extras) will be migrated to use RxJava2 (and of course new features will be added here too).

Features
----------
* [`Strings`](#strings) - create/manipulate streams of `String`, conversions to and from
* [`Bytes`](#bytes) - create/manipulate streams of `byte[]`
* [`StateMachine`](#transformersstatemachine) - a more expressive form of `scan` that can emit multiple events for each source event

Migration
------------
* Primary target type is `Flowable` (the backpressure supporting stream)
* Where applicable `Single`, `Maybe` and `Completable` will be used
* To cross types (say from `Flowable` to `Maybe`) it is necessary to use `to` rather than `compose`
* Transformers (for use with `compose` and `to`) are clustered within the primary owning class rather than bunched together in the `Transformers` class. For example, `Strings.join`:

```java
//produces a stream of "ab"
Maybe<String> o = Flowable
  .just("a","b")
  .to(Strings.join()); 
```


Strings
----------
`concat`, `join`

`decode`

`from(Reader)`,`from(InputStream)`,`from(File)`, ...

`fromClasspath(String, Charset)`

`split`

`trim`

`strings`

`splitLinesFilterComments`

Bytes
--------------
`collect`

`from(InputStream)`, `from(File)`

`unzip(File)`, `unzip(InputStream)`

Transformers.stateMachine
---------------------------
TODO




