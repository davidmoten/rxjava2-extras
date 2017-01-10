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
* [`StateMachine`](#flowabletransformers) - a more expressive form of `scan` that can emit multiple events for each source event
* [`onBackpressureBufferToFile`](#onbackpressurebuffertofile) - high throughput with memory-mapped files
* [`FlowableTransformers`](#flowabletransformers)
* [`ObservableTransformers`](#observabletransformers)
* supports Java 1.6+

Status: *released to Maven Central*

Maven site reports are [here](http://davidmoten.github.io/rxjava2-extras/index.html) including [javadoc](http://davidmoten.github.io/rxjava2-extras/apidocs/index.html).

Getting started
-----------------
Add this to your pom.xml:

```xml
<dependency>
  <groupId>com.github.davidmoten</groupId>
  <artifactId>rxjava2-extras</artifactId>
  <version>VERSION_HERE</version>
</dependency>
```

Or add this to your build.gradle:
```groovy
repositories {
    mavenCentral()
}

dependencies {
    compile 'com.github.davidmoten:rxjava-extras:VERSION_HERE'
}
```

Migration
------------
* Primary target type is `Flowable` (the backpressure supporting stream)
* Operators will be implemented initially without fusion support (later)  
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

`from(Reader)`,`from(InputStream)`,`from(File)`, ..

`fromClasspath(String, Charset)`, ..

`split(String)`, `split(Pattern)`

`splitSimple(String)`

`trim`

`strings`

`splitLinesSkipComments`

Bytes
--------------
`collect`

`from(InputStream)`, `from(File)`

`unzip(File)`, `unzip(InputStream)`

RetryWhen
------------
[Builder](#retrywhen) for `.retryWhen()`

IO
-------------
`serverSocket(port)` 

Flowables
---------------
[`match`](#match-matchwith)

`repeat`

FlowableTransformers
---------------------------
[`doOnEmpty`](#doonempty)

`mapLast`

[`match`, `matchWith`](#match-matchwith)

[`onBackpressureBufferToFile`](#onbackpressurebuffertofile)

`reverse`

[`stateMachine`](#statemachine)

ObservableTransformers
---------------------------
[`onBackpressureBufferToFile`](#onbackpressurebuffertofile)

SchedulerHelper
----------------
`blockUntilWorkFinished`

`withThreadId`

`withThreadIdFromCallSite`

Actions
--------------------
`doNothing`
`setToTrue`
`throwing`

Callables
---------------
`constant`

Consumers
--------------
`addLongTo`
`addTo`
`assertBytesEquals`
`close`
`decrement`
`doNothing`
`increment`
`printStackTrace`
`println`
`set`
`setToTrue`

Functions
------------
`constant`
`identity`
`throwing`

BiFunctions
-------------
`throwing`

Predicates
-------------
`alwaysFalse`
`alwaysTrue`

doOnEmpty
-------------------------
Performs an action only if a stream completes without emitting an item.

```java
flowable.compose(
    FlowableTransformers.doOnEmpty(action));
```

match, matchWith
-------------------------
Finds out-of-order matches in two streams.

<img src="https://raw.githubusercontent.com/davidmoten/rxjava-extras/master/src/docs/match.png?raw=true" />

[javadoc](http://davidmoten.github.io/rxjava-extras/apidocs/com/github/davidmoten/rx/Transformers.html#matchWith--)

You can use `FlowableTranformers.matchWith` or `Flowables.match`:

```java
Flowable<Integer> a = Flowable.just(1, 2, 4, 3);
Flowable<Integer> b = Flowable.just(1, 2, 3, 5, 6, 4);
Flowables.match(a, b,
     x -> x, // key to match on for a
     x -> x, // key to match on for b
     (x, y) -> x // combiner
    )
   .forEach(System.out::println);
```
gives
```
1
2
3
4
```
Don't rely on the output order!

Under the covers elements are requested from `a` and `b` in alternating batches of 128 by default. The batch size is configurable in another overload.


##onBackpressureBufferToFile
With this operator you can offload a stream's emissions to disk to reduce memory pressure when you have a fast producer + slow consumer (or just to minimize memory usage).

<img src="https://raw.githubusercontent.com/davidmoten/rxjava-extras/master/src/docs/onBackpressureBufferToFile.png" />

If you have used the `onBackpressureBuffer` operator you'll know that when a stream is producing faster than the downstream operators can process (perhaps the producer cannot respond meaningfully to a *slow down* request from downstream) then `onBackpressureBuffer` buffers the items to an in-memory queue until they can be processed. Of course if memory is limited then some streams might eventually cause an `OutOfMemoryError`. One solution to this problem is to increase the effectively available memory for buffering by using disk instead (and small in-memory read/write buffers). That's why `onBackpressureBufferToFile` was created. 

*rxjava-extras* uses standard file io to buffer serialized stream items. This operator can still be used with RxJava2 using the [RxJava2Interop](https://github.com/akarnokd/RxJava2Interop) library. 

*rxjava2-extras* uses fixed size memory-mapped files to perform the same operation but with much greater throughput. 

Note that new files for a file buffered observable are created for each subscription and thoses files are in normal circumstances deleted on cancellation (triggered by `onCompleted`/`onError` termination or manual cancellation). 

Here's an example:

```java
// write the source strings to a 
// disk-backed queue on the subscription
// thread and emit the items read from 
// the queue on the io() scheduler.
Flowable<String> source = 
  Flowable
    .just("a", "b", "c")
    .compose(
      FlowableTransformers
          .onBackpressureBufferToFile()
          .serializerUtf8())
```

You can also use an `Observable` source (without converting to `Flowable` with `toFlowable`):
```java
Observable<String> source = 
  Observable
    .just("a", "b", "c")
    .to(
      ObservableTransformers
          .onBackpressureBufferToFile()
          .serializerUtf8())
```
Note that `to` is used above to cross types (`Observable` to `Flowable`).

This example does the same as above but more concisely and uses standard java IO serialization (normally it will be more efficient to write your own `DataSerializer`):

```java
Flowable<String> source = 
  Flowable
    .just("a", "b", "c")
    .compose(FlowableTransformers
        .<String>onBackpressureBufferToFile());
```

An example with a custom serializer:

```java
// define how the items in the source stream would be serialized
DataSerializer<String> serializer = new DataSerializer<String>() {

    @Override
    public void serialize(String s, DataOutput out) throws IOException {
        output.writeUTF(s);
    }

    @Override
    public String deserialize(DataInput in) throws IOException {
        return input.readUTF();
    }
    
    @Override
    public int sizeHint() {
        // exact size unknown
        return 0;
    }
};
Flowable
  .just("a", "b", "c")
  .compose(
    FlowableTransformers
        .onBackpressureBufferToFile()
        .serializer(serializer));
  ...
```
You can configure various options:

```java
Flowable
  .just("a", "b", "c")
  .compose(
    FlowableTransformers
        .onBackpressureBufferToFile()
        .scheduler(Schedulers.computation()) 
        .fileFactory(fileFactory)
        .pageSizeBytes(1024)
        .serializer(serializer)); 
  ...
```
`.fileFactory(Func0<File>)` specifies the method used to create the temporary files used by the queue storage mechanism. The default is a factory that calls `Files.createTempFile("bufferToFile", ".obj")`.

There are some inbuilt `DataSerializer` implementations:

* `DataSerializers.utf8()`
* `DataSerializers.string(Charset)`
* `DataSerializers.bytes()`
* `DataSerializers.javaIO()` - uses standard java serialization (`ObjectOutputStream` and such)

Using default java serialization you can buffer array lists of integers to a file like so:

```java
Flowable.just(1, 2, 3, 4)
    //accumulate into sublists of length 2
    .buffer(2)
    .compose(FlowableTransformers
        .<List<Integer>>onBackpressureBufferToFile()
        .serializerJavaIO())
```

In the above example it's fortunate that `.buffer` emits `ArrayList<Integer>` instances which are serializable. To be strict you might want to `.map` the returned list to a data type you know is serializable:

```java
Flowable.just(1, 2, 3, 4)
    .buffer(2)
    .map(list -> new ArrayList<Integer>(list))
    .compose(
      FlowableTransformers
          .<List<Integer>>onBackpressureBufferToFile()
          .serializerJavaIO())
```

###Performance
Throughput is increased dramatically by using memory-mapped files. 

*rxjava2-extras* can push through 800MB/s using 1K messages compared to *rxjava-extras* 43MB/s. My 2016 2 core i5 HP Spectre laptop with SSD pushes through up to 1.5GB/s.

Smaller messages mean more contention but still on my laptop I am seeing 6 million 40B messages per second.

repeatLast
------------------------
If a stream has elements and completes then the last element is repeated.

<img src="https://raw.githubusercontent.com/davidmoten/rxjava-extras/master/src/docs/repeatLast.png?raw=true" /> 

```java
flowable.compose(
    FlowableTransformers.repeatLast());
```

RetryWhen
----------------------
A common use case for `.retry()` is some sequence of actions that are attempted and then after a delay a retry is attempted. RxJava does not provide 
first class support for this use case but the building blocks are there with the `.retryWhen()` method. `RetryWhen` offers static methods that build a `Function` for use with `Flowable.retryWhen()`.

<img src="http://reactivex.io/documentation/operators/images/retry.C.png" width="500"/>

### Retry after a constant delay

```java
flowable.retryWhen(
    RetryWhen.delay(10, TimeUnit.SECONDS).build());
```

### Retry after a constant delay with a maximum number of retries

```java
flowable.retryWhen(
    RetryWhen.delay(10, TimeUnit.SECONDS)
        .maxRetries(10).build());
```

### Retry after custom delays

```java
//the length of waits determines number of retries
Flowable<Long> delays = Flowable.just(10L,20L,30L,30L,30L);
flowable.retryWhen(
    RetryWhen.delays(delays, TimeUnit.SECONDS).build());
```

### Retry only for a particular exception

```java
flowable.retryWhen(
    RetryWhen.retryWhenInstanceOf(IOException.class)
        .build());
```

stateMachine
--------------------------
Custom operators are difficult things to get right in RxJava mainly because of the complexity of supporting backpressure efficiently. `FlowableTransformers.stateMachine` enables a custom operator implementation when:

* each source emission is mapped to 0 to many emissions (of a different type perhaps) to downstream but those emissions are calculated based on accumulated state

<img src="https://raw.githubusercontent.com/davidmoten/rxjava-extras/master/src/docs/stateMachine.png?raw=true" />

[javadoc](http://davidmoten.github.io/rxjava-extras/apidocs/com/github/davidmoten/rx/Transformers.html#stateMachine-rx.functions.Func0-rx.functions.Func3-rx.functions.Action2-)

An example of such a transformation might be from a list of temperatures you only want to emit sequential values that are less than zero but are part of a sub-zero sequence at least 1 hour in duration. You could use `toListWhile` above (when migrated!) but `Transformers.stateMachine` offers the additional efficiency that it will immediately emit temperatures as soon as the duration criterion is met. 

To implement this example, suppose the source is half-hourly temperature measurements:

```java
static class State {
     final List<Double> list;
     final boolean reachedThreshold;
     State(List<Double> list, boolean reachedThreshold) {
         this.list = list; 
         this.reachedThreshold = reachedThreshold;
     }
}

int MIN_SEQUENCE_LENGTH = 2;

FlowableTransformer<Double, Double> trans = FlowableTransformers 
    .stateMachine() 
    .initialStateFactory(() -> new State(new ArrayList<>(), false))
    .<Double, Double> transition((state, t, subscriber) -> {
        if (t < 0) {
            if (state.reachedThreshold) {
                if (subscriber.isUnsubscribed()) {
                    return null;
                }
                subscriber.onNext(t);
                return state;
            } else if (state.list.size() == MIN_SEQUENCE_LENGTH - 1) {
                for (Double temperature : state.list) {
                    if (subscriber.isUnsubscribed()) {
                        return null;
                    }
                    subscriber.onNext(temperature);
                }
                return new State(null, true);
            } else {
                List<Double> list = new ArrayList<>(state.list);
                list.add(t);
                return new State(list, false);
            }
        } else {
            return new State(new ArrayList<>(), false);
        }
    }).build();
Flowable
    .just(10.4, 5.0, 2.0, -1.0, -2.0, -5.0, -1.0, 2.0, 5.0, 6.0)
    .compose(trans)
    .forEach(System.out::println);
```

