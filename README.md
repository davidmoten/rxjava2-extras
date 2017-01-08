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
* [`StateMachine`](#transformers) - a more expressive form of `scan` that can emit multiple events for each source event
* [`Transformers`](#transformers)
* supports Java 1.6+

Status: *released to Maven Central*

Maven site reports are [here](http://davidmoten.github.io/rxjava2-extras/index.html) including [javadoc](http://davidmoten.github.io/rxjava2-extras/apidocs/index.html).

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
Builder for `.retryWhen()`

IO
-------------
`serverSocket(port)` 

FlowableTransformers
---------------------------
`doOnEmpty`

`mapLast`

`reverse`

`stateMachine`

`match`, `matchWith`

`onBackpressureBufferToFile`

SchedulerHelper
----------------
`blockUntilWorkFinished`

`withThreadId`

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


##FlowableTransformers.onBackpressureBufferToFile
With this operator you can offload as stream's emissions to disk to reduce memory pressure when you have a fast producer + slow consumer (or just to minimize memory usage).

<img src="https://raw.githubusercontent.com/davidmoten/rxjava-extras/master/src/docs/onBackpressureBufferToFile.png" />

If you have used the `onBackpressureBuffer` operator you'll know that when a stream is producing faster than the downstream operators can process (perhaps the producer cannot respond meaningfully to a *slow down* request from downstream) then `onBackpressureBuffer` buffers the items to an in-memory queue until they can be processed. Of course if memory is limited then some streams might eventually cause an `OutOfMemoryError`. One solution to this problem is to increase the effectively available memory for buffering by using disk instead (and small in-memory read/write buffers). That's why `Transformers.onBackpressureBufferToFile` was created. 

*rxjava-extras* uses standard file io to buffer serialized stream items. This operator can still be used with RxJava2 using the [RxJava2Interop](https://github.com/akarnokd/RxJava2Interop) library. 

*rxjava2-extras8* uses fixed size memory-mapped files to perform the same operation but with much greater throughput. 

Note that new files for a file buffered observable are created for each subscription and thoses files are in normal circumstances deleted on cancellation (triggered by `onCompleted`/`onError` termination or manual cancellation). 

Here's an example:

```java
// write the source strings to a 
// disk-backed queue on the subscription
// thread and emit the items read from 
// the queue on the computation() scheduler.
Flowable<String> source = 
  Flowable
    .just("a", "b", "c")
    .compose(
      FlowableTransformers.onBackpressureBufferToFile()
          .serializerUtf8())
```

This example does the same as above but more concisely and uses standard java IO serialization (normally it will be more efficient to write your own `DataSerializer`):

```java
Observable<String> source = 
  Observable
    .just("a", "b", "c")
    .compose(FlowableTransformers.<String>onBackpressureBufferToFile());
```

An example with a custom serializer:

```java
// define how the items in the source stream would be serialized
DataSerializer<String> serializer = new DataSerializer<String>() {

    @Override
    public void serialize(DataOutput output, String s) throws IOException {
        output.writeUTF(s);
    }

    @Override
    public String deserialize(DataInput input, int availableBytes) throws IOException {
        return input.readUTF();
    }
};
Flowable
  .just("a", "b", "c")
  .compose(
    FlowableTransformers.onBackpressureBufferToFile()
        .serializer(serializer));
  ...
```
You can configure various options:

```java
Flowable
  .just("a", "b", "c")
  .compose(
    FlowableTransformers.onBackpressureBufferToFile()
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
* `DataSerializers.byteArray()`
* `DataSerializers.javaIO()` - uses standard java serialization (`ObjectOutputStream` and such)

Using default java serialization you can buffer array lists of integers to a file like so:

```java
Flowable.just(1, 2, 3, 4)
    //accumulate into sublists of length 2
    .buffer(2)
    .compose(
      Transformers.<List<Integer>>onBackpressureBufferToFile().serializerJavaIO())
```

In the above example it's fortunate that `.buffer` emits `ArrayList<Integer>` instances which are serializable. To be strict you might want to `.map` the returned list to a data type you know is serializable:

```java
Flowable.just(1, 2, 3, 4)
    .buffer(2)
    .map(list -> new ArrayList<Integer>(list))
    .compose(
      Transformers.<List<Integer>>onBackpressureBufferToFile().serializerJavaIO())
```

###Performance
Throughput is increased dramatically by using memory-mapped files. 

*rxjava2-extras8* can push through 800MB/s using 1K messages compared to *rxjava-extras* 43MB/s. My 2016 2 core HP Spectre laptop with SSD pushes through up to 1.5GB/s.

Smaller messages mean more read/write/cancellation-check contention but still on my laptop I am seeing 6 million 40B messages per second.
