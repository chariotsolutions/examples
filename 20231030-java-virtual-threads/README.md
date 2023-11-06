This example is a [merge sort](https://en.wikipedia.org/wiki/Merge_sort) that uses threads to perform
its work. It was intended to demonstrate one strength of Java 21 virtual threads: the ability to have
an essentially unbounded number of threads, allowing simple implementation of "embarassingly parallel"
algorithms. As it turned out, [that wasn't quite what happened](https://chariotsolutions.com/blog/post/java-virtual-threads/).

The program uses three approaches to threading:

* "Inline" threads, in which each new task runs to completion when passed to `execute()` (ie, it's actually
  single-threaded).
* Fork-join threads, introduced in Java 7.
* Virtual threads, introduced in Java 21.

For each thread type, we run multiple iterations on random arrays, tracking wall-clock time taken per
iteration.

Note that you can't use "traditional" threads: the program spins up `N(log2 N)` threads for each
iteration, with "parent" threads blocking until their children complete. The operating system
simply can't support that that for any array larger than maybe 1,000 elements.

Note also that this program is intentionally not optimized: I want to compare the performance of
different threading implementations, not see how fast a merge sort can be. 


## Building and Running

You must have Java 21 installed.

Since the program is in the top-level package, and has no dependencies, you can compile directly:

```
javac ThreadSort.java
```

And run it just as easily, passing the size of the desired array:

```
java ThreadSort 1000000
```

It will take some time to run (we're actually running 10 iterations of each threadpool type in
each loop, for 60 total iterations), but you'll see output like the following:

```
virtual: time per iteration = 620 milliseconds
inline: time per iteration = 171 milliseconds
fork-join: time per iteration = 61 milliseconds
```
