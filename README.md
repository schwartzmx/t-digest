# t-digest
scala t-digest (using `mutable.TreeMap`)

Implementation based off of [Tedd Dunning's Java Source](https://github.com/tdunning/t-digest), the [Original TDigest paper](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf), and implementations in other languages.



##### Running 
```sh
sbt package
scala\
    -classpath target/scala-2.12/tdigest_2.12-0.1.jar\
    com.schwartzmx.tdigest.TDigest
```