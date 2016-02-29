# ConsistentHashing
Contains the implementation of consistent hashing.  

Consistent hashing tries to reduce the no of elements that are getting rehashed while a bucket addition/removal happens. There are great documents available on the Internet, I dont want to repeat it here. For newcomers, read [Consistent hashing on Wikipedia](http://en.wikipedia.org/wiki/Consistent_hashing)

### Problem

In one of my projects I was trying to divide sync tasks among set of worker nodes. In case, if a worker node dies, then other nodes have to take care of the dead node's tasks as well. A worker node addition/removal should not cause high disruption to the system, otherwise lot of sync clients will complain. Consistent hashing was the solution, as it reduces the no of elements that are getting remapped. 

I was looking for a consistent hashing library that provides the following functions

1. Store bucket ids.
2. Store members.
3. Retrieve all members that fall into a particular bucket, given the bucket name.
4. Implementation should have virtual buckets concept, so members to bucket distribution will be even. (More on this on the following document, and for the impatient [click here](https://github.com/gomathi/ConsistentHashing#virtual-nodes))
5. A generic interface, so that any kind of buckets (string, integer, custom datatype), and any kind of members can be stored.

Unfortunately I could not find any opensource project that provided the above abilities. Guava library has a [simple consistent hashing function] (http://docs.guava-libraries.googlecode.com/git-history/release18/javadoc/com/google/common/hash/Hashing.html#consistentHash(com.google.common.hash.HashCode, int)) which does not satisfy the above requirements.

Hence I wrote a library to provide those abilities.

### Usage

A consistent hashing object can be created as following

```
ConsistentHasher<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				1, ConsistentHasher.getIntegerToBytesConverter(),
				ConsistentHasher.getIntegerToBytesConverter(),
				ConsistentHasher.SHA1);
```

The above object defines a consistent hasher with bucket type as Integer type, also member type(members are the ones which are going to be stored on the buckets) as Integer type. Since buckets and members can be of any data type, hasher requires byte converter objects to convert buckets and members into corresponding byte array. Also each byte array is hashed by sha1 in order to uniformly place it on the consistent hashing ring. Here I am using virtual nodes size as 1.

The supported operations by the consistent hasher are available at [here](https://github.com/gomathi/ConsistentHashing/blob/master/src/org/consistenthasher/ConsistentHasher.java)

Also defined a usage class at [here](https://github.com/gomathi/ConsistentHashing/blob/master/src/org/consistenthasher/usage/ConsistentHasherUsage.java)

### Client downloads

1. [Standalone jar file] (https://github.com/gomathi/ConsistentHashing/blob/master/jars/consistenthash-standalone.jar)
2. [Jar file with dependencies] (https://github.com/gomathi/ConsistentHashing/blob/master/jars/consistenthash.jar)

### Virtual nodes

Consistent hashing places buckets and members on a ring. Each member's corresponding bucket is found by walking clockwise on the ring, and whichever bucket comes first is the owner of the member. 

If few buckets are placed closely on the ring, then the members distribution to buckets wont be fair. Sometimes a bucket may get almost 90% of the members. To avoid that, the ring is divided into fixed number of segments, and buckets are mapped to these segments. Hence each bucket will get almost close-to-equivalent number of members. 

This implementation uses the concept of virtual nodes, where each bucket will be mapped to many logical buckets. This helps in getting fair distribution of members to buckets.

When you create the consistent hasher, you can specify the virtual nodes size. I got a very unfair distribution for virtual nodes size lesser than 100 for some test sets. I got a good distribution when virtual nodes size are greater than 700. [Output file is available here](https://github.com/gomathi/ConsistentHashing/blob/master/jars/distribution-test-output.txt). Also [ConsistentHasherImpl](https://github.com/gomathi/ConsistentHashing/blob/master/src/org/consistenthasher/ConsistentHasherImpl.java) has set of static methods to calculate the distribution for various virtual nodes size. You can look at [Usage class](https://github.com/gomathi/ConsistentHashing/blob/master/src/org/consistenthasher/usage/ConsistentHasherUsage.java) how to use distribution calculation methods.


