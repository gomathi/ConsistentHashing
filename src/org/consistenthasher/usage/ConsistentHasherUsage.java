package org.consistenthasher.usage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.consistenthasher.ConsistentHasher;
import org.consistenthasher.ConsistentHasherImpl;

/**
 * A simple class that explains how to use {@link ConsistentHasher}. Please read
 * the functions in top to bottom order.
 *
 */
public class ConsistentHasherUsage {

	/**
	 * {@link ConsistentHasher} stores both bucket names and members. It exposes
	 * set of methods to add/remove a bucket, add/remove member, list members of
	 * a bucket.
	 * 
	 * This method creates a consistent hasher where buckets and members are of
	 * Integer type. Also uses SHA1 hash function to generate consistent hashing
	 * id. Also uses virtual bucket size as value 1.
	 * 
	 * @return
	 */
	public static ConsistentHasher<Integer, Integer> createConsistentHasher() {
		ConsistentHasher<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				1, ConsistentHasher.getIntegerToBytesConverter(),
				ConsistentHasher.getIntegerToBytesConverter(),
				ConsistentHasher.SHA1);
		return cHasher;
	}

	/**
	 * Consistent hashing places buckets and members on a ring. Each member's
	 * corresponding bucket is found by walking clockwise on the ring, and
	 * whichever bucket comes first is the owner of the member.
	 * 
	 * If few buckets are placed closely on the ring, then the members
	 * distribution to buckets wont be fair. Sometimes a bucket may get almost
	 * 90% of the members. To avoid that, the ring is divided into fixed number
	 * of segments, and buckets are mapped to these segments. Hence each bucket
	 * will get almost close-to-equivalent number of members.
	 * 
	 * This implementation uses the concept of virtual nodes, where each bucket
	 * will be mapped to that many logical buckets. This helps in getting fair
	 * distribution of members to buckets.
	 * 
	 * {@link ConsistentHasherImpl} exposes set of utility methods using which
	 * we can readily figure out distribution of members to buckets for various
	 * virtual node size.
	 * 
	 * Following example, does the following things
	 * 
	 * 1) Creates consistent hasher with buckets and members as integer data
	 * type.
	 * 
	 * 2) Uses the utility method from {@link ConsistentHasherImpl} to find out
	 * distribution percentage for various virtual nodes size, and prints that
	 * stats. If you look at the output, with virtual node size as 1, the
	 * members distribution is unfair. But with virtual node size more than 700,
	 * member distribution is close to fair.
	 * 
	 */
	public static void printBucketDistribution() {
		Random random = new Random(System.currentTimeMillis());
		List<Integer> buckets = new ArrayList<>();
		for (int i = 0; i < 10; i++)
			buckets.add(random.nextInt(Integer.MAX_VALUE) / 5);
		List<Integer> members = new ArrayList<>();
		for (int i = 0; i < 10000; i++)
			members.add(random.nextInt());
		Map<Integer, Map<Double, Integer>> result = ConsistentHasherImpl
				.getDistributionPercentage(1, 800,
						ConsistentHasher.getIntegerToBytesConverter(),
						ConsistentHasher.getIntegerToBytesConverter(),
						ConsistentHasher.getSHA1HashFunction(), buckets,
						members);
		result.forEach((virtNodeId, map) -> {
			System.out.println("No of virt nodes : " + virtNodeId);
			System.out.printf("%5s%10s\n", "%", "BucketId");
			map.forEach((percent, barray) -> {
				System.out.printf("%5.2f %d\n", percent, barray);
			});
			System.out.println("\n\n");
		});
	}
}
