package org.consistenthasher;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

/**
 * Implementation of {@link ConsistentHasher}.
 * 
 * {@link #addBucket(Object)}, {@link #addMember(Object)} and
 * {@link #removeMember(Object)} operations do not use any lock, should be
 * executed instantly.
 * 
 * {@link #removeBucket(Object)},
 * {@link #tryRemoveBucket(Object, long, TimeUnit)},
 * {@link #getAllBucketsToMembersMapping()} and {@link #getMembersFor(Object)}
 * are using a lock.
 * 
 * @param <B>
 *            bucket type.
 * @param <M>
 *            member type.
 */
@ThreadSafe
public class ConsistentHasherImpl<B, M> implements ConsistentHasher<B, M> {

	/**
	 * The value 700 for virtual instances provides better distribution among
	 * the buckets.
	 */
	public static final int VIRTUAL_INSTANCES_PER_BUCKET = 700;

	private final int startVirtualNodeId, stopVirtualNodeId;
	private final HashFunction hashFunction;
	private final BytesConverter<B> bucketDataToBytesConverter;
	private final BytesConverter<M> memberDataToBytesConverter;

	private final NavigableMap<ByteBuffer, B> bucketsMap;
	private final ConcurrentMap<B, BucketInfo> bucketsAndLocks;
	private final NavigableMap<ByteBuffer, M> membersMap;

	/**
	 * Contains a lock for the bucket, and also contains all virtual bucket
	 * names.
	 * 
	 * Lock is used to achieve atomicity while doing operations on the bucket.
	 *
	 */
	private static class BucketInfo {
		private final ReadWriteLock rwLock;
		// Acts as a cache, and used while listing members of the actual bucket.
		private final List<ByteBuffer> virtBuckets;

		public BucketInfo(ReadWriteLock rwLock, List<ByteBuffer> virtBuckets) {
			this.rwLock = rwLock;
			this.virtBuckets = virtBuckets;
		}
	}

	/**
	 * Creates a consistent hashing ring with the specified hashconverter and
	 * bytesconverter object.
	 * 
	 * @param virtualInstancesPerBucket
	 *            , creates specified number of virtual buckets for each bucket.
	 *            More the virtual instances, more the equal distribution among
	 *            buckets, should be greater than zero, otherwise value 1 is
	 *            used.
	 * @param bucketDataToBytesConverter
	 * @param memberDataToBytesConverter
	 * @param hashFunction
	 *            , is used to hash the given bucket and member.
	 */
	public ConsistentHasherImpl(int virtualInstancesPerBucket,
			BytesConverter<B> bucketDataToBytesConverter,
			BytesConverter<M> memberDataToBytesConverter,
			HashFunction hashFunction) {
		Preconditions.checkNotNull(bucketDataToBytesConverter,
				"Bucket data converter can not be null.");
		Preconditions.checkNotNull(memberDataToBytesConverter,
				"Member data converter can not be null.");
		Preconditions.checkNotNull(hashFunction,
				"HashFunction can not be null.");
		this.bucketsMap = new ConcurrentSkipListMap<>();
		this.bucketsAndLocks = new ConcurrentHashMap<>();
		this.membersMap = new ConcurrentSkipListMap<>();
		this.startVirtualNodeId = 1;
		this.stopVirtualNodeId = (virtualInstancesPerBucket > 0) ? virtualInstancesPerBucket
				: 1;
		this.hashFunction = hashFunction;
		this.bucketDataToBytesConverter = bucketDataToBytesConverter;
		this.memberDataToBytesConverter = memberDataToBytesConverter;
	}

	private ByteBuffer convertAndApplyHash(int virtualNode, B bucketName) {
		byte[] bucketNameInBytes = bucketDataToBytesConverter
				.convert(bucketName);
		byte[] bucketNameAndCode = new byte[Integer.BYTES / Byte.BYTES
				+ bucketNameInBytes.length];
		ByteBuffer bb = ByteBuffer.wrap(bucketNameAndCode);
		bb.put(bucketNameInBytes);
		bb.putInt(virtualNode);
		return ByteBuffer.wrap(hashFunction.hash(bucketNameAndCode));
	}

	@Override
	public void addBucket(B bucketName) {
		Preconditions.checkNotNull(bucketName, "Bucket name can not be null");
		List<ByteBuffer> virtBuckets = new ArrayList<>();
		for (int vitrualNodeId = startVirtualNodeId; vitrualNodeId <= stopVirtualNodeId; vitrualNodeId++) {
			ByteBuffer virtBucket = convertAndApplyHash(vitrualNodeId,
					bucketName);
			bucketsMap.put(virtBucket, bucketName);
			virtBuckets.add(virtBucket);
		}
		bucketsAndLocks.putIfAbsent(bucketName, new BucketInfo(
				new ReentrantReadWriteLock(), virtBuckets));
	}

	@Override
	public void removeBucket(B bucketName) throws InterruptedException {
		removeBucket(bucketName, 0, null, false);
	}

	@Override
	public boolean tryRemoveBucket(B bucketName, long timeout, TimeUnit unit)
			throws InterruptedException {
		return removeBucket(bucketName, timeout, unit, true);
	}

	private boolean removeBucket(B bucketName, long timeout, TimeUnit unit,
			boolean tryLock) throws InterruptedException {
		Preconditions.checkNotNull(bucketName, "Bucket name can not be null");
		BucketInfo bucketInfo = bucketsAndLocks.remove(bucketName);
		if (bucketInfo == null)
			return true;
		ReadWriteLock rwLock = bucketInfo.rwLock;
		boolean result = false;
		try {
			if (tryLock)
				result = rwLock.writeLock().tryLock(timeout, unit);
			else {
				rwLock.writeLock().lock();
				result = true;
			}
			if (result)
				for (ByteBuffer virtNode : bucketInfo.virtBuckets)
					bucketsMap.remove(virtNode);
		} finally {
			if (result)
				rwLock.writeLock().unlock();
		}
		return result;
	}

	private ByteBuffer convertAndApplyHash(M memberName) {
		return ByteBuffer.wrap(hashFunction.hash(memberDataToBytesConverter
				.convert(memberName)));
	}

	@Override
	public void addMember(M memberName) {
		Preconditions.checkNotNull(memberName, "Member name can not be null");
		membersMap.put(convertAndApplyHash(memberName), memberName);
	}

	@Override
	public void removeMember(M memberName) {
		Preconditions.checkNotNull(memberName, "Member name can not be null");
		membersMap.remove(convertAndApplyHash(memberName));
	}

	@Override
	public List<M> getMembersFor(B bucketName, List<? extends M> members) {
		Preconditions.checkNotNull(members, "Members can not be null.");
		NavigableMap<ByteBuffer, M> localMembersMap = new TreeMap<>();
		members.forEach(member -> {
			localMembersMap.put(convertAndApplyHash(member), member);
		});
		return getMembersInternal(bucketName, localMembersMap);
	}

	@Override
	public List<M> getMembersFor(B bucketName) {
		return getMembersInternal(bucketName, membersMap);
	}

	private List<M> getMembersInternal(B bucketName,
			NavigableMap<ByteBuffer, M> members) {
		Preconditions.checkNotNull(bucketName);
		BucketInfo bInfo = bucketsAndLocks.get(bucketName);
		if (bInfo == null)
			return Collections.emptyList();
		ReadWriteLock rwLock = bInfo.rwLock;
		List<M> result = new ArrayList<>();
		try {
			rwLock.readLock().lock();
			if (bucketsAndLocks.containsKey(bucketName)) {
				for (ByteBuffer currNode : bInfo.virtBuckets) {
					ByteBuffer prevNode = bucketsMap.lowerKey(currNode);
					if (prevNode == null) {
						result.addAll(members.headMap(currNode, true).values());
						Optional<ByteBuffer> lastKey = getLastKey(bucketsMap);
						if (lastKey.isPresent()
								&& !lastKey.get().equals(currNode))
							result.addAll(members.tailMap(lastKey.get(), false)
									.values());
					} else {
						result.addAll(members.subMap(prevNode, false, currNode,
								true).values());
					}
				}
			}
		} finally {
			rwLock.readLock().unlock();
		}
		return result;
	}

	@Override
	public Map<B, List<M>> getAllBucketsToMembersMapping() {
		Map<B, List<M>> result = new HashMap<>();
		for (B bucket : bucketsAndLocks.keySet()) {
			List<M> members = getMembersFor(bucket);
			result.put(bucket, members);
		}
		return result;
	}

	@Override
	public List<B> getAllBuckets() {
		return new ArrayList<B>(bucketsAndLocks.keySet());
	}

	@Override
	public List<M> getAllMembers() {
		return new ArrayList<>(membersMap.values());
	}

	/**
	 * Calculates the distribution of members to buckets for various virtual
	 * nodes, and returns distribution buckets and corresponding members list
	 * for each virtual node in a map.
	 * 
	 * @param startVirtNodeId
	 * @param endVirtNodeId
	 * @param bucketDataToBytesConverter
	 * @param memberDataToBytesConverter
	 * @param hashFunction
	 * @param buckets
	 * @param members
	 * @return map of virtual node ids and corresponding distribution map. Value
	 *         contains a map of bucket names and corresponding members.
	 */
	public static <B, M> Map<Integer, Map<B, List<M>>> getDistribution(
			int startVirtNodeId, int endVirtNodeId,
			BytesConverter<B> bucketDataToBytesConverter,
			BytesConverter<M> memberDataToBytesConverter,
			HashFunction hashFunction, List<B> buckets, List<M> members) {
		Map<Integer, Map<B, List<M>>> result = new HashMap<>();
		for (int virtNodeId = startVirtNodeId; virtNodeId <= endVirtNodeId; virtNodeId++) {
			ConsistentHasher<B, M> cHasher = new ConsistentHasherImpl<>(
					virtNodeId, bucketDataToBytesConverter,
					memberDataToBytesConverter, hashFunction);
			buckets.stream().forEach(
					bucketName -> cHasher.addBucket(bucketName));
			members.stream().forEach(
					memberName -> cHasher.addMember(memberName));
			Map<B, List<M>> distribution = cHasher
					.getAllBucketsToMembersMapping();
			result.put(virtNodeId, distribution);
		}
		return result;
	}

	/**
	 * Calculates the distribution of members to buckets for various virtual
	 * nodes, and returns distribution of buckets and corresponding count for
	 * each virtual node in a map.
	 * 
	 * @param startVirtNodeId
	 * @param endVirtNodeId
	 * @param bucketDataToBytesConverter
	 * @param memberDataToBytesConverter
	 * @param hashFunction
	 * @param buckets
	 * @param members
	 * @return map of virtual node ids and corresponding distribution map. Value
	 *         contains a map of bucket names and corresponding members size.
	 */
	public static <B, M> Map<Integer, Map<Integer, B>> getDistributionCount(
			int startVirtNodeId, int endVirtNodeId,
			BytesConverter<B> bucketDataToBytesConverter,
			BytesConverter<M> memberDataToBytesConverter,
			HashFunction hashFunction, List<B> buckets, List<M> members) {
		Map<Integer, Map<B, List<M>>> distribution = getDistribution(
				startVirtNodeId, endVirtNodeId, bucketDataToBytesConverter,
				memberDataToBytesConverter, hashFunction, buckets, members);
		Map<Integer, Map<Integer, B>> result = new TreeMap<>();
		distribution.forEach((vnSize, map) -> {
			Map<Integer, B> pResult = new TreeMap<>();
			map.forEach((b, list) -> {
				pResult.put(list.size(), b);
			});
			result.put(vnSize, pResult);
		});
		return result;
	}

	/**
	 * Calculates the distribution of members to buckets for various virtual
	 * nodes, and returns distribution of buckets and corresponding percentage
	 * for each virtual node in a map.
	 * 
	 * @param startVirtNodeId
	 * @param endVirtNodeId
	 * @param bucketDataToBytesConverter
	 * @param memberDataToBytesConverter
	 * @param hashFunction
	 * @param buckets
	 * @param members
	 * @return map of virtual node ids and corresponding distribution map. Value
	 *         contains a map of bucket names and corresponding percentage of
	 *         members.
	 */
	public static <B, M> Map<Integer, Map<Double, B>> getDistributionPercentage(
			int startVirtNodeId, int endVirtNodeId,
			BytesConverter<B> bucketDataToBytesConverter,
			BytesConverter<M> memberDataToBytesConverter,
			HashFunction hashFunction, List<B> buckets, List<M> members) {
		Map<Integer, Map<B, List<M>>> distribution = getDistribution(
				startVirtNodeId, endVirtNodeId, bucketDataToBytesConverter,
				memberDataToBytesConverter, hashFunction, buckets, members);
		Map<Integer, Map<Double, B>> result = new TreeMap<>();
		distribution.forEach((vnSize, map) -> {
			Map<Double, B> pResult = new TreeMap<>();
			map.forEach((b, list) -> {
				double percentage = ((double) list.size() / (double) members
						.size()) * 100;
				pResult.put(percentage, b);
			});
			result.put(vnSize, pResult);
		});
		return result;
	}

	/**
	 * {@link NavigableMap#lastKey()} throws {@link NoSuchElementException} in
	 * case if the map is empty. This function just wraps up the lastKey if the
	 * value is present, or null inside the Optional and returns the result.
	 * 
	 * @param map
	 * @return
	 */
	private static <T> Optional<T> getLastKey(NavigableMap<T, ?> map) {
		T key = null;
		try {
			if (!map.isEmpty())
				key = map.lastKey();
		} catch (NoSuchElementException e) {
			// Intentionally ignored.
		}
		Optional<T> result = Optional.ofNullable(key);
		return result;
	}
}
