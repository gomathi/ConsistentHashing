package org.consistenthasher;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;

/**
 * Defines consistent hash interface. Consistent hasher tries to reduce the
 * number of values that are getting rehashed while new bucket addition/removal.
 * More information is available on {@link http
 * ://en.wikipedia.org/wiki/Consistent_hashing}.
 * 
 * Defined the interface, so that methods will be clear, rather than being
 * buried inside the implementation.
 *
 * @param <B>
 * @param <M>
 */
public interface ConsistentHasher<B, M> {

	/**
	 * Adds the bucket.
	 * 
	 * @param bucketName
	 * @throws NullPointerException
	 *             , if the given argument is null.
	 */
	void addBucket(B bucketName);

	/**
	 * Removes the bucket. There can be virtual nodes for given a bucket.
	 * Removing a bucket, and listing the members of a bucket should be executed
	 * atomically, otherwise {@link #getMembersFor(Object)} might return partial
	 * members of the given bucket. To avoid that, a lock is used on every
	 * physical bucket. If there is a call {@link #getMembersFor(Object)}
	 * getting executed, then this method waits until all those threads to
	 * finish. In worst case this function might wait for the lock for longer
	 * period of time if multiple readers are using the same lock, and if you
	 * want to return in fixed amount of time then use
	 * {@link #tryRemoveBucket(Object, long, TimeUnit)}
	 * 
	 * @param bucketName
	 * @throws NullPointerException
	 *             , if the given argument is null.
	 */
	void removeBucket(B bucketName) throws InterruptedException;

	/**
	 * Similar to {@link #removeBucket(Object)}, except that this function
	 * returns within the given timeout value.
	 * 
	 * @param bucketName
	 * @param timeout
	 * @param unit
	 * @throws NullPointerException
	 *             , if the given argument is null.
	 */
	boolean tryRemoveBucket(B bucketName, long timeout, TimeUnit unit)
			throws InterruptedException;

	/**
	 * Adds member to the consistent hashing ring.
	 * 
	 * @param memberName
	 * @throws NullPointerException
	 *             , if the given argument is null.
	 */
	void addMember(M memberName);

	/**
	 * Removes member from the consistent hashing ring.
	 * 
	 * @param memberName
	 * @throws NullPointerException
	 *             , if the given argument is null.
	 */
	void removeMember(M memberName);

	/**
	 * Returns all the members that belong to the given bucket. If there is no
	 * such bucket returns an empty list.
	 * 
	 * @param bucketName
	 * @return
	 * @throws NullPointerException
	 *             , if the given argument is null.
	 */
	List<M> getMembersFor(B bucketName);

	/**
	 * Returns all the buckets and corresponding members of that buckets.
	 * 
	 * @return, if there are buckets and members, otherwise returns empty map.
	 * 
	 */
	Map<B, List<M>> getAllBucketsToMembersMapping();

	/**
	 * Returns all buckets that are stored. If there are no buckets, returns an
	 * empty list.
	 * 
	 * @return
	 */
	List<B> getAllBuckets();

	/**
	 * This fetches the members for the given bucket from the given members
	 * list. This method does not use the members which are already stored on
	 * this instance.
	 * 
	 * @param bucketName
	 * @param members
	 * @return
	 * @throws NullPointerException
	 *             , if the given argument is null.
	 */
	List<M> getMembersFor(B bucketName, List<M> members);

	/**
	 * Returns all members that are stored. If there are no members, returns an
	 * empty list.
	 * 
	 * @return
	 */
	List<M> getAllMembers();

	/**
	 * Converts the given data into bytes. Implementation should be thread safe.
	 *
	 * @param <T>
	 */
	public static interface BytesConverter<T> {
		byte[] convert(T data);
	}

	/**
	 * Converts the given data into bytes. Implementation should be thread safe.
	 *
	 */
	public static interface HashFunction {
		byte[] hash(byte[] input);
	}

	// Helper implementations

	public static final HashFunction SHA1 = new SHA1HashFunction();

	public static HashFunction getSHA1HashFunction() {
		return SHA1;
	}

	/**
	 * Returned object is thread safe.
	 * 
	 * @return
	 */
	public static BytesConverter<String> getStringToBytesConverter() {
		return new BytesConverter<String>() {

			@Override
			public byte[] convert(String data) {
				Preconditions.checkNotNull(data);
				return data.getBytes();
			}
		};
	}

	@ThreadSafe
	public static class SHA1HashFunction implements HashFunction {

		@Override
		public byte[] hash(byte[] input) {
			Preconditions.checkNotNull(input);
			return Hashing.sha1().hashBytes(input).asBytes();
		}
	}

	/**
	 * Returned object is thread safe.
	 * 
	 * @return
	 */
	public static BytesConverter<Integer> getIntegerToBytesConverter() {
		return new BytesConverter<Integer>() {

			@Override
			public byte[] convert(Integer input) {
				byte[] inputBytes = new byte[Integer.BYTES / Byte.BYTES];
				ByteBuffer bb = ByteBuffer.wrap(inputBytes);
				bb.putInt(input);
				return inputBytes;
			}
		};
	}
}
