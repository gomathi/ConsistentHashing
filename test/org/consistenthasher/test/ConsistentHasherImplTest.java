package org.consistenthasher.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import junit.framework.Assert;

import org.consistenthasher.ConsistentHasher;
import org.consistenthasher.ConsistentHasher.BytesConverter;
import org.consistenthasher.ConsistentHasherImpl;
import org.junit.Test;

public class ConsistentHasherImplTest {

	@Test
	public void testConsistenHashingWithEmptyInput() {
		BytesConverter<Integer> intToBytesConverter = ConsistentHasher
				.getIntegerToBytesConverter();
		ConsistentHasherImpl<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				1, intToBytesConverter, intToBytesConverter, input -> input);
		List<Integer> members = cHasher.getMembersFor(1);
		Assert.assertNotNull(members);
		Assert.assertEquals(0, members.size());
	}

	@Test
	public void testConsistenHashingWithSingleBucketAndSingleMember()
			throws InterruptedException {
		BytesConverter<Integer> intToBytesConverter = ConsistentHasher
				.getIntegerToBytesConverter();
		ConsistentHasherImpl<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				1, intToBytesConverter, intToBytesConverter, input -> input);
		cHasher.addBucket(1);
		cHasher.addMember(1);
		List<Integer> members = cHasher.getMembersFor(1);
		Assert.assertNotNull(members);
		Assert.assertEquals(1, members.size());
		Assert.assertEquals(1, members.get(0).intValue());
		cHasher.removeBucket(1);
		members = cHasher.getMembersFor(1);
		Assert.assertNotNull(members);
		Assert.assertEquals(0, members.size());
	}

	@Test
	public void testRemoveBucketWithNoBuckets() throws InterruptedException {
		BytesConverter<Integer> intToBytesConverter = ConsistentHasher
				.getIntegerToBytesConverter();
		ConsistentHasherImpl<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				1, intToBytesConverter, intToBytesConverter, input -> input);
		cHasher.removeBucket(1);
		List<Integer> members = cHasher.getMembersFor(1);
		Assert.assertNotNull(members);
		Assert.assertEquals(0, members.size());
	}

	@Test
	public void testConsistentHashingWithMultipleBuckets() {
		BytesConverter<Integer> intToBytesConverter = ConsistentHasher
				.getIntegerToBytesConverter();
		ConsistentHasherImpl<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				1, intToBytesConverter, intToBytesConverter, input -> input);
		testBucketsAndMembers(cHasher);
	}

	@Test
	public void testConsistentHashingWithMultipleBucketsAndVirtualNodes() {
		BytesConverter<Integer> intToBytesConverter = ConsistentHasher
				.getIntegerToBytesConverter();
		ConsistentHasherImpl<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				800, intToBytesConverter, intToBytesConverter, input -> input);
		testBucketsAndMembers(cHasher);
	}

	@Test
	public void testGetAllBucketsWithEmptyInput() {
		BytesConverter<Integer> intToBytesConverter = ConsistentHasher
				.getIntegerToBytesConverter();
		ConsistentHasherImpl<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				1, intToBytesConverter, intToBytesConverter, input -> input);
		Assert.assertTrue(cHasher.getAllBuckets().isEmpty());
	}

	@Test
	public void testGetAllBuckets() {
		BytesConverter<Integer> intToBytesConverter = ConsistentHasher
				.getIntegerToBytesConverter();
		ConsistentHasherImpl<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				1, intToBytesConverter, intToBytesConverter, input -> input);
		List<Integer> expectedBuckets = Arrays.asList(1, 2, 3, 4, 5);
		expectedBuckets.stream().forEach(
				bucketNo -> cHasher.addBucket(bucketNo));
		List<Integer> actualBuckets = cHasher.getAllBuckets();
		Assert.assertEquals(actualBuckets, expectedBuckets);
	}

	@Test
	public void testGetAllMembersWithEmptyInput() {
		BytesConverter<Integer> intToBytesConverter = ConsistentHasher
				.getIntegerToBytesConverter();
		ConsistentHasherImpl<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				1, intToBytesConverter, intToBytesConverter, input -> input);
		Assert.assertTrue(cHasher.getAllMembers().isEmpty());
	}

	@Test
	public void testGetMembers() {
		BytesConverter<Integer> intToBytesConverter = ConsistentHasher
				.getIntegerToBytesConverter();
		ConsistentHasherImpl<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				1, intToBytesConverter, intToBytesConverter, input -> input);
		cHasher.addBucket(5);
		List<Integer> expectedMembers = Arrays.asList(1, 2, 3, 4, 5);
		List<Integer> actualMembers = cHasher.getMembersFor(5, expectedMembers);
		Assert.assertEquals(actualMembers, expectedMembers);
	}

	@Test
	public void testGetAllMembers() {
		BytesConverter<Integer> intToBytesConverter = ConsistentHasher
				.getIntegerToBytesConverter();
		ConsistentHasherImpl<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				1, intToBytesConverter, intToBytesConverter, input -> input);
		List<Integer> expectedMembers = Arrays.asList(1, 2, 3, 4, 5);
		expectedMembers.stream().forEach(
				memberNo -> cHasher.addMember(memberNo));
		List<Integer> actualMembers = cHasher.getAllMembers();
		Assert.assertEquals(actualMembers, expectedMembers);
	}

	@Test
	public void testRemoveMember() throws InterruptedException {
		BytesConverter<Integer> intToBytesConverter = ConsistentHasher
				.getIntegerToBytesConverter();
		ConsistentHasherImpl<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
				1, intToBytesConverter, intToBytesConverter, input -> input);
		cHasher.addBucket(5);
		cHasher.addMember(1);
		List<Integer> members = cHasher.getMembersFor(5);
		Assert.assertNotNull(members);
		Assert.assertEquals(members.size(), 1);
		Assert.assertEquals(members.get(0).intValue(), 1);
		cHasher.removeMember(1);
		members = cHasher.getMembersFor(5);
		Assert.assertNotNull(members);
		Assert.assertEquals(members.size(), 0);
	}

	private static void testBucketsAndMembers(
			ConsistentHasherImpl<Integer, Integer> cHasher) {
		int firstBucket = 5;
		int lastBucket = 20;
		int incrBucketSizeBy = 5;
		int totBuckets = lastBucket / incrBucketSizeBy;
		TreeSet<Integer> sortedBuckets = new TreeSet<>();
		Map<Integer, List<Integer>> expectedBucketsAndMembersMap = new HashMap<Integer, List<Integer>>();
		for (int i = firstBucket; i <= lastBucket; i = i + incrBucketSizeBy) {
			cHasher.addBucket(i);
			expectedBucketsAndMembersMap.put(i, new ArrayList<>());
			sortedBuckets.add(i);
		}
		int noOfMembers = 0;
		for (int i = 1; i <= 25; i++) {
			noOfMembers++;
			cHasher.addMember(i);
			int bucket = sortedBuckets.ceiling(i) == null ? sortedBuckets
					.first() : sortedBuckets.ceiling(i);
			expectedBucketsAndMembersMap.get(bucket).add(i);
		}
		Map<Integer, List<Integer>> bucketsAndMembersMap = cHasher
				.getAllBucketsToMembersMapping();
		Assert.assertNotNull(bucketsAndMembersMap);
		Assert.assertEquals(totBuckets, bucketsAndMembersMap.size());
		Assert.assertEquals(noOfMembers, bucketsAndMembersMap.values().stream()
				.mapToInt(a -> a.size()).sum());
		Assert.assertEquals(bucketsAndMembersMap, expectedBucketsAndMembersMap);
	}
}
