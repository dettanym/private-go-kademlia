package normalizedrt

import (
	"fmt"
	"sort"
)

func (rt *NormalizedRt[K, N]) pickAtRandomFromRecords(n int, peers []peerInfo[K, N]) []peerInfo[K, N] {
	perm := rt.rand.Perm(len(peers))
	topN := perm[:n]
	chosenPeers := make([]peerInfo[K, N], n)
	for index, randomIndex := range topN {
		chosenPeers[index] = peers[randomIndex]
	}
	return chosenPeers
}

// Gets records from higher bucket indices in our RT,
// which have a *high* CPL with our node,
// so correspond to buckets with *closer* nodes.
func (rt *NormalizedRt[K, N]) getRecordsFromHigherBucketIndices(n int, higherBuckets [][]peerInfo[K, N]) []peerInfo[K, N] {
	if n == 0 {
		return make([]peerInfo[K, N], 0)
	}

	recordsHigherBuckets := rt.flattenBucketRecords(higherBuckets)

	if len(recordsHigherBuckets) <= n {
		return recordsHigherBuckets
	}

	return rt.pickAtRandomFromRecords(n, recordsHigherBuckets)
}

func (rt *NormalizedRt[K, N]) createSubBuckets(ownBucketIndex int, earlierBucketIndex int, sampleBucketKey K, records []peerInfo[K, N]) [][]peerInfo[K, N] {
	// numberOfSubBuckets := int(math.Pow(2, math.Abs(float64(ownBucketIndex-earlierBucketIndex))))
	// subBuckets := make([][]peerInfo[K, N], numberOfSubBuckets) //<- this is wrong? because some buckets may be different size.

	numberOfSubBuckets := 1
	// sorts records by distance to sampleBucketKey
	sort.SliceStable(records, func(i, j int) bool {
		distI := records[i].kadId.Xor(rt.self)
		distJ := records[j].kadId.Xor(rt.self)

		cmp := distI.Compare(distJ)
		if cmp != 0 {
			numberOfSubBuckets += 1
			return cmp < 0
		}
		return false
	})
	var subBuckets [][]peerInfo[K, N]

	recordIndexMin := 0
	currDist := records[0].kadId.Xor(rt.self)
	for i := 1; i < len(records); i++ {
		recordDist := records[i].kadId.Xor(rt.self)
		cmp := currDist.Compare(recordDist)
		if cmp != 0 {
			fmt.Println(records[recordIndexMin:i], recordIndexMin, i)
			subBuckets = append(subBuckets, records[recordIndexMin:i])
			currDist = recordDist
			recordIndexMin = i
		}
	}
	subBuckets = append(subBuckets, records[recordIndexMin:])

	// smallestDistanceToOwnBucket := int(math.Pow(2, math.Abs(float64(sampleBucketKey.BitLen()-ownBucketIndex+earlierBucketIndex-1))))
	// subBucketIndex := 0
	// subBucketDistanceLowerLimit := smallestDistanceToOwnBucket
	// distSpannedByEachSubBucket := int(math.Pow(2, math.Abs(float64(sampleBucketKey.BitLen()-ownBucketIndex))))
	// fmt.Println(subBucketIndex, subBucketDistanceLowerLimit, distSpannedByEachSubBucket)

	//for _, record := range records {
	//	recordDist := record.kadId.Xor(sampleBucketKey)
	//	// Compare  recordDist to subBucketDistanceLowerLimit and distSpannedByEachSubBucket
	//
	//}
	// No. of nodes at max in ownBucket, ignoring bucketSize = 2^(255-ownBucketIndex)
	// No. of nodes at max in a lower subBucket ownBucketIndex -j = 2^(255-ownBucketIndex) * 2^j
	// No. of subbuckets = 2^j
	// Each subbucket from lowerBuckets has indices 0 to ownBucketIndex - 1
	// So iterate over them in reverse, get ownBucketIndex - 1, ownBucketIndex - 2... 0
	// Got to put all nodes that are within a certain distance (calculate) into first subbucket, rest into second subbucket etc.
	fmt.Println(sampleBucketKey, records)
	fmt.Println(subBuckets)
	return subBuckets
}

// Gets records from lower bucket indices in our RT,
// which have a *low* CPL with our node,
// so correspond to buckets with *farther* nodes.
func (rt *NormalizedRt[K, N]) getRecordsFromLowerBucketIndices(n int, lowerBuckets [][]peerInfo[K, N], ownBucketIndex int) []peerInfo[K, N] {
	accOuter := make([]peerInfo[K, N], 0)

	for i := len(lowerBuckets) - 1; i >= 0; i-- {
		if len(accOuter) == n {
			return accOuter
		}

		bucket := lowerBuckets[i]
		if len(bucket)+len(accOuter) <= n {
			// If we can add all of this bucket, without exceeding n
			// Then add it
			accOuter = append(accOuter, bucket...)
		} else {
			// Can only add some records from this bucket
			maxSubBucketsLen := n - len(accOuter)
			accInner := make([]peerInfo[K, N], maxSubBucketsLen)

			// First create subbuckets
			// Each subbucket contains nodes that are equidistant to nodes from our bucket,
			// given just the prefix of our bucket
			sampleBucketKey := bucket[0].kadId
			subBuckets := rt.createSubBuckets(ownBucketIndex, i, sampleBucketKey, bucket)

			// Pick upto maxSubBucketsLen elements from subBuckets
			for j := len(subBuckets) - 1; j >= 0; j-- {
				if len(subBuckets[j]) <= maxSubBucketsLen-len(accInner) {
					// Can add next subbucket entirely (without accInner exceeding maxSubBucketsLen)
					accInner = append(accInner, subBuckets[j]...)
				} else {
					// Can't add next subbucket entirely (accInner exceeds maxSubBucketsLen)
					// Pick elements at random from next subbucket
					// (since without knowing the full CID of the target in a given non-full bucket,
					// we can't pick closest nodes from other buckets more precisely).
					subSubBucket := rt.pickAtRandomFromRecords(maxSubBucketsLen-len(accInner), subBuckets[j])
					accInner = append(accInner, subSubBucket...)
				}
			}
			accOuter = append(accOuter, accInner...)
		}
	}
	return accOuter
}

func (rt *NormalizedRt[K, N]) flattenBucketRecords(buckets [][]peerInfo[K, N]) []peerInfo[K, N] {
	chosenPeers := make([]peerInfo[K, N], 0)

	for index := len(buckets) - 1; index >= 0; index-- {
		bucket := buckets[index]
		for _, kadIDPeerIDRecord := range bucket {
			chosenPeers = append(chosenPeers, kadIDPeerIDRecord)
		}
	}

	return chosenPeers
}

// Returns a normalized RT for PIR.
func (rt *NormalizedRt[K, N]) normalizeRT(queryingPeerKadId K) [][]peerInfo[K, N] {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if &queryingPeerKadId != nil {
		rt.initializeNormalizedBucketsForClient(queryingPeerKadId)
	}

	// TODO: Add own key?
	for index := len(rt.buckets) - 1; index >= 0; index-- {
		bucket := rt.buckets[index]

		initialBucketSize := len(bucket)

		recordsFromHigherBuckets := rt.getRecordsFromHigherBucketIndices(
			rt.bucketSize-initialBucketSize,
			rt.buckets[index+1:])

		recordsFromLowerBuckets := rt.getRecordsFromLowerBucketIndices(
			rt.bucketSize-initialBucketSize-len(recordsFromHigherBuckets),
			rt.buckets[:index],
			index,
		)

		normalizedRecords := append(bucket, recordsFromHigherBuckets...)
		normalizedRecords = append(normalizedRecords, recordsFromLowerBuckets...)

		rt.normalizedBuckets[index] = normalizedRecords
	}

	return rt.normalizedBuckets
}

func (rt *NormalizedRt[K, N]) NormalizeRT(queryingPeerKadId K) (bucketsWithOnlyPeerIDs [][]N) {
	var normalizedBuckets [][]peerInfo[K, N]
	if rt.peersAddedSinceNormalization {
		normalizedBuckets = rt.normalizeRT(queryingPeerKadId)
	} else {
		normalizedBuckets = rt.normalizedBuckets
	}

	bucketsWithOnlyPeerIDs = rt.getPeerIDsFromBuckets(normalizedBuckets)

	return bucketsWithOnlyPeerIDs
}

func (rt *NormalizedRt[K, N]) getPeerIDsFromBuckets(buckets [][]peerInfo[K, N]) [][]N {
	bucketsWithOnlyPeerIDs := make([][]N, len(buckets))
	for bid, peerInfos := range buckets {
		for _, peerInfo := range peerInfos {
			bucketsWithOnlyPeerIDs[bid] = append(bucketsWithOnlyPeerIDs[bid], peerInfo.id)
		}
	}

	return bucketsWithOnlyPeerIDs
}
