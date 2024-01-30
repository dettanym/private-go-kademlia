package normalizedrt

import (
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

	// numberOfSubBuckets := 1
	// sorts records by distance to sampleBucketKey
	sort.SliceStable(records, func(i, j int) bool {
		distI := records[i].kadId.Xor(rt.self)
		distJ := records[j].kadId.Xor(rt.self)

		cmp := distI.Compare(distJ)
		if cmp != 0 {
			// numberOfSubBuckets += 1
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
			subBuckets = append(subBuckets, records[recordIndexMin:i])
			currDist = recordDist
			recordIndexMin = i
		}
	}
	subBuckets = append(subBuckets, records[recordIndexMin:])

	return subBuckets
}

// Gets records from lower bucket indices in our RT,
// which have a *low* CPL with our node,
// so correspond to buckets with *farther* nodes.
func (rt *NormalizedRt[K, N]) getRecordsFromLowerBucketIndices(n int, lowerBuckets [][]peerInfo[K, N], ownBucketIndex int) []peerInfo[K, N] {
	accOuter := make([]peerInfo[K, N], 0)

	// Start from closest one and then move all the way up to CPL = i = 0
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
			var accInner []peerInfo[K, N]

			// First create subbuckets
			// Each subbucket contains nodes that are equidistant to nodes from our bucket,
			// given just the prefix of our bucket
			sampleBucketKey := bucket[0].kadId
			subBuckets := rt.createSubBuckets(ownBucketIndex, i, sampleBucketKey, bucket)

			// Pick upto maxSubBucketsLen elements from subBuckets
			for j := 0; j < len(subBuckets); j++ {
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
		chosenPeers = append(chosenPeers, bucket...)
		// for _, kadIDPeerIDRecord := range bucket {
		// 	chosenPeers = append(chosenPeers, kadIDPeerIDRecord)
		// }
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

	var normalizedRecords []peerInfo[K, N]
	// TODO: Add own key?
	for index := len(rt.buckets) - 1; index >= 0; index-- {
		normalizedRecords = []peerInfo[K, N]{} // reset normalizedRecords
		bucket := rt.buckets[index]

		initialBucketSize := len(bucket)
		normalizedRecords = append(normalizedRecords, bucket...)

		recordsFromHigherBuckets := rt.getRecordsFromHigherBucketIndices(
			rt.bucketSize-initialBucketSize,
			rt.buckets[index+1:])

		recordsFromLowerBuckets := rt.getRecordsFromLowerBucketIndices(
			rt.bucketSize-initialBucketSize-len(recordsFromHigherBuckets),
			rt.buckets[:index],
			index,
		)

		normalizedRecords = append(normalizedRecords, recordsFromHigherBuckets...)
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
