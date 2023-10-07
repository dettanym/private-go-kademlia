package normalizedrt

import (
	"fmt"
	"math"
	"sort"
)

func (rt *NormalizedRt[K, N]) pickAtRandomFromRecords(n int, peers []peerInfo[K, N]) []peerInfo[K, N] {
	perm := rt.rand.Perm(len(peers))
	topN := perm[:n]
	chosenPeers := make([]peerInfo[K, N], 0, n)
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
	numberOfSubBuckets := int(math.Pow(2, math.Abs(float64(ownBucketIndex-earlierBucketIndex))))
	subBuckets := make([][]peerInfo[K, N], numberOfSubBuckets)

	sort.SliceStable(records, func(i, j int) bool {
		distI := records[i].kadId.Xor(sampleBucketKey)
		distJ := records[j].kadId.Xor(sampleBucketKey)

		cmp := distI.Compare(distJ)
		if cmp != 0 {
			return cmp < 0
		}
		return false
	})

	smallestDistanceToOwnBucket := int(math.Pow(2, math.Abs(float64(sampleBucketKey.BitLen()-ownBucketIndex+earlierBucketIndex-1))))
	subBucketIndex := 0
	subBucketDistanceLowerLimit := smallestDistanceToOwnBucket
	distSpannedByEachSubBucket := int(math.Pow(2, math.Abs(float64(sampleBucketKey.BitLen()-ownBucketIndex))))
	fmt.Println(subBucketIndex, subBucketDistanceLowerLimit, distSpannedByEachSubBucket)
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
	// Must make a new copy since we can't fill up buckets with more nodes from other buckets.
	// That will mess up with future NearestNodes calls *as a client*.
	regularRT := rt

	if &queryingPeerKadId != nil {
		regularRT.RemoveKey(queryingPeerKadId)
	}

	// TODO: Add own key?

	for index := len(regularRT.buckets) - 1; index >= 0; index-- {
		bucket := regularRT.buckets[index]

		initialBucketSize := len(bucket)

		recordsFromHigherBuckets := regularRT.getRecordsFromHigherBucketIndices(
			rt.bucketSize-initialBucketSize,
			regularRT.buckets[index+1:])

		recordsFromLowerBuckets := regularRT.getRecordsFromLowerBucketIndices(
			rt.bucketSize-initialBucketSize-len(recordsFromHigherBuckets),
			regularRT.buckets[:index],
			index,
		)

		normalizedRecords := append(bucket, recordsFromHigherBuckets...)
		normalizedRecords = append(normalizedRecords, recordsFromLowerBuckets...)

		regularRT.buckets[index] = normalizedRecords
	}

	return regularRT.buckets
}

func (rt *NormalizedRt[K, N]) NormalizeRT(queryingPeerKadId K) (bucketsWithOnlyPeerIDs [][]N) {
	buckets := rt.normalizeRT(queryingPeerKadId)

	bucketsWithOnlyPeerIDs = rt.getPeerIDsFromBuckets(buckets)

	return bucketsWithOnlyPeerIDs
}

func (rt *NormalizedRt[K, N]) getPeerIDsFromBuckets(buckets [][]peerInfo[K, N]) [][]N {
	bucketsWithOnlyPeerIDs := make([][]N, len(buckets))
	for bid, peerInfos := range buckets {
		bucketsWithOnlyPeerIDs[bid] = make([]N, len(peerInfos))
		for _, peerInfo := range peerInfos {
			bucketsWithOnlyPeerIDs[bid] = append(bucketsWithOnlyPeerIDs[bid], peerInfo.id)
		}
	}

	return bucketsWithOnlyPeerIDs
}
