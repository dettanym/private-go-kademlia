package normalizedrt

import "math"

func (rt *NormalizedRt[K, N]) pickAtRandomFromRecords(n int, peers []peerInfo[K, N]) []peerInfo[K, N] {
	perm := rt.rand.Perm(len(peers))
	topN := perm[:n]
	chosenPeers := make([]peerInfo[K, N], 0, len(topN))
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

// Gets records from lower bucket indices in our RT,
// which have a *low* CPL with our node,
// so correspond to buckets with *farther* nodes.
func (rt *NormalizedRt[K, N]) getRecordsFromLowerBucketIndices(n int, nextBuckets [][]peerInfo[K, N], ownBucketMaxSize int) []peerInfo[K, N] {
	return nextBuckets[0]
}

func (rt *NormalizedRt[K, N]) flattenBucketRecords(buckets [][]peerInfo[K, N]) []peerInfo[K, N] {
	chosenPeers := make([]peerInfo[K, N], 0)

	for _, bucket := range buckets {
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
