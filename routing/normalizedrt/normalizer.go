package normalizedrt

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
	normalizedRT := rt

	normalizedRT.RemoveKey(queryingPeerKadId)

	return normalizedRT.buckets
}

func (rt *NormalizedRt[K, N]) NormalizeRT(queryingPeerKadId K) (bucketsWithOnlyPeerIDs [][]N) {
	buckets := rt.normalizeRT(queryingPeerKadId)

	bucketsWithOnlyPeerIDs = rt.getPeerIDsFromBuckets(buckets)

	return bucketsWithOnlyPeerIDs
}

func (rt *NormalizedRt[K, N]) getPeerIDsFromBuckets(buckets [][]peerInfo[K, N]) [][]N {
	bucketsWithOnlyPeerIDs := make([][]N, 0)
	for bid, peerInfos := range buckets {
		bucketsWithOnlyPeerIDs[bid] = make([]N, 0)
		for _, peerInfo := range peerInfos {
			bucketsWithOnlyPeerIDs[bid] = append(bucketsWithOnlyPeerIDs[bid], peerInfo.id)
		}
	}

	return bucketsWithOnlyPeerIDs
}