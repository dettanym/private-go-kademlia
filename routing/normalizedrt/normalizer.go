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

// TODO: Here previous buckets refers to closer buckets, but the RT is organized by CPL so it's the other way around.
func (rt *NormalizedRt[K, N]) getRecordsFromPreviousBuckets(n int, previousBuckets [][]peerInfo[K, N]) []peerInfo[K, N] {
	if n == 0 {
		return make([]peerInfo[K, N], 0)
	}

	previousRecords := rt.flattenBucketRecords(previousBuckets)

	if len(previousRecords) <= n {
		return previousRecords
	}

	return rt.pickAtRandomFromRecords(n, previousRecords)
}

// TODO: In this RT, own bucket max size can be given as a function of the CPL or the bucket ID.
func (rt *NormalizedRt[K, N]) getRecordsFromNextBuckets(n int, nextBuckets [][]peerInfo[K, N], ownBucketMaxSize int) []peerInfo[K, N] {

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