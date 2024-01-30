package normalizedrt

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/plprobelab/go-kademlia/libp2p"

	kt "github.com/plprobelab/go-kademlia/internal/kadtest"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/stretchr/testify/require"
)

var rng = rand.New(rand.NewSource(1337))

func zeroBytes(n int) []byte {
	bytes := make([]byte, n)
	for i := 0; i < n; i++ {
		bytes[i] = 0
	}
	return bytes
}

var (
	key0  = key.NewKey256(zeroBytes(32))                          // 000000...000
	key1  = key.NewKey256(append([]byte{0x40}, zeroBytes(31)...)) // 010000...000
	key2  = key.NewKey256(append([]byte{0x80}, zeroBytes(31)...)) // 100000...000
	key3  = key.NewKey256(append([]byte{0xc0}, zeroBytes(31)...)) // 110000...000
	key4  = key.NewKey256(append([]byte{0xe0}, zeroBytes(31)...)) // 111000...000
	key5  = key.NewKey256(append([]byte{0x60}, zeroBytes(31)...)) // 011000...000
	key6  = key.NewKey256(append([]byte{0x70}, zeroBytes(31)...)) // 011100...000
	key7  = key.NewKey256(append([]byte{0x18}, zeroBytes(31)...)) // 000110...000
	key8  = key.NewKey256(append([]byte{0x14}, zeroBytes(31)...)) // 000101...000
	key9  = key.NewKey256(append([]byte{0x10}, zeroBytes(31)...)) // 000100...000
	key10 = key.NewKey256(append([]byte{0x20}, zeroBytes(31)...)) // 001000...000
	key11 = key.NewKey256(append([]byte{0x30}, zeroBytes(31)...)) // 001100...100
)

func TestBasic(t *testing.T) {
	bucketSize := 100
	rt := New[key.Key256](kt.NewID(key0), bucketSize)
	require.Equal(t, bucketSize, rt.BucketSize())

	require.Equal(t, key0, rt.Self())
}

func TestAddPeer(t *testing.T) {
	p := kt.NewID(key0) // irrelevant

	rt := New[key.Key256](kt.NewID(key0), 2)

	require.Equal(t, 0, rt.SizeOfBucket(0))

	// add peer CPL=1, bucket=0
	success := rt.addPeer(key1, p)
	require.True(t, success)
	require.Equal(t, 1, rt.SizeOfBucket(0))

	// cannot add the same peer twice
	success = rt.addPeer(key1, p)
	require.False(t, success)
	require.Equal(t, 1, rt.SizeOfBucket(0))

	// add peer CPL=0, bucket=0
	success = rt.addPeer(key2, p)
	require.True(t, success)
	require.Equal(t, 2, rt.SizeOfBucket(0))

	// add peer CPL=0, bucket=0. split of bucket0
	// key1 goes to bucket1
	success = rt.addPeer(key3, p)
	require.True(t, success)
	require.Equal(t, 2, rt.SizeOfBucket(0))
	require.Equal(t, 1, rt.SizeOfBucket(1))

	// already 2 peers with CPL = 0, so this should fail
	success = rt.addPeer(key4, p)
	require.False(t, success)
	// add peer CPL=1, bucket=1
	success = rt.addPeer(key5, p)
	require.True(t, success)
	require.Equal(t, 2, rt.SizeOfBucket(1))

	// already 2 peers with CPL = 1, so this should fail
	// even if bucket 1 is the last bucket
	success = rt.addPeer(key6, p)
	require.False(t, success)

	// add two peers with CPL = 3, bucket=2
	success = rt.addPeer(key7, p)
	require.True(t, success)
	success = rt.addPeer(key8, p)
	require.True(t, success)
	// cannot add a third peer with CPL = 3
	success = rt.addPeer(key9, p)
	require.False(t, success)

	// add two peers with CPL = 2, bucket=2
	success = rt.addPeer(key10, p)
	require.True(t, success)
	success = rt.addPeer(key11, p)
	require.True(t, success)

	// remove all peers with CPL = 0
	success = rt.RemoveKey(key2)
	require.True(t, success)
	success = rt.RemoveKey(key3)
	require.True(t, success)

	// a new peer with CPL = 0 can be added
	// note: p belongs to bucket 0
	success = rt.AddNode(p)
	require.True(t, success)
	// cannot add the same peer twice even tough
	// the bucket is not full
	success = rt.AddNode(p)
	require.False(t, success)
}

func TestRemoveKey(t *testing.T) {
	p := kt.NewID(key0) // irrelevant

	rt := New[key.Key256](kt.NewID(key0), 2)
	rt.addPeer(key1, p)
	success := rt.RemoveKey(key2)
	require.False(t, success)
	success = rt.RemoveKey(key1)
	require.True(t, success)
}

func TestGetNode(t *testing.T) {
	p := kt.NewID(key0)

	rt := New[key.Key256](kt.NewID(key0), 2)
	success := rt.addPeer(key1, p)
	require.True(t, success)

	peerid, found := rt.GetNode(key1)
	require.True(t, found)
	require.Equal(t, p, peerid)

	peerid, found = rt.GetNode(key2)
	require.False(t, found)
	require.Zero(t, peerid)

	success = rt.RemoveKey(key1)
	require.True(t, success)

	peerid, found = rt.GetNode(key1)
	require.False(t, found)
	require.Zero(t, peerid)
}

func TestNearestPeers(t *testing.T) {
	peerIds := make([]libp2p.PeerID, 0, 12)
	for i := 0; i < 12; i++ {
		peerIds = append(peerIds, libp2p.PeerID{ID: peer.ID(fmt.Sprintf("QmPeer%d", i))})
	}

	bucketSize := 5

	rt := NormalizedRt[key.Key256, libp2p.PeerID]{
		self:       key0,
		buckets:    make([][]peerInfo[key.Key256, libp2p.PeerID], 0),
		bucketSize: bucketSize,
	}
	rt.buckets = append(rt.buckets, make([]peerInfo[key.Key256, libp2p.PeerID], 0))

	rt.addPeer(key1, peerIds[1])
	rt.addPeer(key2, peerIds[2])
	rt.addPeer(key3, peerIds[3])
	rt.addPeer(key4, peerIds[4])
	rt.addPeer(key5, peerIds[5])
	rt.addPeer(key6, peerIds[6])
	rt.addPeer(key7, peerIds[7])
	rt.addPeer(key8, peerIds[8])
	rt.addPeer(key9, peerIds[9])
	rt.addPeer(key10, peerIds[10])
	rt.addPeer(key11, peerIds[11])

	// find the 5 nearest peers to key0
	peers := rt.NearestNodes(key0, bucketSize)
	require.Equal(t, bucketSize, len(peers))

	expectedOrder := []libp2p.PeerID{peerIds[9], peerIds[8], peerIds[7], peerIds[10], peerIds[11]}
	require.Equal(t, expectedOrder, peers)

	peers = rt.NearestNodes(key11, 2)
	require.Equal(t, 2, len(peers))

	// create routing table with a single duplicate peer
	// useful to test peers sorting with duplicate (even tough it should never happen)
	rt2 := NormalizedRt[key.Key256, libp2p.PeerID]{
		self:       key0,
		buckets:    make([][]peerInfo[key.Key256, libp2p.PeerID], 0),
		bucketSize: bucketSize,
	}
	rt2.buckets = append(rt2.buckets, make([]peerInfo[key.Key256, libp2p.PeerID], 0))

	rt2.buckets[0] = append(rt2.buckets[0], peerInfo[key.Key256, libp2p.PeerID]{peerIds[1], key1})
	rt2.buckets[0] = append(rt2.buckets[0], peerInfo[key.Key256, libp2p.PeerID]{peerIds[1], key1})
	peers = rt2.NearestNodes(key0, 10)
	require.Equal(t, peers[0], peers[1])
}

func TestTableConcurrentReadWrite(t *testing.T) {
	nodes := make([]*kt.ID[key.Key32], 5000)
	for i := range nodes {
		nodes[i] = kt.NewID(kt.RandomKey())
	}

	rt := New[key.Key32](kt.NewID(key.Key32(0)), 2)

	workers := 3
	var wg sync.WaitGroup
	wg.Add(workers)

	// start workers to concurrently read and write the routing table
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			work := make([]*kt.ID[key.Key32], len(nodes))
			copy(work, nodes)
			rand.Shuffle(len(work), func(i, j int) { work[i], work[j] = work[j], work[i] })

			for i := range work {
				node := work[i]
				_, found := rt.GetNode(node.Key())
				if !found {
					// add new peer
					rt.AddNode(work[i])
				} else {
					// remove it
					rt.RemoveKey(node.Key())
				}

			}
		}()
	}
	wg.Wait()
}

func TestRunNormalizeFullBucket(t *testing.T) {
	///////////////////////////////////////////////////////////////////////////////
	bucketSize := 5

	rt := NewWithKey[key.Key256, libp2p.PeerID](key0, bucketSize)

	require.Equal(t, 0, rt.SizeOfBucket(0))
	peerIds := make([]libp2p.PeerID, 0, 12)
	for i := 0; i < 12; i++ {
		peerIds = append(peerIds, libp2p.PeerID{ID: peer.ID(fmt.Sprintf("QmPeer%d", i))})
	}

	// adds peers in order of closest to farthest
	// nodes in each bucket (including the last bucket) have CID equal to the bucket index
	rt.addPeer(key9, peerIds[9])
	rt.addPeer(key8, peerIds[8])
	rt.addPeer(key7, peerIds[7])
	rt.addPeer(key10, peerIds[10])
	rt.addPeer(key11, peerIds[11])
	rt.addPeer(key1, peerIds[1])
	rt.addPeer(key5, peerIds[5])
	rt.addPeer(key6, peerIds[6])
	rt.addPeer(key2, peerIds[2])
	rt.addPeer(key3, peerIds[3])
	rt.addPeer(key4, peerIds[4])

	// find the 5 nearest peers to key0
	peers := rt.NearestNodesAsServer(key0, key3) // fetches nodes from bucket 3 (highest CPL with key0)
	require.Equal(t, bucketSize, len(peers))

	// tests getRecordsFromLowerBucketIndices --- outer if loop
	// add all of a lower bucket, without going into subbuckets at all (or randomization)
	expectedOrder := []libp2p.PeerID{peerIds[9], peerIds[8], peerIds[7], peerIds[10], peerIds[11]}
	require.Equal(t, expectedOrder, peers)

	peersNearestNodes := rt.NearestNodes(key10, bucketSize)
	peersNearestNodesAsServer := rt.NearestNodesAsServer(key10, key3) // fetches nodes from bucket 2

	require.Equal(t, bucketSize, len(peersNearestNodes))
	require.Equal(t, bucketSize, len(peersNearestNodesAsServer))

	// tests getRecordsFromHigherBucketIndices, flattenBucketRecords
	// add all of a higher bucket, without randomization
	expectedOrder = []libp2p.PeerID{peerIds[10], peerIds[11], peerIds[9], peerIds[8], peerIds[7]}
	require.Equal(t, expectedOrder, peersNearestNodes)
	require.Equal(t, expectedOrder, peersNearestNodesAsServer)

	// TODO: check -- this was set to query for key10 but I think it should be key1 or key5
	peersNearestNodes = rt.NearestNodes(key1, bucketSize) // fetches nodes from bucket 1
	peersNearestNodesAsServer = rt.NearestNodesAsServer(key1, key3)

	// tests getRecordsFromHigherBucketIndices, flattenBucketRecords
	// add all of a higher bucket, *with* randomization (pick 2 of total 5 peers across 2 higher buckets)
	// picks peerID 9, 8 with seed 37 initialized internally in NewWithKey above
	// then picks peerID 10, 7
	expectedOrder = []libp2p.PeerID{peerIds[1], peerIds[5], peerIds[6], peerIds[9], peerIds[8]}
	require.Equal(t, expectedOrder, peersNearestNodes)
	expectedOrder = []libp2p.PeerID{peerIds[1], peerIds[5], peerIds[6], peerIds[10], peerIds[7]}
	require.Equal(t, expectedOrder, peersNearestNodesAsServer)

	peersNearestNodes = rt.NearestNodes(key3, bucketSize)
	peersNearestNodesAsServer = rt.NearestNodesAsServer(key3, key10) // fetches nodes from bucket 0

	// tests the same functions as the previous case (bucket 1)
	// tests getRecordsFromHigherBucketIndices, flattenBucketRecords
	// add all of a higher bucket, *with* randomization (pick 2 of total 8 peers across 2 higher buckets)
	// picks peerID 1, 5 with seed 37 initialized internally in NewWithKey above
	// then picks peerID 9, 1
	expectedOrder = []libp2p.PeerID{peerIds[3], peerIds[4], peerIds[2], peerIds[1], peerIds[5]}
	require.Equal(t, expectedOrder, peersNearestNodes)
	expectedOrder = []libp2p.PeerID{peerIds[2], peerIds[3], peerIds[4], peerIds[9], peerIds[1]}
	require.Equal(t, expectedOrder, peersNearestNodesAsServer)
}

func TestRunNormalizeLower(t *testing.T) {
	bucketSize := 4

	rt := NewWithKey[key.Key256, libp2p.PeerID](key0, bucketSize)

	require.Equal(t, 0, rt.SizeOfBucket(0))
	peerIds := make([]libp2p.PeerID, 0, 12)
	for i := 0; i < 12; i++ {
		peerIds = append(peerIds, libp2p.PeerID{ID: peer.ID(fmt.Sprintf("QmPeer%d", i))})
	}

	// adds peers in order of closest to farthest
	// nodes in each bucket (including the last bucket) have CID equal to the bucket index
	rt.addPeer(key9, peerIds[9])
	rt.addPeer(key8, peerIds[8])
	rt.addPeer(key7, peerIds[7])
	rt.addPeer(key10, peerIds[10])
	rt.addPeer(key11, peerIds[11])
	rt.addPeer(key1, peerIds[1])
	rt.addPeer(key5, peerIds[5])
	rt.addPeer(key6, peerIds[6])
	rt.addPeer(key2, peerIds[2])
	rt.addPeer(key3, peerIds[3])
	rt.addPeer(key4, peerIds[4])

	// find the 4 nearest peers to key0
	peers := rt.NearestNodesAsServer(key0, key3) // fetches nodes from bucket 3 (highest CPL with key0)
	require.Equal(t, bucketSize, len(peers))

	// 3 nodes in desired bucket, and one from lower bucket (either 10 or 11), randomly
	// This is broken right now.
	// It works if you add *all* nodes from a lower bucket but not the random part.
	// The problem is that createSubBuckets function is not complete ?
	expectedOrder := []libp2p.PeerID{peerIds[9], peerIds[8], peerIds[7], peerIds[10]}
	require.Equal(t, expectedOrder, peers)
}

func TestRunNormalizeRandLower(t *testing.T) {
	bucketSize := 5

	rt := NewWithKey[key.Key256, libp2p.PeerID](key0, bucketSize)

	require.Equal(t, 0, rt.SizeOfBucket(0))
	peerIds := make([]libp2p.PeerID, 0, 12)
	for i := 0; i < 12; i++ {
		peerIds = append(peerIds, libp2p.PeerID{ID: peer.ID(fmt.Sprintf("QmPeer%d", i))})
	}

	// adds peers in order of closest to farthest
	// nodes in each bucket (including the last bucket) have CID equal to the bucket index
	rt.addPeer(key9, peerIds[9])
	rt.addPeer(key10, peerIds[10])
	rt.addPeer(key11, peerIds[11])
	rt.addPeer(key1, peerIds[1])
	rt.addPeer(key5, peerIds[5])
	rt.addPeer(key6, peerIds[6])
	rt.addPeer(key2, peerIds[2])
	rt.addPeer(key3, peerIds[3])
	rt.addPeer(key4, peerIds[4])

	// find the 5 nearest peers to key0
	peers := rt.NearestNodesAsServer(key0, key3) // fetches nodes from bucket 3 (highest CPL with key0)
	require.Equal(t, bucketSize, len(peers))

	// 1 node from current bucket. 2 nodes from lower bucket. 2 random nodes from next lower bucket.
	// the 2 random should be chosen from 1, 5, 6.
	expectedOrder := []libp2p.PeerID{peerIds[9], peerIds[10], peerIds[11], peerIds[1], peerIds[5]}
	require.Equal(t, expectedOrder, peers)

	// find the 5 nearest peers to key0
	peers = rt.NearestNodesAsServer(key11, key3) // fetches nodes from bucket 2
	require.Equal(t, bucketSize, len(peers))

	// 2 node from current buckets. 1 node from higher bucket. 2 random nodes from next lower bucket.
	// the 2 random should be chosen from 1, 5, 6.
	expectedOrder = []libp2p.PeerID{peerIds[9], peerIds[10], peerIds[11], peerIds[1], peerIds[5]}
	require.Equal(t, expectedOrder, peers)
}

func newPeerID(t testing.TB) (peer.ID, crypto.PrivKey) {
	id, key := newIdentity(t)
	return id, key
}

func newIdentity(t testing.TB) (peer.ID, crypto.PrivKey) {
	t.Helper()

	priv, pub, err := crypto.GenerateEd25519Key(rng)
	require.NoError(t, err)

	id, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	return id, priv
}

func TestNormalizeFullBuckets(t *testing.T) {
	n := 250
	peers := make([]*libp2p.PeerID, n)
	bucketSize := 20
	rt := NewWithKey[key.Key256, libp2p.PeerID](key0, bucketSize)

	for i := 0; i < n; i++ {
		// generate peer ID
		ppid, _ := newPeerID(t)
		pid := libp2p.NewPeerID(ppid)

		peers[i] = pid

		// add peer to routing table
		rt.addPeer(pid.Key(), *pid)
	}

	bucketsWithPeerIds := rt.NormalizeRT(rt.self)

	// check that all buckets are full
	for i := 0; i < len(rt.normalizedBuckets)-1; i++ {
		if len(rt.normalizedBuckets[i]) != bucketSize {
			t.Errorf("bucket %d is not full, only %d values", i, len(rt.normalizedBuckets[i]))
		}
	}

	for bid, bucket := range bucketsWithPeerIds {
		for _, peerID := range bucket {
			if len(peerID.ID) == 0 {
				t.Errorf("bucket %d contains empty peerID", bid)
			}
		}
	}

}
