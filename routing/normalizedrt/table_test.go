package normalizedrt

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/plprobelab/go-kademlia/libp2p"

	kt "github.com/plprobelab/go-kademlia/internal/kadtest"
	"github.com/plprobelab/go-kademlia/key"
	"github.com/stretchr/testify/require"
)

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

	peersNearestNodes = rt.NearestNodes(key10, bucketSize) // fetches nodes from bucket 1
	peersNearestNodesAsServer = rt.NearestNodesAsServer(key10, key3)

	// tests getRecordsFromHigherBucketIndices, flattenBucketRecords
	// add all of a higher bucket, *with* randomization (pick 2 of total 5 peers across 2 higher buckets)
	// picks peerID 11, 7 with seed 37 initialized internally in NewWithKey above
	expectedOrder = []libp2p.PeerID{peerIds[1], peerIds[5], peerIds[6], peerIds[11], peerIds[7]}
	require.Equal(t, expectedOrder, peersNearestNodes)
	require.Equal(t, expectedOrder, peersNearestNodesAsServer)

	peersNearestNodes = rt.NearestNodes(key3, bucketSize)
	peersNearestNodesAsServer = rt.NearestNodesAsServer(key3, key10) // fetches nodes from bucket 0

	// tests the same functions as the previous case (bucket 1)
	// tests getRecordsFromHigherBucketIndices, flattenBucketRecords
	// add all of a higher bucket, *with* randomization (pick 2 of total 8 peers across 2 higher buckets)
	// picks peerID 6, 8 with seed 37 initialized internally in NewWithKey above
	expectedOrder = []libp2p.PeerID{peerIds[2], peerIds[3], peerIds[4], peerIds[6], peerIds[8]}
	require.Equal(t, expectedOrder, peersNearestNodes)
	require.Equal(t, expectedOrder, peersNearestNodesAsServer)
}

func TestRunNormalizeLower(t *testing.T) {
	p := kt.NewID(key0)
	bucketSize := 4
	rt := New[key.Key256](kt.NewID(key0), bucketSize)

	require.Equal(t, 0, rt.SizeOfBucket(0))

	success := rt.addPeer(key10, p)
	require.True(t, success)
	success = rt.addPeer(key9, p)
	require.True(t, success)
	success = rt.addPeer(key11, p)
	require.True(t, success)
	success = rt.addPeer(key6, p)
	require.True(t, success)

	client := key3 // NormalizeRT drops the client's key before normalizing
	target := key8
	// rt.NormalizeRT(client) // NearestNodesAsServer runs NormalizeRT
	require.Equal(t, bucketSize, rt.SizeOfBucket(0))

	peers := rt.NearestNodesAsServer(target, client)
	require.Equal(t, bucketSize, len(peers))
}

func TestRunNormalizeHigher(t *testing.T) {
	p := kt.NewID(key0)
	bucketSize := 2
	rt := New[key.Key256](kt.NewID(key0), bucketSize)

	require.Equal(t, 0, rt.SizeOfBucket(0))

	success := rt.addPeer(key10, p)
	require.True(t, success)
	success = rt.addPeer(key9, p)
	require.True(t, success)
	success = rt.addPeer(key11, p)
	require.True(t, success)
	success = rt.addPeer(key6, p)
	require.True(t, success)

	client := key3 // NormalizeRT drops the client's key before normalizing
	target := key8
	peers := rt.NearestNodesAsServer(target, client)
	require.Equal(t, bucketSize, len(peers))
}

// func TestPreviousBucketsUptoK(t *testing.T) {
// 	// rust code:
// 	/*
// 	let rt = RoutingTable::new()
//         .add_record(0, "0010", "RE")
//         .add_record(1, "0001", "RE")
//         .add_record(1, "0000", "RE")
//         .add_record(2, "0111", "RE");

// 	0011

//     let normalize_rt = rt.normalize(SplitMix64::seed_from_u64(0).borrow_mut());
//     assert_eq!(normalize_rt.buckets[2].records[0].cid, "0111");
//     assert_eq!(normalize_rt.buckets[2].records[1].cid, "0010");
//     assert_eq!(normalize_rt.buckets[2].records[2].cid, "0001");
//     assert_eq!(normalize_rt.buckets[2].records[3].cid, "0000");
// 	*/
// 	p := kt.NewID(key0)

// 	rt := New[key.Key256](kt.NewID(key0), 4)

// 	require.Equal(t, 0, rt.SizeOfBucket(0))

// 	success := rt.addPeer(key10, p)
// 	require.True(t, success)
// 	success = rt.addPeer(key9, p)
// 	require.True(t, success)
// 	success = rt.addPeer(key11, p)
// 	require.True(t, success)
// 	success = rt.addPeer(key6, p)
// 	require.True(t, success)

// 	rt.NormalizeRT(key0)
// }
