package IpfsLink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-libipfs/files"

	iface "github.com/ipfs/boxo/coreiface"
	ifacepath "github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/kubo/config"
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/bootstrap"
	"github.com/ipfs/kubo/core/coreapi"
	libp2pIFPS "github.com/ipfs/kubo/core/node/libp2p"
	"github.com/ipfs/kubo/plugin/loader"
	"github.com/ipfs/kubo/repo/fsrepo"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

// DiscoveryInterval is how often we re-publish our mDNS records.
const DiscoveryInterval = time.Hour

// DiscoveryServiceTag is used in our mDNS advertisements to discover other chat peers.
const DiscoveryServiceTag = "pubsub-chat-example"

// printErr is like fmt.Printf, but writes to stderr.
func printErr(m string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, m, args...)
}

// defaultNick generates a nickname based on the $USER environment variable and
// the last 8 chars of a peer ID.
func defaultNick(p peer.ID) string {
	return fmt.Sprintf("%s-%s", os.Getenv("USER"), shortID(p))
}

// shortID returns the last 8 chars of a base58-encoded peer id.
func shortID(p peer.ID) string {
	pretty := p.Pretty()
	return pretty[len(pretty)-8:]
}

// discoveryNotifee gets notified when we find a new peer via mDNS discovery
type discoveryNotifee struct {
	h host.Host
}

// HandlePeerFound connects to peers discovered via mDNS. Once they're connected,
// the PubSub system will automatically start interacting with them if they also
// support PubSub.
func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	fmt.Printf("discovered new peer %s\n", pi.Addrs[0])
	err := n.h.Connect(context.Background(), pi)
	if err != nil {
		fmt.Printf("error connecting to peer %s: %s\n", pi.ID.Pretty(), err)
	}
}

// setupDiscovery creates an mDNS discovery service and attaches it to the libp2p Host.
// This lets us automatically discover peers on the same LAN and connect to them.
func setupDiscovery(h host.Host) error {
	// setup mDNS discovery to find local peers
	s := mdns.NewMdnsService(h, DiscoveryServiceTag, &discoveryNotifee{h: h})
	return s.Start()
}

type MultiAddressesJson struct {
	PeerID      string   `json:"peer_id"`
	AddressList []string `json:"listened_addresses"`
}

type IpfsLink struct {
	Cancel          context.CancelFunc
	Ctx             context.Context
	IpfsCore        iface.CoreAPI
	IpfsNode        *core.IpfsNode
	Topics          []*pubsub.Topic
	Hst             host.Host
	GossipSub       *pubsub.PubSub
	Cr              *Client
	ParalelRetrieve bool
}

func InitNode(peerName string, bootstrapPeer string, ipfsBootstrap []byte, swarmKey bool, parallelRetrieve bool) (*IpfsLink, error) {
	ct, cancl := context.WithCancel(context.Background())

	// Spawn a local peer using a temporary path, for testing purposes
	var idBootstrap peer.AddrInfo
	var ipfsA iface.CoreAPI
	var nodeA *core.IpfsNode
	var err error
	bootstrapConfig := MultiAddressesJson{
		AddressList: make([]string, 0),
	}
	if len(ipfsBootstrap) > 0 {
		err = json.Unmarshal(ipfsBootstrap, &bootstrapConfig)
		if err != nil {
			printErr("error Unmarshalling bootstrap file, %v", err)
			panic(err)
		}

		e := idBootstrap.UnmarshalJSON(ipfsBootstrap)
		if e != nil {
			panic(fmt.Errorf("couldn't Unmarshal bootstrap peer addr info, error : %s", e))
		}
	}

	ipfsA, nodeA, err = spawnEphemeral(ct, bootstrapConfig.AddressList, swarmKey)

	if err != nil {
		panic(fmt.Errorf("failed to spawn peer node: %s", err))
	}
	h := InitClient(peerName, bootstrapPeer)
	ipfs := IpfsLink{
		Cancel:          cancl,
		Ctx:             ct,
		IpfsCore:        ipfsA,
		IpfsNode:        nodeA,
		Hst:             nodeA.PeerHost,
		GossipSub:       h.Ps,
		Cr:              h,
		ParalelRetrieve: parallelRetrieve,
	}

	//fmt.Println(ipfs.IpfsNode.Peerstore.PeerInfo(ipfs.IpfsNode.PeerHost.ID()))
	return &ipfs, err
}

func WritePeerInfo(sys IpfsLink, file string) {
	// Get peerID from IPFS Key
	ipfsKey, err := sys.IpfsCore.Key().Self(context.Background())
	if err != nil {
		panic(err)
	}
	peerID := ipfsKey.ID().String()

	announcedAddressesList, err := sys.IpfsCore.Swarm().LocalAddrs(context.Background())
	if err != nil {
		panic(err)
	}

	// Store the announced addresses
	myPeerLocalInfo := MultiAddressesJson{
		PeerID:      peerID,
		AddressList: []string{},
	}
	for _, addr := range announcedAddressesList {
		myPeerLocalInfo.AddressList = append(myPeerLocalInfo.AddressList, addr.String()+"/p2p/"+peerID)
	}

	myPeerlocalInfoBytes, _ := json.Marshal(&myPeerLocalInfo)
	err = ioutil.WriteFile(file, myPeerlocalInfoBytes, 0644)

}

var loadPluginsOnce sync.Once

func setupPlugins(externalPluginsPath string) error {
	// Load any external plugins if available on externalPluginsPath
	plugins, err := loader.NewPluginLoader(filepath.Join(externalPluginsPath, "plugins"))
	if err != nil {
		return fmt.Errorf("error loading plugins: %s", err)
	}

	// Load preloaded and external plugins
	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	return nil
}

var LoopBackAddresses = []string{
	"/ip4/127.0.0.1/ipcidr/8",
	"/ip6/::1/ipcidr/128",
}

const PORT = 0

func createTempRepo(BootstrapMultiAddrList []string) (string, error) {
	repoPath, err := os.MkdirTemp("", "ipfs-shell")
	if err != nil {
		return "", fmt.Errorf("failed to get temp dir: %s", err)
	}

	// Create a config with default options and a 2048 bit key
	cfg, err := config.Init(io.Discard, 2048)
	if err != nil {
		return "", err
	}

	cfg.Bootstrap = BootstrapMultiAddrList

	bootstrap.DefaultBootstrapConfig = bootstrap.BootstrapConfig{
		MinPeerThreshold:        0,
		Period:                  10 * time.Second,
		ConnectionTimeout:       (10 * time.Second) / 3, // Period / 3
		BackupBootstrapInterval: 1 * time.Hour,
	}

	cfg.Discovery.MDNS.Enabled = false
	cfg.AutoNAT = config.AutoNATConfig{
		ServiceMode: config.AutoNATServiceEnabled,
	}

	cfg.Swarm = config.SwarmConfig{
		AddrFilters: LoopBackAddresses,
		RelayService: config.RelayService{
			Enabled: config.True,
		},
	}
	cfg.Addresses = config.Addresses{
		Swarm: []string{
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", PORT)},
		NoAnnounce: LoopBackAddresses,
	}
	cfg.Datastore = config.DefaultDatastoreConfig()
	dataStoreFilePath := filepath.Join(repoPath, "datastore_spec")
	datastoreContent := map[string]interface{}{
		"mounts": []interface{}{
			map[string]interface{}{
				"mountpoint": "/blocks",
				"path":       "blocks",
				"shardFunc":  "/repo/flatfs/shard/v1/next-to-last/2",
				"type":       "flatfs",
			},
			map[string]interface{}{
				"mountpoint": "/",
				"path":       "datastore",
				"type":       "levelds",
			},
		},
		"type": "mount",
	}

	datastoreContentBytes, err := json.Marshal(datastoreContent)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(dataStoreFilePath, []byte(datastoreContentBytes), 0644); err != nil {
		panic(err)
	}

	myRepositoryVersion := []byte("14")
	if err = os.WriteFile(filepath.Join(repoPath, "version"), myRepositoryVersion, 0644); err != nil {
		panic(err)
	}

	plugins, err := loader.NewPluginLoader(repoPath)
	if err != nil {
		panic(fmt.Errorf("error loading plugins: %s", err))
	}

	if err := plugins.Initialize(); err != nil {
		panic(fmt.Errorf("error initializing plugins: %s", err))
	}

	if err := plugins.Inject(); err != nil {
		panic(fmt.Errorf("error initializing plugins: %s", err))
	}

	// Create the repo with the config
	err = fsrepo.Init(repoPath, cfg)
	if err != nil {
		return "", fmt.Errorf("failed to init ephemeral node: %s", err)
	}

	return repoPath, nil
}

/// ------ Spawning the node

// Creates an IPFS node and returns its coreAPI
func createNode(ctx context.Context, repoPath string) (*core.IpfsNode, error) {
	// Open the repo
	repo, err := fsrepo.Open(repoPath)
	if err != nil {
		return nil, err
	}

	nodeOptions := &core.BuildCfg{
		Online:  true,
		Routing: libp2pIFPS.DHTOption, // This option sets the node to be a full DHT node (both fetching and storing DHT Records)
		// Routing: libp2p.DHTClientOption, // This option sets the node to be a client DHT node (only fetching records)
		Repo:      repo,
		Permanent: true,
	}

	node, err := core.NewNode(ctx, nodeOptions)
	if err != nil {
		return nil, err
	}
	return node, nil

}

// Spawns a node to be used just for this run (i.e. creates a tmp repo)
func spawnEphemeral(ctx context.Context, btstrap []string, swarmKey bool) (iface.CoreAPI, *core.IpfsNode, error) {
	// Create a Temporary Repo containing all configuration
	repoPath, err := createTempRepo(btstrap)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create temp repo: %s", err)
	}

	// Create an IPFS node
	printErr("repository : %s\n", repoPath)
	if swarmKey {
		os.WriteFile(repoPath+"/swarm.key", []byte("/key/swarm/psk/1.0.0/\n/base16/\nedd99a84bbdd5c9cfc06bcc039d219b1000885ecba26901c02e7c8792bfaaa70"), fs.FileMode(os.O_CREATE|os.O_WRONLY|os.O_APPEND))
	}

	node, err := createNode(ctx, repoPath)
	if err != nil {
		return nil, nil, err
	}
	if swarmKey {
		node.PNetFingerprint = []byte("4c7dc2a2735a84b4b11ff5b39225aa771cea1abd3acf9b98708a25f286df851c")
	}
	// Connect the node to the other private network nodes

	api, err := coreapi.NewCoreAPI(node)

	return api, node, err
}

func AddIPFS(ipfs *IpfsLink, message []byte) (ifacepath.Resolved, error) {

	peerCidFile, err := ipfs.IpfsCore.Unixfs().Add(ipfs.Ctx,
		files.NewBytesFile(message))

	if err != nil {
		panic(fmt.Errorf("could not add File: %s", err))
	}
	go ipfs.IpfsCore.Dht().Provide(ipfs.Ctx, peerCidFile)
	// if err != nil {
	// 	panic(fmt.Errorf("Could not provide File - %s", err))
	// }
	return peerCidFile, err
}

type CID struct{ str string }

func GetIPFS(ipfs *IpfsLink, cids []cid.Cid) ([]files.Node, error) {
	// str_CID, err := ContentIdentifier.Decode(c)

	var files []files.Node = make([]files.Node, len(cids))
	var err error
	var file *os.File

	ti := time.Now()
	if len(cids) > 0 {

		file, err = os.OpenFile("node1/time/timeConcurrentRetrieve.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
		if err != nil {
			panic(fmt.Errorf("could not Close Debug File in IPFSLink:: GetIPFS\nerror:%s", err))
		}

		file.WriteString("" +
			"===============================New Batch of Cid To Retrieve===============================\n" +
			"=============================The Cids to  Download are thoose=============================\n")
	}

	wg := sync.WaitGroup{}
	errhapened := true
	for errhapened {

		errhapened = false
		for index, c := range cids {

			if ipfs.ParalelRetrieve {
				clocal := c
				wg.Add(1)
				go func(i int) {
					str_CID := cids[i]
					cctx, _ := context.WithDeadline(ipfs.Ctx, time.Now().Add(time.Second*3000))
					//files[i], err = ipfs.IpfsCore.Dag().Get(cctx, str_CID)
					array_one := make([]cid.Cid, 1)
					array_one[0] = str_CID
					channel_f := ipfs.IpfsCore.Dag().GetMany(cctx, array_one)
					for f := range channel_f {
						if f.Node != nil {
							files[i], _ = ipfs.IpfsCore.Unixfs().Get(cctx, ifacepath.IpfsPath(f.Node.Cid()))
						} else {
							err = errors.New("NIL :/")
							file.WriteString(fmt.Sprintf("Could not get the CID %s, node is NIL \n", cids[i]))
						}
					}
					if err != nil {
						printErr("could not get file with CID - %s : %s", clocal, err)
						errhapened = true
					}

					wg.Done()
				}(index)

			} else {
				// It has been asked to be retrieved one by one
				str_CID := cids[index]
				cctx, _ := context.WithDeadline(ipfs.Ctx, time.Now().Add(time.Second*3000))
				array_one := make([]cid.Cid, 1)
				array_one[0] = str_CID
				channel_f := ipfs.IpfsCore.Dag().GetMany(cctx, array_one)
				for f := range channel_f {
					files[index], _ = ipfs.IpfsCore.Unixfs().Get(cctx, ifacepath.IpfsPath(f.Node.Cid()))
				}
			}
			if ipfs.ParalelRetrieve {
				wg.Wait()
			}
		}
	}
	file.WriteString("Got all the cids asked\n")
	if len(cids) > 0 {
		file.WriteString("\n" +
			"Nb of Cids: " + strconv.Itoa(len(cids)) + "\n" +
			"Time To Download: " + strconv.FormatInt(time.Since(ti).Milliseconds(), 10) + " ms\n" +
			"=================================The end of CID retrieval=================================\n" +
			"\n" +
			"\n" +
			"\n")
		err = file.Close()
		if err != nil {
			panic(fmt.Errorf("could not Close Debug File in IPFSLink:: GetIPFS\nerror:%s", err))
		}
	} else {

		file.WriteString("\n" +
			fmt.Sprintf("Even if no CID Where downloaded, len(cids):%d", len(cids)) +
			"=================================The end of CID retrieval=================================\n")
	}

	return files, err
}

func PubIPFS(ipfs *IpfsLink, msg []byte) {
	ipfs.Cr.Publish(msg)
}
