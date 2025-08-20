package main

import (
	"IPFS_CRDT/CRDTDag"
	"IPFS_CRDT/Config"
	"IPFS_CRDT/Payload"
	Tests "IPFS_CRDT/example/tests"
	IPFSLink "IPFS_CRDT/ipfsLink"
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"

	// "time"

	files "github.com/ipfs/go-ipfs-files"
	// "github.com/pkg/profile"
)

type PayloadExample struct {
	X int
}

func (this *PayloadExample) FromString(payload string) {
	v, err := strconv.Atoi(payload)
	this.X = v
	if err != nil {
		panic(fmt.Errorf("error with PayloadExample FromString: %s", payload))
	}
	if false {

		files.WriteTo(nil, "file1.data")
		IPFSLink.InitClient("a", "a")

	}
}
func (this *PayloadExample) ToString() string {
	return strconv.Itoa(this.X)
}
func testCRDTDag() {
	x := CRDTDag.CRDTDagNode{}
	dd := make([]CRDTDag.EncodedStr, 0)
	dd = append(dd, CRDTDag.EncodedStr{Str: []byte("dependence1")})
	dd = append(dd, CRDTDag.EncodedStr{Str: []byte("dependence2")})
	peerID := "123PID321"
	pl := PayloadExample{X: 3}
	var plprime Payload.Payload = &pl
	x.CreateNode(dd, peerID, &plprime)
	x.ToFile("thisisafile.txt")
	y := CRDTDag.CRDTDagNode{}
	pl2 := PayloadExample{X: 3}
	var plprime2 Payload.Payload = &pl2
	y.CreateNodeFromFile("thisisafile.txt", &plprime2)
	y.ToFile("thisisafile2.txt")
}

var mu sync.Mutex

func main() {

	// Tests.RemoteTestSet()n

	peerName := flag.String("name", "node1", "name/identity of the current node")
	mode := flag.String("mode", "", "mode of the current application")
	updatesNB := flag.Int("updatesNB", 1000, "Number of updates")
	totalNbUpdate := flag.Int("totalNbUpdate", -1, "Number of updates")
	updating := flag.Bool("updating", false, "do I update the data")
	measurement := flag.Bool("TimeMeasurement", true, "do I Measure the different timefor each CID, adds files continainning these")
	ntpServ := flag.String("NTPS", "0.europe.pool.ntp.org", "Available NTP server for time measures")
	encode := flag.String("encode", "", "Data encription key")
	bootstrapPeer := flag.String("ni", "", "Client bootstrap for pubsub")
	ipfsbootstrap := flag.String("IPFSBootstrap", "", "IPFS bootstrap peer to have a private network")
	swarmKey := flag.Bool("SwarmKey", true, "IPFS bootstrap peer to have a private network")
	parralelRetrieve := flag.Bool("ParallelRetrieve", true, "If true, doesn't block algorithm while retrieving data")
	waitTime := flag.Int("WaitTime", 30, "Number of awaiten micro seconds betweek each look-up, increase to retrieve more concurrently every updates")
	syncTime := flag.Int("SyncTime", 5, "Number of awaiten seconds betweek each Send States (State-based), increase to Syncronyse sooner but this may stress the algorithm")

	flag.Parse()

	cfg := Config.IM_CRDTConfig{
		PeerName:         *peerName,
		Mode:             *mode,
		UpdatesNB:        *updatesNB,
		TotalNbUpdate:    *totalNbUpdate,
		Measurement:      *measurement,
		NtpServ:          *ntpServ,
		Encode:           *encode,
		BootstrapPeer:    *bootstrapPeer,
		IPFSbootstrap:    *ipfsbootstrap,
		SwarmKey:         *swarmKey,
		ParallelRetrieve: *parralelRetrieve,
		Updating:         *updating,
		WaitTime:         *waitTime,
		SyncTime:         *syncTime,
		TestMode:         false,
	}
	fmt.Fprintf(os.Stderr, "Updates Number : %d\n", cfg.UpdatesNB)
	_ = measurement
	_ = updating
	Config.PrintConfig(cfg)
	Config.ToFile(cfg, "./configSAVE.cfg")
	if *mode == "BootStrap" {
		fmt.Println("bootstrap peer :", *bootstrapPeer)
		// if err := os.Mkdir(*peerName, os.ModePerm); err != nil {
		// 	panic(err)
		// }
		if err := os.Mkdir(cfg.PeerName, os.ModePerm); err != nil {
			fmt.Print(err, "\n")
		}
		if err := os.Mkdir(cfg.PeerName+"/remote", os.ModePerm); err != nil {
			fmt.Print(err, "\n")
		}
		if err := os.Mkdir(cfg.PeerName+"/rootNode", os.ModePerm); err != nil {
			fmt.Print(err, "\n")
		}
		if err := os.Mkdir(cfg.PeerName+"/time", os.ModePerm); err != nil {
			fmt.Print(err, "\n")
		}
		Config.ToFile(cfg, cfg.PeerName+"/time/config.cfg")
		// Tests.Peer1Concu(cfg) // ------------- MANAGE CONCURENCY !!! Operation based representation of 2P-Set
		// Tests.BootstrapDeltaBasedSetUp(cfg) // ------------- MANAGE CONCURENCY !!! Delta based CLSet
		// Tests.BootstrapStateBasedSetUp(cfg) // ------------- State-based CLSet !!!
		// Tests.Peer1IPFS(cfg) // ------------- NO CONCURENCY, ONLY IPFS ALONE !!!
		// Tests.Peer1(*peerName, *updatesNB, *ntpServ) // ------------- NO CONCURENCY, CRDT + IPFS  !!!
		Tests.PeerSendIPFSBootstrapSharingFilesOnly(cfg) // ------------ Exchange files, and only files, to test

		// Tests.LogootBootstrap_OpBased(cfg) // Logoot, manage Concurrency
	} else if *mode == "update" {

		fmt.Println("bootstrap peer :", *bootstrapPeer)
		// if err := os.Mkdir(*peerName, os.ModePerm); err != nil {
		// 	panic(err)
		// }
		if err := os.Mkdir(cfg.PeerName+"/remote", os.ModePerm); err != nil {
			fmt.Print(err, "\n")
		}
		if err := os.Mkdir(cfg.PeerName+"/rootNode", os.ModePerm); err != nil {
			fmt.Print(err, "\n")
		}
		if err := os.Mkdir(cfg.PeerName+"/time", os.ModePerm); err != nil {
			fmt.Print(err, "\n")
		}
		Config.ToFile(cfg, cfg.PeerName+"/time/config.cfg")

		// defer profile.Start(profile.CPUProfile).Stop()
		// if false {
		// 	Tests.Peer2Concu(*peerName, *bootstrapPeer, *updatesNB)
		// 	fmt.Println("test2")
		// }

		if cfg.Updating {
			// fmt.Println("UPDATING IN FACT")
			Tests.Peer2ConcuUpdate(cfg) // ------------- 2P-Set MANAGE CONCURENCY - OP based!!!
			// Tests.Peer_DeltaUpdating(cfg) // ------------- MANAGE CONCURENCY !!!
			// Tests.LogootUpdate_OpBased(cfg) // Logoot, manage Concurrency
			// Tests.Peer_Updating(cfg) // ------------- State-based CLSet !!!
		} else {
			// fmt.Println("NOT UPDATING FIOU")
			// Tests.Peer2Concu(cfg) // ------------------- 2P-Set MANAGE CONCURENCY OP based!!!
			Tests.PeerSendIPFSBootstrapReceivingFilesOnly(cfg) // ------------ Exchange files, and only files, to test
			// Tests.Peer_DeltaNotUpdating(cfg) // ------------- MANAGE CONCURENCY !!!
			// Tests.LogootNoUpdate_OpBased(cfg) // Logoot, manage Concurrency
			// Tests.Peer_NotUpdating(cfg) // ------------- State-based CLSet !!!
		}

		// Tests.Peer2IPFS(cfg) // ------------- NO CONCURENCY, ONLY IPFS ALONE !!!
		// Tests.Peer2(*peerName, *bootstrapPeer, *updatesNB, *ntpServ) // ------------- NO CONCURENCY, CRDT + IPFS  !!!
	}
}
