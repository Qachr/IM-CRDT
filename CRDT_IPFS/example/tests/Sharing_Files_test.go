package tests

// import (
// 	"IPFS_CRDT/Config"
// 	"IPFS_CRDT/example/CLSet"
// 	IpfsLink "IPFS_CRDT/ipfsLink"
// 	"math/rand"

// 	"encoding/json"

// 	"github.com/ipfs/go-cid"
// 	Files "github.com/ipfs/go-libipfs/files"

// 	"github.com/ipfs/interface-go-ipfs-core/path"

// 	"context"
// 	"errors"
// 	"fmt"
// 	"os"
// 	"strconv"
// 	"time"

// 	"golang.org/x/sync/semaphore"
// 	// "github.com/beevik/ntp"
// )

// func sendToIPFS(ipfs *IpfsLink.IpfsLink, message []byte) (path.Resolved, error) {
// 	path, err := IpfsLink.AddIPFS(ipfs, message)
// 	if err != nil {
// 		panic(fmt.Errorf("CRDTSetOpBasedDag Increment, could not add the file to IFPS\nerror: %s", err))
// 	}
// 	return path, err
// }

// func create_randomdata(size int) []byte {
// 	data := make([]byte, size)

// 	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// 	for i := range data {
// 		data[i] = letterBytes[rand.Intn(len(letterBytes))]
// 	}

// 	return data
// }

// type measurementTime struct {
// 	cid          string
// 	time         int64
// 	timeSend     int64
// 	timeretrieve int64
// 	timeWrite    int64
// 	batchsize    int
// }

// func sendFiles(nbUpdates int, ipfs *IpfsLink.IpfsLink, sema *semaphore.Weighted) []measurementTime {
// 	k := 0
// 	time_add := make([]measurementTime, 0)
// 	for k < nbUpdates {

// 		getSema(sema, context.Background())

// 		//TODO : send a random file
// 		randomData := create_randomdata(100)

// 		t := time.Now()
// 		path, err := sendToIPFS(ipfs, randomData)
// 		if err != nil {
// 			panic(fmt.Errorf("error push IPFS node :  %s", err))
// 		}

// 		measure := measurementTime{}
// 		measure.batchsize = 1
// 		measure.cid = path.Cid().String()
// 		measure.time = int64(t.Nanosecond())
// 		measure.timeSend = time.Since(t).Nanoseconds()
// 		measure.timeretrieve = 0

// 		time_add = append(time_add, measure)
// 		returnSema(sema)

// 		by, err := json.Marshal(path.Cid())
// 		if err != nil {
// 			panic(fmt.Errorf("error marshall CID :  %s", err))
// 		}
// 		IpfsLink.PubIPFS(ipfs, by)

// 		k++

// 	}

// 	return time_add
// }

// // \/ BOOTSTRAP PEER IS THIS ONE \/
// func PeerSendIPFSBootstrapSharingFilesOnly(cfg Config.IM_CRDTConfig) {
// 	fileRead, _ := os.OpenFile(cfg.PeerName+"/time/FileRead.log", os.O_CREATE|os.O_WRONLY, 0755)
// 	file, _ := os.OpenFile(cfg.PeerName+"/time/time.csv", os.O_CREATE|os.O_WRONLY, 0755)
// 	sema := semaphore.NewWeighted(1)

// 	// init ipfs system
// 	sys1, err := IpfsLink.InitNode(cfg.PeerName, "", make([]byte, 0), cfg.SwarmKey, cfg.ParallelRetrieve)
// 	if err != nil {
// 		panic(fmt.Errorf("Failed To instanciate IFPS & LibP2P clients : %s", err))
// 	}

// 	str := ""
// 	for i := range sys1.Cr.Host.Addrs() {
// 		s := sys1.Cr.Host.Addrs()[i].String()
// 		str += s + "/p2p/" + sys1.Cr.Host.ID().String() + "\n"
// 	}

// 	if _, err := os.Stat("./ID2"); !errors.Is(err, os.ErrNotExist) {
// 		os.Remove("./ID2")
// 	}

// 	WriteFile("./ID2", []byte(str))

// 	IpfsLink.WritePeerInfo(*sys1, "./IDBootstrapIPFS")

// 	time.Sleep(20 * time.Second)

// 	// init measurement files

// 	fileRead.WriteString("Taking Sema to write headers ... \n")
// 	getSema(sema, sys1.Ctx)
// 	file.WriteString("CID,time,timeSend,Timeretrieve,batchsize\n")
// 	returnSema(sema)
// 	_, err = fileRead.WriteString("Header just written\n")
// 	if err != nil {
// 		panic(fmt.Errorf("Error openning file file\nerror : %s", err))
// 	}

// 	ti := time.Now()
// 	// Sleep 60s before emiting updates to wait others
// 	for time.Since(ti) < 60*time.Second {
// 		time.Sleep(time.Duration(cfg.WaitTime) * time.Microsecond)

// 	}

// 	timeAdd := sendFiles(cfg.UpdatesNB, sys1, sema)

// 	for x := range timeAdd {
// 		file.WriteString(fmt.Sprintf("%s,%d,%d,%d,%d,d,%d\n", timeAdd[x].cid, timeAdd[x].time, timeAdd[x].timeSend, timeAdd[x].timeretrieve, timeAdd[x].timeWrite, timeAdd[x].batchsize))
// 	}

// }

// func retrieveCID(c context.Context, ipfs *IpfsLink.IpfsLink, retrieving []cid.Cid, fileNames []string) ([]string, []measurementTime) {
// 	ti := time.Now()

// 	Storedtimes := make([]measurementTime, 0)
// 	fils, err := IpfsLink.GetIPFS(ipfs, retrieving)
// 	if err != nil {
// 		panic(fmt.Errorf("issue retrieving the IPFS Node :%s", err))
// 	}
// 	filees_ret := make([]string, 0)
// 	timeDownload := 0
// 	if len(fils) > 0 {
// 		timeDownload = int(time.Since(ti).Nanoseconds())
// 	}
// 	for i, fil := range fils {
// 		ti := time.Now()
// 		fstr := fileNames[i]
// 		_ = os.Remove(fstr) // In cas the file where already existing ( which should never be the case)

// 		filees_ret = append(filees_ret, fstr)

// 		Files.WriteTo(fil, fstr) // BottleNeck

// 		thisstime := measurementTime{}
// 		thisstime.batchsize = len(fils)
// 		thisstime.time = int64(time.Now().Nanosecond())
// 		thisstime.cid = retrieving[i].String()
// 		thisstime.timeSend = 0
// 		thisstime.timeWrite = time.Since(ti).Nanoseconds()
// 		thisstime.timeretrieve = int64(timeDownload)

// 		Storedtimes = append(Storedtimes, thisstime)
// 	}

// 	return filees_ret, Storedtimes
// }

// func CheckCIDs(ipfs *IpfsLink.IpfsLink, sema *semaphore.Weighted, limitSize int, c context.Context, folder string, nb_updates int) []measurementTime {
// 	i := 0
// 	ToRetrieveCid := make([]string, 0)
// 	fileName := make([]string, 0)
// 	Storedtimes := make([]measurementTime, 0)
// 	for len(Storedtimes) < nb_updates {
// 		msg, err := ipfs.Cr.Sub.Next(c)
// 		if err != nil {
// 			panic(fmt.Errorf("Check For remote update failed, message not received\nError: %s", err))
// 		} else if msg.ReceivedFrom == ipfs.Cr.Host.ID() {
// 			fmt.Println("Received message from myself")
// 			continue
// 		} else {
// 			fmt.Println("Received message from", msg.ReceivedFrom, "data:", string(msg.Data))

// 			ToRetrieveCid = append(ToRetrieveCid, string(msg.GetData()))
// 			fileName = append(fileName, fmt.Sprintf("%s/cid%d", folder, i))
// 			i = i + 1

// 			if len(ToRetrieveCid) >= limitSize {
// 				retrieving := make([]cid.Cid, 0)
// 				fileNameHere := make([]string, 0)
// 				x := 0
// 				for x < limitSize {
// 					var cidhere cid.Cid
// 					err := json.Unmarshal([]byte(ToRetrieveCid[x]), &cidhere)
// 					if err != nil {
// 						panic(fmt.Errorf("couldn't unmarshall the path, byte :%s \nerror : %s", ToRetrieveCid[x], err))
// 					}

// 					retrieving = append(retrieving, cidhere)
// 					fileNameHere = append(fileNameHere, fileName[x])
// 					x = x + 1
// 				}
// 				ToRetrieveCid = ToRetrieveCid[limitSize:]
// 				fileName = fileName[limitSize:]
// 				_, timesHere := retrieveCID(c, ipfs, retrieving, fileNameHere)
// 				Storedtimes = append(Storedtimes, timesHere...)
// 			}
// 		}
// 	}
// 	return Storedtimes
// }

// // func receiveCID(cfg Config.IM_CRDTConfig) {
// // 	IPFSbootstrapBytes, err := os.ReadFile(cfg.IPFSbootstrap)
// // 	sema := semaphore.NewWeighted(1)
// // 	if err != nil {
// // 		panic(fmt.Errorf("failed to read ipfs bootstrap peer multiaddr : %s", err))
// // 	}
// // 	sys1, err := IpfsLink.InitNode(cfg.PeerName, cfg.BootstrapPeer, IPFSbootstrapBytes, cfg.SwarmKey, cfg.ParallelRetrieve)

// // 	if err != nil {
// // 		panic(fmt.Errorf("failed to instanciate ipfs & libp2p clients : %s", err))
// // 	}
// // 	time.Sleep(10 * time.Second)

// // 	var knownCid *[]string
// // 	*knownCid = make([]string, 0)

// // 	var ToRetrieveCid *[]string
// // 	*ToRetrieveCid = make([]string, 0)

// // 	// init measurement files

// // 	file, err := os.OpenFile(cfg.PeerName+"/time/time.csv", os.O_CREATE|os.O_WRONLY, 0755)
// // 	file.WriteString("CID,time,time_retrieve,time_compute,time_add_IPFS,time_encrypt,time_decrypt,time_Retreive_Whole_Batch,ArrivalTime,sateSize\n")
// // 	if err != nil {
// // 		panic(fmt.Errorf("error openning file file\nerror : %s", err))
// // 	}
// // 	fmt.Printf("Starting the Set, updating %d times\n", cfg.UpdatesNB)

// // 	for {
// // 		time.Sleep(time.Duration(cfg.WaitTime) * time.Microsecond)

// // 		strList := CheckCIDs(sema, )
// // 		if len(strList) > 0 {
// // 			t := strconv.Itoa(GetTime(cfg.NtpServ))

// // 			for j := 0; j < len(strList); j++ {
// // 				file.WriteString(strList[j].Cid + "," + t + "," + strconv.Itoa(strList[j].RetrievalAlone) + "," +
// // 					strconv.Itoa(strList[j].CalculTime) + ",0,0," + strconv.Itoa(strList[j].Time_decrypt) + "," +
// // 					strconv.Itoa(strList[j].RetrievalTotal) + "," + strconv.Itoa(strList[j].ArrivalTime) + "," +
// // 					strconv.Itoa(strList[j].FileSize) + "\n")
// // 			}
// // 		}

// // 	}
// // }

// func PeerReceiving(cfg Config.IM_CRDTConfig) {
// 	sema := semaphore.NewWeighted(1)

// 	// Reading the IPFSBootstrap file
// 	fileInfo, err := os.Stat(cfg.IPFSbootstrap)
// 	if err != nil {
// 		panic(fmt.Errorf("Peer2ConcuUpdate - could Not Open IPFSBootstrap file toread bootstrap address\nerror: %s", err))
// 	}
// 	IPFSbootstrapBytes := make([]byte, fileInfo.Size())
// 	// Writing bytes in the file @file
// 	fil, err := os.OpenFile(cfg.IPFSbootstrap, os.O_RDONLY, 0755)
// 	if err != nil {
// 		panic(fmt.Errorf("Peer2ConcuUpdate - could Not Open IPFSBootstrap file to read it\nerror: %s", err))
// 	}
// 	_, err = fil.Read(IPFSbootstrapBytes)
// 	if err != nil {
// 		panic(fmt.Errorf("could Not read IPFSBootstrap file - Peer2ConcuUpdate - \nerror: %s", err))
// 	}
// 	err = fil.Close()
// 	if err != nil {
// 		panic(fmt.Errorf("could Not Close IPFSBootstrap file - Peer2ConcuUpdate\nerror: %s", err))
// 	}
// 	sys1, err := IpfsLink.InitNode(cfg.PeerName, cfg.BootstrapPeer, IPFSbootstrapBytes, cfg.SwarmKey, cfg.ParallelRetrieve)
// 	if err != nil {
// 		fmt.Printf("Failed To instanciate IFPS & LibP2P clients : %s", err)
// 		panic(err)
// 	}
// 	time.Sleep(10 * time.Second)

// 	file, _ := os.OpenFile(cfg.PeerName+"/time/time.csv", os.O_CREATE|os.O_WRONLY, 0755)
// 	getSema(sema, sys1.Ctx)
// 	file.WriteString("CID,time,timeSend,Timeretrieve,batchsize\n")
// 	returnSema(sema)

// 	if err != nil {
// 		fmt.Printf("Error openning file file\nerror : %s", err)
// 		panic(err)
// 	}

// 	// Sleep 60s to wait others
// 	ti := time.Now()
// 	for time.Since(ti) < 60*time.Second {
// 		time.Sleep(time.Duration(cfg.WaitTime) * time.Microsecond)
// 	}

// 	timeAdd := CheckCIDs(sys1, sema, cfg.UpdatesNB, context.Background(), cfg.PeerName+"/remote", cfg.TotalNbUpdate)

// 	for x := range timeAdd {
// 		file.WriteString(fmt.Sprintf("%s,%d,%d,%d,%d,d,%d\n", timeAdd[x].cid, timeAdd[x].time, timeAdd[x].timeSend, timeAdd[x].timeretrieve, timeAdd[x].timeWrite, timeAdd[x].batchsize))
// 	}
// }

// func SendRandomData(SetCrdt1 *CLSet.CRDTCLSetStateBasedDag, ntpServ string, file *os.File, netID string, sema *semaphore.Weighted, cfg Config.IM_CRDTConfig) {
// 	fileWrite, _ := os.OpenFile(SetCrdt1.GetCRDTManager().Nodes_storage_enplacement+"/time/FileWrite.log", os.O_CREATE|os.O_WRONLY, 0755)
// 	fileWrite.WriteString(fmt.Sprintf("starting the Set, Sending my state every %d s\n", cfg.SyncTime))
// 	ti := time.Now()
// 	defer func(fileWrite *os.File) {
// 		fileWrite.WriteString("WRITE - all updates are done\n")
// 		fileWrite.Close()
// 	}(fileWrite)
// 	k := 0
// 	for {
// 		time.Sleep(100 * time.Microsecond)

// 		if time.Since(ti) >= time.Second/time.Duration(10) {
// 			getSema(sema, context.Background())
// 			fileWrite.WriteString("updating the data\n")
// 			encodedCid, times := SetCrdt1.SendState()
// 			fileWrite.WriteString("updating the data - taking sema\n")
// 			fileWrite.WriteString("Semaphore tooken\n")
// 			file.WriteString(encodedCid + "," + strconv.Itoa(GetTime(ntpServ)) + "," + "0,0," + strconv.Itoa(times.Time_add) + "," + strconv.Itoa(times.Time_encrypt) + ",0,0,0," + strconv.Itoa(times.FileSize) + "\n")
// 			fileWrite.WriteString("returning Semaphore\n")
// 			fileWrite.WriteString("WRITE - 1 line added to time.csv\n")
// 			returnSema(sema)
// 			k++
// 			ti = time.Now()
// 		}

// 	}
// }
