package CLSet

import (
	CRDTDag "IPFS_CRDT/CRDTDag"
	"IPFS_CRDT/Config"
	CRDT "IPFS_CRDT/Crdt"
	Payload "IPFS_CRDT/Payload"
	IpfsLink "IPFS_CRDT/ipfsLink"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"golang.org/x/sync/semaphore"
)

// =======================================================================================
// Payload - OpBased
// =======================================================================================

type Element string
type OpNature int

const (
	ADD OpNature = iota
	REMOVE
)

type Operation struct {
	Elem Element
	Op   OpNature
}
type State struct {
	SetData map[Element]int
}

func (thisState State) mergeState(other State) {
	for x := range other.SetData {
		val, ok := thisState.SetData[x]
		valother := thisState.SetData[x]
		if ok {
			if val < valother {
				thisState.SetData[x] = valother
			}
		} else {
			thisState.SetData[x] = valother
		}

	}
}

func (thisElement Element) ToString() string {
	b, err := json.Marshal(thisElement)
	if err != nil {
		panic(fmt.Errorf("set Operation To string fail to Marshal\nError: %s", err))
	}
	return string(b[:])
}
func (op *Element) op_from_string(s string) {
	err := json.Unmarshal([]byte(s), op)
	if err != nil {
		panic(fmt.Errorf("set Operation To string fail to Marshal\nError: %s", err))
	}
}

type PayloadStateBased struct {
	Payload.Payload
	SetState State
	Id       string
}

func (thisPayload *PayloadStateBased) Create_PayloadStateBased(s string, state State) {

	thisPayload.SetState = state
	thisPayload.Id = s
}
func (thisPayload *PayloadStateBased) ToString() string {
	b, err := json.Marshal(thisPayload)
	if err != nil {
		panic(fmt.Errorf("set Operation To string fail to Marshal\nError: %s", err))
	}
	return string(b[:])
}
func (thisPayload *PayloadStateBased) FromString(s string) {
	err := json.Unmarshal([]byte(s), thisPayload)
	if err != nil {
		panic(fmt.Errorf("set Operation To string fail to Marshal\nError: %s", err))
	}
}

// =======================================================================================
// CRDTSet OpBased
// =======================================================================================

type CRDTCLSetStateBased struct {
	sys      *IpfsLink.IpfsLink
	SetState State
}

func Create_CRDTCLSetStateBased(s *IpfsLink.IpfsLink) CRDTCLSetStateBased {
	return CRDTCLSetStateBased{
		sys:      s,
		SetState: State{SetData: make(map[Element]int)},
	}
}

func (thisCRDT *CRDTCLSetStateBased) Add(x string) {
	if _, ok := thisCRDT.SetState.SetData[Element(x)]; !ok {
		thisCRDT.SetState.SetData[Element(x)] = 0
	}
	if thisCRDT.SetState.SetData[Element(x)]%2 == 0 {
		thisCRDT.SetState.SetData[Element(x)] = thisCRDT.SetState.SetData[Element(x)] + 1
	}
}

func (thisCRDT *CRDTCLSetStateBased) Remove(x string) {
	if _, ok := thisCRDT.SetState.SetData[Element(x)]; !ok {
		thisCRDT.SetState.SetData[Element(x)] = 0
	}
	if thisCRDT.SetState.SetData[Element(x)]%2 == 1 {
		thisCRDT.SetState.SetData[Element(x)] = thisCRDT.SetState.SetData[Element(x)] + 1
	}
}

func (thisCRDT *CRDTCLSetStateBased) Lookup() []string {
	i := make([]string, 0)
	fmt.Println("size", len(thisCRDT.SetState.SetData))
	for x := range thisCRDT.SetState.SetData {
		if thisCRDT.SetState.SetData[Element(x)]%2 == 1 {
			i = append(i, string(x))
		}
	}

	return i
}

func (thisCRDT *CRDTCLSetStateBased) ToFile(file string) {

	b, err := json.Marshal(thisCRDT)
	if err != nil {
		panic(fmt.Errorf("CRDTDagNode - ToFile Could not Marshall %s\nError: %s", file, err))
	}
	f, err := os.Create(file)
	if err != nil {
		panic(fmt.Errorf("CRDTDagNode - ToFile Could not Create the file %s\nError: %s", file, err))
	}
	f.Write(b)
	err = f.Close()
	if err != nil {
		panic(fmt.Errorf("CRDTDagNode - ToFile Could not Write to the file %s\nError: %s", file, err))
	}
}

// =======================================================================================
// CRDTSetDagNode OpBased
// =======================================================================================

type CRDTCLSetStateBasedDagNode struct {
	DagNode CRDTDag.CRDTDagNode
}

func (thisCRDTDagNode *CRDTCLSetStateBasedDagNode) FromFile(fil string) {
	var pl Payload.Payload = &PayloadStateBased{}
	thisCRDTDagNode.DagNode.CreateNodeFromFile(fil, &pl)
}

func (thisCRDTDagNode *CRDTCLSetStateBasedDagNode) GetDirect_dependency() []CRDTDag.EncodedStr {

	return thisCRDTDagNode.DagNode.DirectDependency
}

func (thisCRDTDagNode *CRDTCLSetStateBasedDagNode) ToFile(file string) {

	thisCRDTDagNode.DagNode.ToFile(file)
}
func (thisCRDTDagNode *CRDTCLSetStateBasedDagNode) GetEvent() *Payload.Payload {

	return thisCRDTDagNode.DagNode.Event
}
func (thisCRDTDagNode *CRDTCLSetStateBasedDagNode) GetPiD() string {

	return thisCRDTDagNode.DagNode.PID
}
func (thisCRDTDagNode *CRDTCLSetStateBasedDagNode) CreateEmptyNode() *CRDTDag.CRDTDagNodeInterface {
	n := CreateDagNode(State{SetData: make(map[Element]int)}, "")
	var node CRDTDag.CRDTDagNodeInterface = &n
	return &node
}
func CreateDagNode(s State, id string) CRDTCLSetStateBasedDagNode {
	var pl Payload.Payload = &PayloadStateBased{SetState: s, Id: id}
	slic := make([]CRDTDag.EncodedStr, 0)
	return CRDTCLSetStateBasedDagNode{
		DagNode: CRDTDag.CRDTDagNode{
			Event:            &pl,
			PID:              id,
			DirectDependency: slic,
		},
	}
}

// =======================================================================================
// CRDTSetDag OpBased
// =======================================================================================

type CRDTCLSetStateBasedDag struct {
	dag           *CRDTDag.CRDTManager
	measurement   bool
	setValue      CRDTCLSetStateBased
	lastSentValue CRDTCLSetStateBased
}

func (thisCRDTDag *CRDTCLSetStateBasedDag) GetDag() *CRDTDag.CRDTManager {

	return thisCRDTDag.dag
}
func (thisCRDTDag *CRDTCLSetStateBasedDag) SendRemoteUpdates() {

	thisCRDTDag.dag.SendRemoteUpdates()
}
func (thisCRDTDag *CRDTCLSetStateBasedDag) GetCRDTManager() *CRDTDag.CRDTManager {

	return thisCRDTDag.dag
}
func (thisCRDTDag *CRDTCLSetStateBasedDag) IsKnown(cid CRDTDag.EncodedStr) bool {

	find := false
	for x := range thisCRDTDag.dag.GetAllNodes() {
		if string(thisCRDTDag.dag.GetAllNodes()[x]) == string(cid.Str) {
			find = true
			break
		}
	}
	return find
}
func (thisCRDTDag *CRDTCLSetStateBasedDag) Merge(cids []CRDTDag.EncodedStr) []string {

	to_add := make([]CRDTDag.EncodedStr, 0)
	for _, cid := range cids {
		find := thisCRDTDag.IsKnown(cid)
		if !find {
			to_add = append(to_add, cid)
		}
	}

	fils, err := thisCRDTDag.dag.GetNodeFromEncodedCid(to_add)
	if err != nil {
		panic(fmt.Errorf("could not get ndoes from encoded cids\nerror :%s", err))
	}

	for index := range fils {
		fil := fils[index]
		n := CreateDagNode(State{}, "")           // Create an Empty operation
		n.FromFile(fil)                           // Fill it with the operation just read
		thisCRDTDag.remoteAddNode(cids[index], n) // Add the data as a Remote operation (which are applied as a local one)
		thisCRDTDag.setValue.SetState.mergeState((*n.DagNode.Event).(*PayloadStateBased).SetState)
	}
	return fils
}

func (thisCRDTDag *CRDTCLSetStateBasedDag) remoteAddNode(cID CRDTDag.EncodedStr, newnode CRDTCLSetStateBasedDagNode) {
	var pl CRDTDag.CRDTDagNodeInterface = &newnode
	thisCRDTDag.dag.RemoteAddNodeSuper(cID, &pl)
}

func (thisCRDTDag *CRDTCLSetStateBasedDag) callAddToIPFS(bytes []byte, file string) (path.Resolved, error) {
	time_toencrypt := -1
	ti := time.Now()
	var path path.Resolved
	var err error
	if thisCRDTDag.dag.Key != "" {
		path, err = thisCRDTDag.GetCRDTManager().AddToIPFS(thisCRDTDag.dag.Sys, bytes, &time_toencrypt)
	} else {
		path, err = thisCRDTDag.GetCRDTManager().AddToIPFS(thisCRDTDag.dag.Sys, bytes)
		time_toencrypt = 0
	}
	if err != nil {
		panic(fmt.Errorf("error in callAddToIPFS, Couldn't add file to IPFS\nError: %s\n \t", err))
	}
	Total_AddTime := int(time.Since(ti).Nanoseconds())
	time_add := Total_AddTime - time_toencrypt

	if thisCRDTDag.measurement {
		// Write time to encrypt in a file
		fstrBis := ""
		if thisCRDTDag.dag.Key != "" {
			fstrBis = file + ".timeEncrypt"
			if _, err := os.Stat(fstrBis); !errors.Is(err, os.ErrNotExist) {
				os.Remove(fstrBis)
			}
			fil, err := os.OpenFile(fstrBis, os.O_CREATE|os.O_WRONLY, 0755)
			if err != nil {
				panic(fmt.Errorf("error RemoteAddNodeSupde - , Could not open the time file to write encoded data\nError: %s", err))
			}
			_, err = fil.Write([]byte(strconv.Itoa(time_toencrypt)))
			if err != nil {
				panic(fmt.Errorf("error RemoteAddNodeSupde - , Could not write the time file to write encoded data\nError: %s", err))
			}
			err = fil.Close()
			if err != nil {
				panic(fmt.Errorf("error RemoteAddNodeSupde - , Could not close the time file to write encoded data \nError: %s", err))
			}
		}

		// Write time to add to IFPS
		fstrBis = file + ".timeAdd"
		if _, err := os.Stat(fstrBis); !errors.Is(err, os.ErrNotExist) {
			os.Remove(fstrBis)
		}
		fil, err := os.OpenFile(fstrBis, os.O_CREATE|os.O_WRONLY, 0755)
		if err != nil {
			panic(fmt.Errorf("error RemoteAddNodeSupde - , Could not open the time file to write encoded data\nError: %s", err))
		}
		_, err = fil.Write([]byte(strconv.Itoa(time_add)))
		if err != nil {
			panic(fmt.Errorf("error RemoteAddNodeSupde - , Could not write the time file to write encoded data\nError: %s", err))
		}
		err = fil.Close()
		if err != nil {
			panic(fmt.Errorf("error remoteAddNodeSupde - , Could not close the time file to write encoded data \nError: %s", err))
		}
	}

	return path, err
}

func (thisCRDTDag *CRDTCLSetStateBasedDag) SendState() (string, TimeTuple) {
	if !reflect.DeepEqual(thisCRDTDag.lastSentValue.SetState.SetData, thisCRDTDag.setValue.SetState.SetData) {
		newNode := CreateDagNode(thisCRDTDag.setValue.SetState, thisCRDTDag.GetSys().IpfsNode.Identity.Pretty())
		newNode.DagNode.DirectDependency = append(newNode.DagNode.DirectDependency, thisCRDTDag.dag.Root_nodes...)

		strFile := thisCRDTDag.dag.NextFileName()
		if _, err := os.Stat(strFile); !errors.Is(err, os.ErrNotExist) {
			os.Remove(strFile)
		}
		newNode.ToFile(strFile)
		bytes, err := os.ReadFile(strFile)
		if err != nil {
			panic(fmt.Errorf("ERROR INCREMENT CRDTSetOpBasedDag, could not read file\nerror: %s", err))
		}
		path, err := thisCRDTDag.callAddToIPFS(bytes, strFile)
		if err != nil {
			panic(fmt.Errorf("CRDTSetOpBasedDag Increment, could not add the file to IFPS\nerror: %s", err))
		}

		encodedCid := thisCRDTDag.dag.EncodeCid(path)
		c := cid.Cid{}
		err = json.Unmarshal(encodedCid.Str, &c)
		if err != nil {
			panic(fmt.Errorf("CRDTSetOpBasedDag Increment, could not UnMarshal\nerror: %s", err))
		}

		// fmt.Println("encodedCid Increment :", c.String())
		var pl CRDTDag.CRDTDagNodeInterface = &newNode

		thisCRDTDag.dag.AddNode(encodedCid, &pl) // Adding the node created before to the Merkle-DAG

		thisCRDTDag.SendRemoteUpdates() // Send the StateBased node.

		times := TimeTuple{} // Time measurement structure, for analysis only (when thisCRDTDag.Measurement is true)
		if thisCRDTDag.measurement {
			//Add time
			times.FileSize = len(bytes)
			b, err := os.ReadFile(strFile + ".timeAdd")
			if err != nil {
				panic(fmt.Errorf("couldn't read TimeAdd file\nError: %s\n\t", err))
			}
			intAdd, err := strconv.Atoi(string(b))
			if err != nil {
				panic(fmt.Errorf(" timeAdd file is malformatted, and couldn't be Atoi'ed\nError: %s\n\t", err))
			}
			times.Time_add = intAdd

			err = os.Remove(strFile + ".timeAdd")
			if err != nil {
				panic(fmt.Errorf("couldn't Remove TimeAdd file\nError: %s\n\t", err))
			}

			// Encrypt Time
			times.Time_encrypt = 0
			if thisCRDTDag.dag.Key != "" {
				b, err = os.ReadFile(strFile + ".timeEncrypt")
				if err != nil {
					panic(fmt.Errorf("couldn't read timeEncrypt file\nError: %s\n\t", err))
				}
				intAdd, err = strconv.Atoi(string(b))
				if err != nil {
					panic(fmt.Errorf("timeEncrypt file is malformatted, and couldn't be Atoi'ed\nError: %s\n\t", err))
				}
				times.Time_encrypt = intAdd

				err = os.Remove(strFile + ".timeEncrypt")
				if err != nil {
					panic(fmt.Errorf("couldn't Remove timeEncrypt file\nError: %s\n\t", err))
				}
			}
		}
		thisCRDTDag.lastSentValue.SetState.mergeState(thisCRDTDag.setValue.SetState)
		return c.String(), times
	} else {
		return "", TimeTuple{}
	}
}
func (thisCRDTDag *CRDTCLSetStateBasedDag) Add(x string) {
	thisCRDTDag.setValue.Add(x)
}

func (thisCRDTDag *CRDTCLSetStateBasedDag) Remove(x string) {
	thisCRDTDag.setValue.Remove(x)
}

func Create_CRDTCLSetStateBasedDag(sys *IpfsLink.IpfsLink, cfg Config.IM_CRDTConfig) *CRDTCLSetStateBasedDag {
	man := CRDTDag.Create_CRDTManager(sys, cfg.PeerName, cfg.BootstrapPeer, cfg.Encode, cfg.Measurement)
	crdtSet := CRDTCLSetStateBasedDag{dag: &man, measurement: cfg.Measurement, setValue: Create_CRDTCLSetStateBased(sys)}
	if cfg.BootstrapPeer == "" {
		x, err := os.ReadFile("initial_value")
		if err != nil {
			panic(fmt.Errorf("could not read initial_value, error : %s", err))
		}
		crdtSet.Add(string(x))
		// newNode := CreateDagNode(Operation{Elem: Element(x), Op: ADD}, crdtSet.GetSys().IpfsNode.Identity.Pretty())
		// strFile := crdtSet.dag.NextFileName()

		// if _, err := os.Stat(strFile); !errors.Is(err, os.ErrNotExist) {
		// 	os.Remove(strFile)
		// }
		// newNode.ToFile(strFile)

		// bytes, err := os.ReadFile(strFile)
		// if err != nil {
		// 	panic(fmt.Errorf("ERROR INCREMENT CRDTCLSetStateBasedDag, could not read file\nerror: %s", err))
		// }
		// path, err := man.AddToIPFS(crdtSet.dag.Sys, bytes) // Add Inital State ( so it isn't counted as messages)
		// if err != nil {
		// 	panic(fmt.Errorf("CRDTCLSetStateBasedDag Increment, could not add the file to IFPS\nerror: %s", err))
		// }

		// encodedCid := crdtSet.dag.EncodeCid(path)
		// c := cid.Cid{}
		// err = json.Unmarshal(encodedCid.Str, &c)
		// if err != nil {
		// 	panic(fmt.Errorf("CRDTCLSetStateBasedDag Increment, could not UnMarshal\nerror: %s", err))
		// }
		// // fmt.Println("encodedCid Increment :", c.String())
		// var pl1 CRDTDag.CRDTDagNodeInterface = &newNode

		// crdtSet.dag.AddNode(encodedCid, &pl1) // TODOSetCrdt Complete Node interface

	}
	var pl CRDTDag.CRDTDag = &crdtSet

	CRDTDag.CheckForRemoteUpdates(&pl, sys.Cr.Sub, man.Sys.Ctx)

	return &crdtSet
}

func (thisCRDTDag *CRDTCLSetStateBasedDag) GetSys() *IpfsLink.IpfsLink {

	return thisCRDTDag.dag.Sys
}

func (thisCRDTDag *CRDTCLSetStateBasedDag) Lookup_ToSpecifyType() *CRDT.CRDT {

	var pl CRDT.CRDT = &thisCRDTDag.setValue
	return &pl
}
func (thisCRDTDag *CRDTCLSetStateBasedDag) Lookup() CRDTCLSetStateBased {

	// crdt := thisCRDTDag.logokup_ToSpecifyType()
	// var pl CRDTDag.CRDTDag = &crdtSet
	return *(*thisCRDTDag.Lookup_ToSpecifyType()).(*CRDTCLSetStateBased)
}

type TimeTuple struct {
	Cid            string
	RetrievalAlone int
	RetrievalTotal int
	CalculTime     int
	Time_add       int
	Time_encrypt   int
	Time_decrypt   int
	ArrivalTime    int
	FileSize       int
}

// semaphore usage
func getSema(sema *semaphore.Weighted, ctx context.Context) {
	t := time.Now()
	err := sema.Acquire(ctx, 1)
	for err != nil && time.Since(t) < 10*time.Second {
		time.Sleep(10 * time.Microsecond)
		err = sema.Acquire(ctx, 1)
	}
	if err != nil {
		panic(fmt.Errorf("semaphore of read/write file locked !!!!\n cannot acquire it\n \t"))
	}
}

func returnSema(sema *semaphore.Weighted) {
	sema.Release(1)
}

func checkFileExists(filePath string) bool {
	_, error := os.Stat(filePath)
	//return !os.IsNotExist(err)
	return !errors.Is(error, os.ErrNotExist)
}

// Check update function retrieve files from ipfs (long)
// and then reserves the semaphore to actually modify the data (short)
func (thisCRDTDag *CRDTCLSetStateBasedDag) CheckUpdate(sema *semaphore.Weighted) []TimeTuple {
	received := make([]TimeTuple, 0)
	files, err := ioutil.ReadDir(thisCRDTDag.GetDag().Nodes_storage_enplacement + "/remote")

	// filez, err := os.OpenFile(thisCRDTDag.GetDag().Nodes_storage_enplacement+"/time/timeBIS.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0755)
	if err != nil {
		fmt.Printf("CheckUpdate - Checkupdate could not open folder\nerror: %s\n", err)
	} else {
		ti := time.Now()
		to_add := make([]([]byte), 0)
		computetime := make([]int64, 0)
		arrivalTime := make([]int64, 0)
		// filez.WriteString(fmt.Sprintf("Files found : %d\nFolder : %s\n========\n", len(files), thisCRDTDag.GetDag().Nodes_storage_enplacement+"/remote"))
		for _, file := range files {
			// filez.WriteString("1\n")
			if file.Size() > 0 && !strings.Contains(file.Name(), ".ArrivalTime") {
				// filez.WriteString("2\n")
				fil, err := os.OpenFile(thisCRDTDag.GetDag().Nodes_storage_enplacement+"/remote/"+file.Name(), os.O_RDONLY, os.ModeAppend)
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not open the sub file\nError: %s", err))
				}
				stat, err := fil.Stat()
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not get stat the sub file\nError: %s", err))
				}
				// filez.WriteString("3\n")
				bytesread := make([]byte, stat.Size())
				n, err := fil.Read(bytesread)
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not read the sub file\nError: %s", err))
				}

				// fmt.Println("stat.size :", stat.Size(), "read :", n)
				if int64(n) != stat.Size() {
					panic(fmt.Errorf("error in checkupdate, Could not read entirely the sub file\nError: read %d byte unstead of %d", n, stat.Size()))
				}
				err = fil.Close()
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not close the sub file\nError: %s", err))
				}
				if !thisCRDTDag.IsKnown(CRDTDag.EncodedStr{Str: bytesread}) {
					to_add = append(to_add, bytesread)
				}
				s := cid.Cid{}
				json.Unmarshal(bytesread, &s)

				// filez.WriteString("4\n")
				err = os.Remove(thisCRDTDag.GetDag().Nodes_storage_enplacement + "/remote/" + file.Name())
				if err != nil || errors.Is(err, os.ErrNotExist) {
					panic(fmt.Errorf("error in checkupdate, Could not remove the sub file\nError: %s", err))
				}

				// Take the time measurement of this file
				// Get the time of arrival to compute pubsub time
				fil, err = os.OpenFile(thisCRDTDag.GetDag().Nodes_storage_enplacement+"/remote/"+file.Name()+".ArrivalTime", os.O_RDONLY, os.ModeAppend)
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not open the sub file\nError: %s", err))
				}
				stat, err = fil.Stat()
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not get stat the sub file\nError: %s", err))
				}
				bytesread = make([]byte, stat.Size())
				n, err = fil.Read(bytesread)
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not read the sub file\nError: %s", err))
				}

				// filez.WriteString("5\n")
				fmt.Println("stat.size :", stat.Size(), "read :", n)
				if int64(n) != stat.Size() {
					panic(fmt.Errorf("error in checkupdate, Could not read entirely the sub file\nError: read %d byte unstead of %d", n, stat.Size()))
				}
				err = fil.Close()
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not close the sub file\nError: %s", err))
				}
				time_of_arrival, _ := strconv.Atoi(string(bytesread))
				arrivalTime = append(arrivalTime, int64(time_of_arrival))

				// filez.WriteString("6\n")
				//computation time, time to manage this file
				timeToCompute := time.Since(ti).Nanoseconds()
				computetime = append(computetime, timeToCompute)
				ti = time.Now()
			} else {
				fmt.Printf("Remote folder contain a FILE of a NULL SIZE\n")
				// filez.WriteString("7\n")
			}
		}

		// apply the update on the peer's data
		// filez.WriteString("8\n")
		getSema(sema, thisCRDTDag.GetSys().Ctx)
		// filez.WriteString("9\n")
		received = thisCRDTDag.add_cids(to_add, computetime, arrivalTime, ti)
		// filez.WriteString("10\n")

		if len(to_add) > 0 {
			thisCRDTDag.GetDag().UpdateRootNodeFolder()
		}

		// filez.WriteString("11\n")
		returnSema(sema)
		// filez.WriteString("12\n")
	}
	// filez.WriteString("13\n")
	return received
}

func (thisCRDTDag *CRDTCLSetStateBasedDag) add_cids(to_add []([]byte), computetime []int64, arrivalTime []int64, ti time.Time) []TimeTuple {
	received := make([]TimeTuple, 0)

	bytes_encoded := make([]CRDTDag.EncodedStr, 0)

	for _, bytesread := range to_add {
		bytes_encoded = append(bytes_encoded, CRDTDag.EncodedStr{Str: bytesread})
	}

	filesWritten := thisCRDTDag.Merge(bytes_encoded)

	for index, bytesread := range to_add {
		s := cid.Cid{}
		json.Unmarshal(bytesread, &s)
		timeRetrieve := 0
		timeDecrypt := 0
		fileSize := 0
		if thisCRDTDag.measurement && filesWritten[index] != "node1/node1" {
			// Get Time of Retrieval
			str, err := os.ReadFile(filesWritten[index] + ".timeRetrieve")
			fileInfo, _ := os.Stat(filesWritten[index])
			fileSize = int(fileInfo.Size())
			if err != nil {
				panic(fmt.Errorf("set.go - could not read time to retrieve measurement\nerror: %s", err))
			}
			timeRetrieve, err = strconv.Atoi(string(str))
			if err != nil {
				panic(fmt.Errorf("set.go - could not translate time to retrieve to string, maybe malformerd ?\nerror: %s", err))
			}

			err = os.Remove(filesWritten[index] + ".timeRetrieve")
			if err != nil {
				panic(fmt.Errorf("set.go - could not remove time to retrieve file\nerror: %s", err))
			}

			//If we use it, get time of decryption of the file
			if thisCRDTDag.dag.Key != "" {
				str, err = os.ReadFile(filesWritten[index] + ".timeDecrypt")
				if err != nil {
					panic(fmt.Errorf("set.go - could not read time decrypt measurement\nerror: %s", err))
				}
				timeDecrypt, err = strconv.Atoi(string(str))
				if err != nil {
					panic(fmt.Errorf("set.go - could not translate time to retrieve to string, maybe malformerd ?\nerror: %s", err))
				}
				err = os.Remove(filesWritten[index] + ".timeDecrypt")
				if err != nil {
					panic(fmt.Errorf("set.go - could not remove time to decrypt file\nerror: %s", err))
				}

			}

		}
		// fmt.Println("calling UpdateRootNodeFolder")

		received = append(received, TimeTuple{Cid: s.String(), RetrievalAlone: timeRetrieve, RetrievalTotal: timeRetrieve * len(to_add), CalculTime: int(computetime[index]), ArrivalTime: int(arrivalTime[index]), Time_decrypt: timeDecrypt, Time_encrypt: 0, FileSize: fileSize})
	}

	thisCRDTDag.GetDag().UpdateRootNodeFolder()
	return received
}
