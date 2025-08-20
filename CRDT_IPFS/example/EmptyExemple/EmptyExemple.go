package EmptyExemple

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
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"golang.org/x/sync/semaphore"
)

// ===================================================================
// operation - OpBased
// Define what is an operation, and on what element they act
// This should include :
//   - (self Operation) ToString() string
//   - (op *Operation) op_from_string(s string)
// ===================================================================

type Element string
type OpNature int

const (
	OP1 OpNature = iota
	OP2
	OP3
)

type Operation struct {
	Elem Element
	Op   OpNature
}

func (self Operation) ToString() string {
	b, err := json.Marshal(self)
	if err != nil {
		panic(fmt.Errorf("Set Operation To string fail to Marshal\nError: %s", err))
	}
	return string(b[:])
}
func (op *Operation) op_from_string(s string) {
	err := json.Unmarshal([]byte(s), op)
	if err != nil {
		panic(fmt.Errorf("Set Operation To string fail to Marshal\nError: %s", err))
	}
}

// ===================================================================
// Payload - OpBased
// Define what a payload should contain to be well define
// This should include :
//  - (self *PayloadOpBased) ToString() string
//  - (self *PayloadOpBased) FromString(s string)
// ===================================================================

type PayloadOpBased struct {
	Op Operation
	Id string
}

func (self *PayloadOpBased) Create_PayloadOpBased(s string, o1 Operation) {

	self.Op = o1
	self.Id = s
}
func (self *PayloadOpBased) ToString() string {
	b, err := json.Marshal(self)
	if err != nil {
		panic(fmt.Errorf("Set Operation To string fail to Marshal\nError: %s", err))
	}
	return string(b[:])
}
func (self *PayloadOpBased) FromString(s string) {
	err := json.Unmarshal([]byte(s), self)
	if err != nil {
		panic(fmt.Errorf("Set Operation To string fail to Marshal\nError: %s", err))
	}
}

// ===================================================================
// CRDTEmptyExempleOpBased - OpBased
// Define what a CRDT definition should contain to be well defined
// This should include :
//  - Operations to change on the CRDT
//  - (self *CRDTEmptyExempleOpBased) Lookup() DataType
//                           --> Here DataType = []String(string set)
//  - func (self *CRDTEmptyExempleOpBased) ToFile(file string)
// ===================================================================

type CRDTEmptyExempleOpBased struct {
	sys     *IpfsLink.IpfsLink
	added   []string
	removed []string
}

func Create_CRDTSetOpBased(s *IpfsLink.IpfsLink) CRDTEmptyExempleOpBased {
	return CRDTEmptyExempleOpBased{
		sys:     s,
		added:   make([]string, 0),
		removed: make([]string, 0),
	}
}

func search(list []string, x string) int {
	for i := 0; i < len(list); i++ {
		if list[i] == x {
			return i
		}
	}
	return -1
}

func (self *CRDTEmptyExempleOpBased) Operation1(x string) {
	if search(self.added, x) == -1 {
		self.added = append(self.added, x)
	}
}

func (self *CRDTEmptyExempleOpBased) Operation2(x string) {
	if search(self.removed, x) == -1 {
		self.removed = append(self.removed, x)
	}
}

func (self *CRDTEmptyExempleOpBased) Operation3(x string) {
	if search(self.added, x) == -1 {
		self.added = append(self.added, x)
	}
	if search(self.removed, x) == -1 {
		self.removed = append(self.removed, x)
	}
}

func (self *CRDTEmptyExempleOpBased) Lookup() []string {
	i := make([]string, 0)
	fmt.Println("size", len(self.added))
	for x := range self.added {
		if search(self.removed, self.added[x]) == -1 {
			i = append(i, self.added[x])
			i = append(i, ",")
		}
	}

	return i
}

func (self *CRDTEmptyExempleOpBased) ToFile(file string) {

	b, err := json.Marshal(self)
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

// ===================================================================
// CRDTEmptyExempleOpBasedDagNode
// represent what a DAG node contains to have all wanted data
// This should implement CRDTDagNodeInterface,
// i.e. it must include :
// @ToFile(file string)
//
//	--> to wwrite the node to a file, so it can be sended with IPFS
//
// @FromFile(file string)
//
//	--> to receive a node with a file, toshare & receive with IPFS
//
// @GetEvent() *Payload.Payload
//
//	--> Get the Payload in the DAG Node
//
// @GetPiD() string
//
//	--> Get the identifier of the peer creating this file
//	   (must exists for uniqueness of Nodes)
//
// @GetDirect_dependency() []EncodedStr
//
//	--> Get the List Of CIDs this node has in its dependency
//
// @CreateEmptyNode() *CRDTDagNodeInterface
//
//	  --> Create an empty version of this
//		    either for DAG root, or in order to call node.FromFile(fstr))
//
// ===================================================================
type CRDTEmptyExempleOpBasedDagNode struct {
	DagNode CRDTDag.CRDTDagNode
}

func (self *CRDTEmptyExempleOpBasedDagNode) ToFile(file string) {
	self.DagNode.ToFile(file)
}

func (self *CRDTEmptyExempleOpBasedDagNode) FromFile(fil string) {
	var pl Payload.Payload = &PayloadOpBased{}
	self.DagNode.CreateNodeFromFile(fil, &pl)
}

func (self *CRDTEmptyExempleOpBasedDagNode) GetEvent() *Payload.Payload {
	return self.DagNode.Event
}

func (self *CRDTEmptyExempleOpBasedDagNode) GetPiD() string {
	return self.DagNode.PID
}

func (self *CRDTEmptyExempleOpBasedDagNode) GetDirect_dependency() []CRDTDag.EncodedStr {
	return self.DagNode.DirectDependency
}

func (self *CRDTEmptyExempleOpBasedDagNode) CreateEmptyNode() *CRDTDag.CRDTDagNodeInterface {
	n := CreateDagNode(Operation{}, "")
	var node CRDTDag.CRDTDagNodeInterface = &n
	return &node
}

func CreateDagNode(o Operation, id string) CRDTEmptyExempleOpBasedDagNode {
	var pl Payload.Payload = &PayloadOpBased{Op: o, Id: id}
	slic := make([]CRDTDag.EncodedStr, 0)
	return CRDTEmptyExempleOpBasedDagNode{
		DagNode: CRDTDag.CRDTDagNode{
			Event:            &pl,
			PID:              id,
			DirectDependency: slic,
		},
	}
}

//===================================================================
// CRDTEmptyExempleOpBasedDag
// represent what a DAG node contains to have all wanted data
// This should implement type CRDTDag interface
// i.e. it must include :
// @Lookup_ToSpecifyType() *CRDT.CRDT
//   --> so the type can be found from the interface
//
// @SendRemoteUpdates()
//   --> Just calling CRDTManager.SendRemoteUpdates()
//      to send the roots, nodes, measurement can be added here.
//
// @GetSys() *IPFSLink.IpfsLink
//   --> Get the IPFS deamon & handler linked to this peer
//
// @GetCRDTManager() *CRDTManager(file string)
//   --> link to the CRDT manager implem to call function easily
//
// @Merge(cid []EncodedStr) []string
//   --> So the Dag add new nodes from the CID @Cids And update
//       its data
//   --> it can use CRDTDag.CRDTManager) RemoteAddNodeSuper
//   --> to add a node to
//
// ===== For usage ===
// define the cretion of the CRDT with its initial Value
// Define the operation you will do

// ===================================================================

type CRDTEmptyExempleOpBasedDag struct {
	dag CRDTDag.CRDTManager
}

func (self *CRDTEmptyExempleOpBasedDag) Lookup_ToSpecifyType() *CRDT.CRDT {

	crdt := CRDTEmptyExempleOpBased{
		sys:     self.GetSys(),
		added:   make([]string, 0),
		removed: make([]string, 0),
	}
	for x := range self.dag.GetAllNodes() {
		node := self.dag.GetAllNodesInterface()[x]
		payload := (*(*node).GetEvent()).(*PayloadOpBased)
		if payload.Op.Op == OP1 {
			// fmt.Println("add")
			crdt.Operation1(string((*(*node).GetEvent()).(*PayloadOpBased).Op.Elem))
		} else if payload.Op.Op == OP2 {
			// fmt.Println("remove")
			crdt.Operation2(string((*(*node).GetEvent()).(*PayloadOpBased).Op.Elem))
		} else {

			crdt.Operation3(string((*(*node).GetEvent()).(*PayloadOpBased).Op.Elem))
		}
	}
	var pl CRDT.CRDT = &crdt
	return &pl
}

func (self *CRDTEmptyExempleOpBasedDag) SendRemoteUpdates() {

	self.dag.SendRemoteUpdates()
}

func (self *CRDTEmptyExempleOpBasedDag) GetSys() *IpfsLink.IpfsLink {

	return self.dag.Sys
}

func (self *CRDTEmptyExempleOpBasedDag) GetCRDTManager() *CRDTDag.CRDTManager {

	return &self.dag
}

func (self *CRDTEmptyExempleOpBasedDag) GetDag() *CRDTDag.CRDTManager {

	return &self.dag
}

func (self *CRDTEmptyExempleOpBasedDag) IsKnown(cid CRDTDag.EncodedStr) bool {

	find := false
	for x := range self.dag.GetAllNodes() {
		if string(self.dag.GetAllNodes()[x]) == string(cid.Str) {
			find = true
			break
		}
	}
	return find
}
func (self *CRDTEmptyExempleOpBasedDag) Merge(cids []CRDTDag.EncodedStr) []string {

	to_add := make([]CRDTDag.EncodedStr, 0)
	for _, cid := range cids {
		find := self.IsKnown(cid)
		if !find {
			to_add = append(to_add, cid)
		}
	}

	fils, err := self.dag.GetNodeFromEncodedCid(to_add)
	if err != nil {
		panic(fmt.Errorf("could not get ndoes from encoded cids\nerror :%s", err))
	}

	for index := range fils {
		fil := fils[index]
		// Create an Empty operation
		n := CreateDagNode(Operation{}, "")
		// Fill it with the operation just read
		n.FromFile(fil)
		// Add The node which is a Remote operation
		// It is applied like a local operation
		// But it check the dependency
		self.remoteAddNode(cids[index], n)
	}
	return fils
}

func (self *CRDTEmptyExempleOpBasedDag) remoteAddNode(cID CRDTDag.EncodedStr, newnode CRDTEmptyExempleOpBasedDagNode) {
	var pl CRDTDag.CRDTDagNodeInterface = &newnode
	self.dag.RemoteAddNodeSuper(cID, &pl)
}

func (self *CRDTEmptyExempleOpBasedDag) callAddToIPFS(bytes []byte, file string) (path.Resolved, error) {
	var path path.Resolved
	var err error

	path, err = self.GetCRDTManager().AddToIPFS(self.dag.Sys, bytes)
	if err != nil {
		panic(fmt.Errorf("Error in callAddToIPFS, Couldn't add file to IPFS\nError: %s\n", err))
	}

	return path, err
}

func (self *CRDTEmptyExempleOpBasedDag) OP1(x string) string {
	newNode := CreateDagNode(Operation{Elem: Element(x), Op: OP1}, self.GetSys().IpfsNode.Identity.Pretty())
	for dependency := range self.dag.Root_nodes {
		newNode.DagNode.DirectDependency = append(newNode.DagNode.DirectDependency, self.dag.Root_nodes[dependency])
	}

	strFile := self.dag.NextFileName()
	if _, err := os.Stat(strFile); !errors.Is(err, os.ErrNotExist) {
		os.Remove(strFile)
	}
	newNode.ToFile(strFile)
	bytes, err := os.ReadFile(strFile)
	if err != nil {
		panic(fmt.Errorf("ERROR INCREMENT CRDTSetOpBasedDag, could not read file\nerror: %s", err))
	}
	path, err := self.callAddToIPFS(bytes, strFile)
	if err != nil {
		panic(fmt.Errorf("CRDTSetOpBasedDag Increment, could not add the file to IFPS\nerror: %s", err))
	}
	encodedCid := self.dag.EncodeCid(path)
	c := cid.Cid{}
	err = json.Unmarshal(encodedCid.Str, &c)
	if err != nil {
		panic(fmt.Errorf("CRDTSetOpBasedDag Increment, could not UnMarshal\nerror: %s", err))
	}
	var pl CRDTDag.CRDTDagNodeInterface = &newNode
	// Adding the node created before to the Merkle-DAG
	self.dag.AddNode(encodedCid, &pl)
	// Op-Based force us to send updates to other at each update
	self.SendRemoteUpdates()
	return c.String()
}

func Create_CRDTSetOpBasedDag(sys *IpfsLink.IpfsLink, cfg Config.IM_CRDTConfig) CRDTEmptyExempleOpBasedDag {
	man := CRDTDag.Create_CRDTManager(sys, cfg.PeerName, cfg.BootstrapPeer, cfg.Encode, false)
	crdtSet := CRDTEmptyExempleOpBasedDag{dag: man}

	if cfg.BootstrapPeer == "" {
		x, err := os.ReadFile("initial_value")
		if err != nil {
			panic(fmt.Errorf("Could not read initial_value, error : %s", err))
		}
		newNode := CreateDagNode(Operation{Elem: Element(x), Op: OP1}, crdtSet.GetSys().IpfsNode.Identity.Pretty())
		strFile := crdtSet.dag.NextFileName()

		if _, err := os.Stat(strFile); !errors.Is(err, os.ErrNotExist) {
			os.Remove(strFile)
		}
		newNode.ToFile(strFile)
		bytes, err := os.ReadFile(strFile)
		if err != nil {
			panic(fmt.Errorf("ERROR INCREMENT CRDTSetOpBasedDag, could not read file\nerror: %s", err))
		}

		// Add Inital State ( so it isn't counted as messages)
		path, err := man.AddToIPFS(crdtSet.dag.Sys, bytes)
		if err != nil {
			panic(fmt.Errorf("CRDTSetOpBasedDag Increment, could not add the file to IFPS\nerror: %s", err))
		}

		encodedCid := crdtSet.dag.EncodeCid(path)
		c := cid.Cid{}
		err = json.Unmarshal(encodedCid.Str, &c)
		if err != nil {
			panic(fmt.Errorf("CRDTSetOpBasedDag Increment, could not UnMarshal\nerror: %s", err))
		}
		var pl1 CRDTDag.CRDTDagNodeInterface = &newNode

		crdtSet.dag.AddNode(encodedCid, &pl1)
	}

	var pl CRDTDag.CRDTDag = &crdtSet

	CRDTDag.CheckForRemoteUpdates(&pl, sys.Cr.Sub, man.Sys.Ctx)

	return crdtSet
}

func (self *CRDTEmptyExempleOpBasedDag) Lookup() CRDTEmptyExempleOpBased {

	// crdt := self.logokup_ToSpecifyType()
	// var pl CRDTDag.CRDTDag = &crdtSet
	return *(*self.Lookup_ToSpecifyType()).(*CRDTEmptyExempleOpBased)
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
		panic(fmt.Errorf("Semaphore of READ/WRITE file locked !!!!\n Cannot acquire it\n"))
	}
}

func returnSema(sema *semaphore.Weighted) {
	sema.Release(1)
}

// Check update function retrieve files from ipfs (long)
// and then reserves the semaphore to actually modify the data (short)
func (self *CRDTEmptyExempleOpBasedDag) CheckUpdate(sema *semaphore.Weighted) {
	files, err := ioutil.ReadDir(self.GetDag().Nodes_storage_enplacement + "/remote")
	if err != nil {
		fmt.Printf("CheckUpdate - Checkupdate could not open folder\nerror: %s\n", err)
	} else {
		to_add := make([]([]byte), 0)
		for _, file := range files {
			// if the file exists, and is not currently being written
			if file.Size() > 0 && !strings.Contains(file.Name(), ".ArrivalTime") {
				// open the file and read it
				fil, err := os.OpenFile(self.GetDag().Nodes_storage_enplacement+"/remote/"+file.Name(), os.O_RDONLY, os.ModeAppend)
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not open the sub file\nError: %s", err))
				}
				stat, err := fil.Stat()
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not get stat the sub file\nError: %s", err))
				}
				bytesread := make([]byte, stat.Size())
				n, err := fil.Read(bytesread)
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not read the sub file\nError: %s", err))
				}
				if int64(n) != stat.Size() {
					panic(fmt.Errorf("error in checkupdate, Could not read entirely the sub file\nError: read %d byte unstead of %d", n, stat.Size()))
				}
				err = fil.Close()
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not close the sub file\nError: %s", err))
				}

				// transfer the content of the file into a CID, with json.Unmarshal
				s := cid.Cid{}
				json.Unmarshal(bytesread, &s)
				fil, err = os.OpenFile(self.GetDag().Nodes_storage_enplacement+"/remote/"+file.Name()+".ArrivalTime", os.O_RDONLY, os.ModeAppend)
				stat, _ = fil.Stat()
				if err == nil && stat.Size() != 0 {
					if !self.IsKnown(CRDTDag.EncodedStr{Str: bytesread}) {
						to_add = append(to_add, bytesread)
					}
					err = os.Remove(self.GetDag().Nodes_storage_enplacement + "/remote/" + file.Name())
					if err != nil || errors.Is(err, os.ErrNotExist) {
						panic(fmt.Errorf("error in checkupdate, Could not remove the sub file\nError: %s", err))
					}

					bytesread = make([]byte, stat.Size())
					n, err = fil.Read(bytesread)
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

				} else {
					fmt.Printf("File doesn't exists yet, ignoring and waiting")
				}
			} else {
				fmt.Printf("Remote folder contain a FILE of a NULL SIZE\n")
			}
		}

		// apply the update on the peer's data
		getSema(sema, self.GetSys().Ctx)
		self.add_cids(to_add)
		returnSema(sema)

		if len(to_add) > 0 {
			getSema(sema, self.GetSys().Ctx)
			self.GetDag().UpdateRootNodeFolder()
			returnSema(sema)
		}
	}
}

func checkFileExists(filePath string) bool {
	_, error := os.Stat(filePath)
	//return !os.IsNotExist(err)
	return !errors.Is(error, os.ErrNotExist)
}

func Index(stringmap []([]byte), elem []byte) int {
	for i, x := range stringmap {
		if string(x) == string(elem) {
			return i
		}
	}
	return -1

}

// Check update function retrieve files from ipfs (long)
// and then reserves the semaphore to actually modify the data (short)
func (self *CRDTEmptyExempleOpBasedDag) CheckUpdate_20Version(sema *semaphore.Weighted) {
	fileRead, err := os.OpenFile(self.GetDag().Nodes_storage_enplacement+"/time/FileBIS.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		fmt.Printf("CheckUpdate - Cannot create FileBIS: %s\n", err)
	}
	files, err := ioutil.ReadDir(self.GetDag().Nodes_storage_enplacement + "/remote")
	if err != nil {
		fmt.Printf("CheckUpdate - Checkupdate could not open folder\nerror: %s\n", err)
	} else {
		to_add := make([]([]byte), 0)
		cpt := 0
		fileRead.WriteString("===================================================================================================\n")
		fileRead.WriteString("Starting to read the folder\n")
		willBeAdded := make([]([]byte), 0)
		for _, file := range files {
			if file.Size() > 0 {
				fil, err := os.OpenFile(self.GetDag().Nodes_storage_enplacement+"/remote/"+file.Name(), os.O_RDONLY, os.ModeAppend)
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not open the sub file\nError: %s", err))
				}
				stat, err := fil.Stat()
				if err != nil {
					panic(fmt.Errorf("error in checkupdate, Could not get stat the sub file\nError: %s", err))
				}
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
				if !self.IsKnown(CRDTDag.EncodedStr{Str: bytesread}) && Index(willBeAdded, bytesread) == -1 {
					cpt = cpt + 1
					willBeAdded = append(willBeAdded, bytesread)

				}
				if cpt >= 20 {
					break
				}
			}
		}

		fileRead.WriteString(fmt.Sprintf("after reading files, cpt value is %d\n", cpt))
		if cpt >= 20 {

			cpt = 0
			fileRead.WriteString(fmt.Sprintf("we're in \n"))
			for _, file := range files {
				if file.Size() > 0 {
					fil, err := os.OpenFile(self.GetDag().Nodes_storage_enplacement+"/remote/"+file.Name(), os.O_RDONLY, os.ModeAppend)
					if err != nil {
						panic(fmt.Errorf("error in checkupdate, Could not open the sub file\nError: %s", err))
					}
					stat, err := fil.Stat()
					if err != nil {
						panic(fmt.Errorf("error in checkupdate, Could not get stat the sub file\nError: %s", err))
					}
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
					if !self.IsKnown(CRDTDag.EncodedStr{Str: bytesread}) {
						cpt = cpt + 1
						to_add = append(to_add, bytesread)
					}
					s := cid.Cid{}
					json.Unmarshal(bytesread, &s)

					err = os.Remove(self.GetDag().Nodes_storage_enplacement + "/remote/" + file.Name())
					if err != nil || errors.Is(err, os.ErrNotExist) {
						panic(fmt.Errorf("error in checkupdate, Could not remove the sub file\nError: %s", err))
					}

					// Take the time measurement of this file
					// Get the time of arrival to compute pubsub time
					fil, err = os.OpenFile(self.GetDag().Nodes_storage_enplacement+"/remote/"+file.Name()+".ArrivalTime", os.O_RDONLY, os.ModeAppend)
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

					// fmt.Println("stat.size :", stat.Size(), "read :", n)
					if int64(n) != stat.Size() {
						panic(fmt.Errorf("error in checkupdate, Could not read entirely the sub file\nError: read %d byte unstead of %d", n, stat.Size()))
					}
					err = fil.Close()
					if err != nil {
						panic(fmt.Errorf("error in checkupdate, Could not close the sub file\nError: %s", err))
					}
					if cpt >= 20 {
						break
					}

				} else {
					fmt.Printf("Remote folder contain a FILE of a NULL SIZE\n")
				}
			}
			// apply the update on the peer's data
			getSema(sema, self.GetSys().Ctx)

			if len(to_add) > 0 {
				self.GetDag().UpdateRootNodeFolder()
			}
			fileRead.WriteString(fmt.Sprintf("Returning %d values\n", len(to_add)))

			returnSema(sema)
		}
	}
}

func (self *CRDTEmptyExempleOpBasedDag) add_cids(to_add []([]byte)) {

	bytes_encoded := make([]CRDTDag.EncodedStr, 0)

	for _, bytesread := range to_add {
		bytes_encoded = append(bytes_encoded, CRDTDag.EncodedStr{Str: bytesread})
	}

	self.Merge(bytes_encoded)

	self.GetDag().UpdateRootNodeFolder()
}
