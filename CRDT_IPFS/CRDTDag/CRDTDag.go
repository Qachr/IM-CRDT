package CRDTDag

import (
	CRDT "IPFS_CRDT/Crdt"
	IPFSLink "IPFS_CRDT/ipfsLink"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	IpfsLink "IPFS_CRDT/ipfsLink"

	"github.com/ipfs/go-cid"
	Files "github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/interface-go-ipfs-core/path"
	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"golang.org/x/sync/semaphore"
)

/*
 *
 *
 *
 *
 * Encryption and decryption function
 * Taken from :
 * https://www.golinuxcloud.com/golang-encrypt-decrypt/
 *
 *
 */
func encrypt(keyString string, stringToEncrypt string) (encryptedString string) {
	// convert key to bytes
	key, _ := hex.DecodeString(keyString)
	plaintext := []byte(stringToEncrypt)

	//Create a new Cipher Block from the key
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}

	// The IV needs to be unique, but not secure. Therefore it's common to
	// include it at the beginning of the ciphertext.
	ciphertext := make([]byte, aes.BlockSize+len(plaintext))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		panic(err)
	}

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], plaintext)

	// convert to base64
	return base64.URLEncoding.EncodeToString(ciphertext)
}

// decrypt from base64 to decrypted string
func decrypt(keyString string, stringToDecrypt string) string {
	key, _ := hex.DecodeString(keyString)
	ciphertext, _ := base64.URLEncoding.DecodeString(stringToDecrypt)

	block, err := aes.NewCipher(key)
	if err != nil {
		panic(fmt.Errorf("Couldn't Create a Cipher !!!\nError: %s\n", err))
	}

	// The IV needs to be unique, but not secure. Therefore it's common to
	// include it at the beginning of the ciphertext.
	if len(ciphertext) < aes.BlockSize {
		panic(fmt.Errorf("ciphertext too short, here is the Key :%s\n", keyString))
	}
	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, iv)

	// XORKeyStream can work in-place if the two arguments are the same.
	stream.XORKeyStream(ciphertext, ciphertext)

	return fmt.Sprintf("%s", ciphertext)
}

///==============================================================
/// CRDTDag definitions
///==============================================================

type CRDTDag interface {
	Lookup_ToSpecifyType() *CRDT.CRDT
	SendRemoteUpdates()
	Merge(cid []EncodedStr) []string
	GetSys() *IPFSLink.IpfsLink
	GetCRDTManager() *CRDTManager
}
type CRDTManager struct {
	Root_nodes                []EncodedStr
	nodesId                   []([]byte)
	nodesInterface            []*CRDTDagNodeInterface
	Nodes_storage_enplacement string
	pubsubTopic               string
	checkfile                 string
	SubscribedFile            string
	sign                      string
	Key                       string
	nodesToAdd_Key            []EncodedStr
	nodesToAdd_value          []*CRDTDagNodeInterface
	nbLineAlreadyWritten      int
	nextNodeName              int
	nextNodeNameBis           int
	Sys                       *IPFSLink.IpfsLink
	retrieveMode              bool
	measurement               bool
	SemaphoreRootFolderWrite  semaphore.Weighted
}

func (self *CRDTManager) GetSys() *IPFSLink.IpfsLink {
	return self.Sys
}
func Create_CRDTManager(s *IPFSLink.IpfsLink, storage string, bootStrapPeer string, key string, measurement bool) CRDTManager {
	inkey := ""
	if key != "" {
		inkey = hex.EncodeToString([]byte(key))
	}
	crdt := CRDTManager{
		Root_nodes:                make([]EncodedStr, 0),
		nodesId:                   make([]([]byte), 0),
		nodesInterface:            make([]*CRDTDagNodeInterface, 0),
		Nodes_storage_enplacement: storage,
		SubscribedFile:            "",
		checkfile:                 "",
		sign:                      "",
		pubsubTopic:               "",
		Key:                       inkey,
		nodesToAdd_Key:            make([]EncodedStr, 0),
		nodesToAdd_value:          make([]*CRDTDagNodeInterface, 0),
		nbLineAlreadyWritten:      0,
		nextNodeName:              0,
		nextNodeNameBis:           0,
		Sys:                       s,
		retrieveMode:              true,
		measurement:               measurement,
		SemaphoreRootFolderWrite:  *semaphore.NewWeighted(1),
	}
	fmt.Println("storage : ", crdt.Nodes_storage_enplacement)
	crdt.SubscribedFile = crdt.nextFileName2()
	if _, err := os.Stat(crdt.SubscribedFile); !errors.Is(err, os.ErrNotExist) {
		os.Remove(crdt.SubscribedFile)
	}
	// if bootStrapPeer == "" {
	// 	crdt.ManageRootNodesConnexion(bootStrapPeer, storage+"rootNode")
	// } else {
	// 	crdt.ManageRootNodesConnexion(bootStrapPeer, storage+"remote")
	// }
	return crdt
}

type EncodedStr struct {
	Str []byte
}

func (self *CRDTManager) GetAllNodes() [][]byte {

	return self.nodesId
}
func (self *CRDTManager) GetAllNodesInterface() []*CRDTDagNodeInterface {
	return self.nodesInterface
}
func (self CRDTManager) EncodeCid(s path.Resolved) EncodedStr {
	b, err := json.Marshal(s.Cid())
	if err != nil {
		panic(fmt.Errorf("Couldn't marshall the path, byte :\nerror : %s", err))
	}
	x := EncodedStr{Str: b}
	return x
}

func (self *CRDTManager) GetNodeFromEncodedCid(stringIn []EncodedStr) ([]string, error) {
	ti := time.Now()
	Cids := make([]cid.Cid, len(stringIn))

	for index, s := range stringIn {
		cid := cid.Cid{}
		err := json.Unmarshal(s.Str, &cid)
		if err != nil {
			panic(fmt.Errorf("couldn't unmarshall the path, byte :%s \nerror : %s", s.Str, err))
		}
		Cids[index] = cid
	}

	fils, err := IPFSLink.GetIPFS(self.Sys, Cids)
	if err != nil {
		panic(fmt.Errorf("issue retrieving the IPFS Node :%s", err))
	}
	filees_ret := make([]string, 0)
	timeDownload := 0
	if len(fils) > 0 {
		timeDownload = int(time.Since(ti).Nanoseconds()) / len(fils)
	}
	for _, fil := range fils {
		ti := time.Now()
		fstr := self.nextFileName2()
		_ = os.Remove(fstr) // In cas the file where already existing ( which should never be the case)

		filees_ret = append(filees_ret, fstr)

		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		Files.WriteTo(fil, fstr)
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!! (botleneck for mispelling)25
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!
		// THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!  THIS IS THE BOTTLENECK DIscuss about it !!!!!

		time_Retrieve := timeDownload + int(time.Since(ti).Nanoseconds())
		// If data has been encoded, We decode it here : \/
		ti = time.Now()
		if self.Key != "" {
			dataEncoded, err := os.ReadFile(fstr)
			if err != nil {
				panic(fmt.Errorf("error, could not read data to decrypt it\nError: %s", err))
			}
			dataClear := decrypt(self.Key, string(dataEncoded))

			os.Remove(fstr)
			if _, err := os.Stat(fstr); !errors.Is(err, os.ErrNotExist) {
				os.Remove(fstr)
			}
			fil, err := os.OpenFile(fstr, os.O_CREATE|os.O_WRONLY, 0755)
			if err != nil {
				panic(fmt.Errorf("Error RemoteAddNodeSupde - , Could not open the sub file to write encoded data\nError: %s", err))
			}
			_, err = fil.Write([]byte(dataClear))
			if err != nil {
				panic(fmt.Errorf("Error RemoteAddNodeSupde - , Could not write the sub file to write encoded data\nError: %s", err))
			}
			err = fil.Close()
			if err != nil {
				panic(fmt.Errorf("Error RemoteAddNodeSupde - , Could not close the sub file to write encoded data \nError: %s", err))
			}
		}
		time_decrypt := time.Since(ti).Nanoseconds()

		// Measurement matters bellow
		if self.measurement {
			fstrBis := fstr + ".timeRetrieve"
			if _, err := os.Stat(fstrBis); !errors.Is(err, os.ErrNotExist) {
				os.Remove(fstrBis)
			}
			fil, err := os.OpenFile(fstrBis, os.O_CREATE|os.O_WRONLY, 0755)
			if err != nil {
				panic(fmt.Errorf("Error RemoteAddNodeSupde - , Could not open the time file to write encoded data\nError: %s", err))
			}
			_, err = fil.Write([]byte(strconv.Itoa(time_Retrieve)))
			if err != nil {
				panic(fmt.Errorf("Error RemoteAddNodeSupde - , Could not write the time file to write encoded data\nError: %s", err))
			}
			err = fil.Close()
			if err != nil {
				panic(fmt.Errorf("Error RemoteAddNodeSupde - , Could not close the time file to write encoded data \nError: %s", err))
			}
			if self.Key != "" {
				fstrBis = fstr + ".timeDecrypt"
				if _, err := os.Stat(fstrBis); !errors.Is(err, os.ErrNotExist) {
					os.Remove(fstrBis)
				}
				fil, err = os.OpenFile(fstrBis, os.O_CREATE|os.O_WRONLY, 0755)
				if err != nil {
					panic(fmt.Errorf("Error RemoteAddNodeSupde - , Could not open the time file to write encoded data\nError: %s", err))
				}
				_, err = fil.Write([]byte(strconv.Itoa(int(time_decrypt))))
				if err != nil {
					panic(fmt.Errorf("Error RemoteAddNodeSupde - , Could not write the time file to write encoded data\nError: %s", err))
				}
				err = fil.Close()
				if err != nil {
					panic(fmt.Errorf("Error RemoteAddNodeSupde - , Could not close the time file to write encoded data \nError: %s", err))
				}
			}
		}

	}

	return filees_ret, nil
}

// / @brief Creation of a new empty CRDT Counter in the Operation-based principle
// / @param nodes_stor A free folder where it's possible to write files. The  System will write the nodes here.
// / @param s IPFS System linkin you to the IPFS network, it have to be initialized
func (self *CRDTManager) InitCRDTManager(folderNodeStorage string, s *IPFSLink.IpfsLink, signature int) {

	self.Nodes_storage_enplacement = folderNodeStorage
	self.checkfile = self.nextFileName2()
	self.Sys = s
	self.retrieveMode = true
	// self.sign = self.Sys.Hst. Sys HgetID() + "_" + std::to_string(signature)

	self.pubsubTopic = self.sign + "CRDT_" + time.Now().String()
	self.SubscribedFile = folderNodeStorage + "/" + self.pubsubTopic + self.sign + ".data"
	//self.Sys should be subscribed by default
	self.nbLineAlreadyWritten = 0
}

func (self *CRDTManager) NextFileName() string {
	remove_to_save_space := true
	if remove_to_save_space {

		files, err := ioutil.ReadDir(self.Nodes_storage_enplacement)
		if err != nil {
			panic(fmt.Errorf("UpdateRootNodeFolder could not open folder\nError: %s", err))
		}

		for _, file := range files {
			if !file.IsDir() {
				os.Remove(file.Name())
			}
		}
	}
	res := self.Nodes_storage_enplacement + "/node" + strconv.Itoa(self.nextNodeName)
	self.nextNodeName += 1
	return res
}

func (self *CRDTManager) nextFileName2() string {
	remove_to_save_space := true
	if remove_to_save_space {

		files, err := ioutil.ReadDir(self.Nodes_storage_enplacement)
		if err != nil {
			panic(fmt.Errorf("UpdateRootNodeFolder could not open folder\nError: %s", err))
		}

		for _, file := range files {
			if !file.IsDir() {
				os.Remove(file.Name())
			}
		}
	}
	res := self.Nodes_storage_enplacement + "/node" + strconv.Itoa(self.nextNodeNameBis)
	self.nextNodeNameBis += 1
	return res
}

func (self *CRDTManager) NextRemoteFileName() string {

	res := self.Nodes_storage_enplacement + "/remote/" + strconv.Itoa(self.nextNodeName)
	self.nextNodeName += 1
	return res
}

func (self *CRDTManager) IsKnown(bytes []byte) bool {
	for x := range self.nodesId {
		if string(self.nodesId[x]) == string(bytes) {
			return true
		}
	}
	return false
}

func (self *CRDTManager) getSema() {
	t := time.Now()
	err := self.SemaphoreRootFolderWrite.Acquire(self.Sys.Ctx, 1)
	for err != nil && time.Since(t) < 10*time.Second {
		time.Sleep(10 * time.Microsecond)
		err = self.SemaphoreRootFolderWrite.Acquire(self.Sys.Ctx, 1)
	}

	if err != nil {
		panic(fmt.Errorf("Semaphore of RootFolder locked !!!!\n Cannot acquire it\n"))
	}
}

func (self *CRDTManager) returnSema() {
	self.SemaphoreRootFolderWrite.Release(1)

}
func (self *CRDTManager) UpdateRootNodeFolder() {
	// Get the semaphore "Permission" to modify the Root Node FOlder (In case another files wants so)
	self.getSema()
	files, err := ioutil.ReadDir(self.Nodes_storage_enplacement + "/rootNode/")
	t := time.Now()
	for (err != nil) && (time.Since(t) < 500*time.Millisecond) {
		time.Sleep(time.Millisecond)
		files, err = ioutil.ReadDir(self.Nodes_storage_enplacement + "/rootNode/")
	}
	if err != nil {
		panic(fmt.Errorf("UpdateRootNodeFolder could not open folder\nError: %s", err))
	}

	for _, file := range files {
		if file.Size() > 0 {
			fil, err := os.Open(self.Nodes_storage_enplacement + "/rootNode/" + file.Name())
			t = time.Now()
			for (err != nil) && (time.Since(t) < 500*time.Millisecond) {
				time.Sleep(time.Millisecond)
				fil, err = os.Open(self.Nodes_storage_enplacement + "/rootNode/" + file.Name())
			}
			if err != nil {
				panic(fmt.Errorf("UPDATE - 1 could Not Open RootNode %s to update rootnodefolder\nerror: %s", file.Name(), err))
			}
			stat, err := fil.Stat()
			t = time.Now()
			for (err != nil) && (time.Since(t) < 500*time.Millisecond) {
				time.Sleep(time.Millisecond)
				stat, err = fil.Stat()
			}
			if err != nil {
				panic(fmt.Errorf("UPDATE - error in UpdateRootNode, Could not get stat the sub file\nError: %s", err))
			}
			bytesread := make([]byte, stat.Size())
			_, err = fil.Read(bytesread)
			t = time.Now()
			for (err != nil) && (time.Since(t) < 500*time.Millisecond) {
				time.Sleep(time.Millisecond)
				_, err = fil.Read(bytesread)
			}
			if err != nil {
				panic(fmt.Errorf("UPDATE - error in UpdateRootNode, Could not read the sub file\nError: %s", err))
			}
			err = fil.Close()
			t = time.Now()
			for (err != nil) && (time.Since(t) < 500*time.Millisecond) {
				time.Sleep(time.Millisecond)
				err = fil.Close()
			}
			if err != nil {
				panic(fmt.Errorf("UPDATE - error in UpdateRootNode, Could not close the sub file\nError: %s", err))
			}
			if self.IsKnown(bytesread) {
				err = os.Remove(self.Nodes_storage_enplacement + "/rootNode/" + file.Name())

				t = time.Now()
				for (err != nil) && (time.Since(t) < 1000*time.Millisecond) {
					time.Sleep(time.Millisecond)
					err = os.Remove(self.Nodes_storage_enplacement + "/rootNode/" + file.Name())
				}
				if err != nil {
					panic(fmt.Errorf("UPDATE -error in UpdateRootNodeFolder, Could not remove the known file\nError: %s", err))
				}
			}
		}
	}

	for n := range self.Root_nodes {
		fileName := self.Nodes_storage_enplacement + "/rootNode/" + fmt.Sprintf("root%d", self.nextNodeName)
		self.nextNodeName += 1
		fil, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0755)
		if err != nil {
			panic(fmt.Errorf("UPDATE - 2 could Not Open RootNode to update rootnodefolder\nerror: %s", err))
		}
		_, err = fil.Write(self.Root_nodes[n].Str)
		if err != nil {
			panic(fmt.Errorf("could Not write in RootNode to update rootnodefolder\nerror: %s", err))
		}
		err = fil.Close()
		if err != nil {
			panic(fmt.Errorf("could Not Close RootNode to update rootnodefolder\nerror: %s", err))
		}
	}

	// Release the Semaphore so others can work
	self.returnSema()
}

func (self *CRDTManager) AddRoot_node(nodeId EncodedStr, node *CRDTDagNodeInterface) {
	self.getSema()
	self.Root_nodes = append(self.Root_nodes, nodeId)
	self.returnSema()
	self.UpdateRootNodeFolder()
}

func (self *CRDTManager) RemoveRoot_node(nodeId EncodedStr) {
	self.getSema()
	i := -1
	for x := range self.Root_nodes {
		if string(self.Root_nodes[x].Str) == string(nodeId.Str) {
			i = x
			break
		}
	}
	if i != -1 {
		self.Root_nodes[i] = self.Root_nodes[len(self.Root_nodes)-1]
		self.Root_nodes = self.Root_nodes[:len(self.Root_nodes)-1]
	}
	self.returnSema()
	self.UpdateRootNodeFolder()
}

// /// @brief add the node with the IPFS node ID @node corresponding to @d, assuming no unknown dependance but manage the local root nodes
// /// @param node the node ID to add
// /// @param d The Node itself, with event (data) and direct dependencies (sons)
func (self *CRDTManager) AddNode(node EncodedStr, d *CRDTDagNodeInterface) {

	self.nodesId = append(self.nodesId, node.Str)
	self.nodesInterface = append(self.nodesInterface, d)
	if len((*d).GetDirect_dependency()) > 0 {
		for j := range (*d).GetDirect_dependency() {
			self.RemoveRoot_node((*d).GetDirect_dependency()[j])
		}
	}

	self.AddRoot_node(node, d)
}

func (self *CRDTManager) RemoteAddNodeSuper(cID EncodedStr, newnode *CRDTDagNodeInterface) {

	toDl := make([]EncodedStr, 0)
	fmt.Println("=======\nRemote add note\n=======\nPeer:", self.GetSys().Cr.Name, "\nEvent:", (*(*newnode).GetEvent()).ToString(), "\nDirect Dependency:", (*newnode).GetDirect_dependency())
	if self.retrieveMode {
		// newNodeFile := ""

		for node := range (*newnode).GetDirect_dependency() {
			if !self.IsKnown((*newnode).GetDirect_dependency()[node].Str) {
				toDl = append(toDl, (*newnode).GetDirect_dependency()[node])
			}
		}

		fils, err := self.GetNodeFromEncodedCid(toDl)

		for index := range toDl {
			fil := fils[index]
			var nn *CRDTDagNodeInterface = (*newnode).CreateEmptyNode()
			if err != nil {
				panic(fmt.Errorf("RemoteAddNodeSuper - DeepCopy failed on newnode\nError: %s", err))
			}

			(*nn).FromFile(fil)

			self.RemoteAddNodeSuper(toDl[index], nn)
		}
		self.AddNode(cID, newnode)
	} else {
		knowAllDependency := true

		// first check if all dependency are resolved
		for node := range (*newnode).GetDirect_dependency() {
			exists := false
			for x := range self.nodesId {
				if string(self.nodesId[x]) == string((*newnode).GetDirect_dependency()[node].Str) {
					exists = true
				}
			}

			// If the key exists
			if exists {
				knowAllDependency = false
				break
			}
		}

		// then Add it weather to :
		// actual node if you have all dependency
		if knowAllDependency {
			stack_EncStr := make([]EncodedStr, 0)
			stack_CRDTDagNode := make([]*CRDTDagNodeInterface, 0)

			stack_EncStr = append(stack_EncStr, cID)
			stack_CRDTDagNode = append(stack_CRDTDagNode, newnode)
			for len(stack_EncStr) > 0 {
				key := stack_EncStr[0]
				actualNode := stack_CRDTDagNode[0]

				//Deleting the first elemnt of the stack ( ^pop)
				stack_CRDTDagNode = stack_CRDTDagNode[1:]
				stack_EncStr = stack_EncStr[:1]
				self.AddNode(key, actualNode)

				for i := range self.nodesToAdd_Key {
					toadd_key := self.nodesToAdd_Key[i]
					toadd_value := self.nodesToAdd_value[i]
					knowAllDependency = true
					for k := range (*toadd_value).GetDirect_dependency() {
						exists := false
						for x := range self.nodesId {
							if string(self.nodesId[x]) == string((*toadd_value).GetDirect_dependency()[k].Str) {
								exists = true
							}
						}
						// If the key exists
						if !exists {
							knowAllDependency = false
							break
						}
					}
					if knowAllDependency {

						stack_CRDTDagNode = append(stack_CRDTDagNode, toadd_value)
						stack_EncStr = append(stack_EncStr, toadd_key)

						self.nodesToAdd_Key[i] = self.nodesToAdd_Key[len(self.nodesToAdd_Key)-1]
						self.nodesToAdd_Key = self.nodesToAdd_Key[:len(self.nodesToAdd_Key)-1]
						self.nodesToAdd_value[i] = self.nodesToAdd_value[len(self.nodesToAdd_value)-1]
						self.nodesToAdd_value = self.nodesToAdd_value[:len(self.nodesToAdd_value)-1]
					}
				}
			}
		} else { // waiting other node to add other people
			self.nodesToAdd_Key = append(self.nodesToAdd_Key, cID)
			self.nodesToAdd_value = append(self.nodesToAdd_value, newnode)
		}

	}

	self.UpdateRootNodeFolder()

}
func (self *CRDTManager) AddToIPFS(ipfs *IpfsLink.IpfsLink, message []byte, args ...*int) (path.Resolved, error) {
	ti := time.Now()
	if self.Key != "" {
		message = []byte(encrypt(self.Key, string(message)))
	}
	time_Encrypt := int(time.Since(ti).Nanoseconds()) // Wrtie this encrypt time in optionnall results, if there are some
	if len(args) > 0 {
		*args[0] = time_Encrypt
	}
	path, err := IpfsLink.AddIPFS(ipfs, message)
	if err != nil {
		panic(fmt.Errorf("CRDTSetOpBasedDag Increment, could not add the file to IFPS\nerror: %s", err))
	}
	return path, err
}

func (self *CRDTManager) SendRemoteUpdates() {

	// Lock the data so we can read it with no modification
	self.getSema()
	x := make([]([]byte), len(self.Root_nodes))
	for i := range self.Root_nodes {
		x[i] = self.Root_nodes[i].Str
	}

	self.returnSema()

	// Publish with IPFS the state we read after releasing the semaphore,
	// Like so we the rest of the algorithm isn't locked for a long time (time to send)
	for i := range x {
		IPFSLink.PubIPFS(self.Sys, x[i])
	}
}

func CheckForRemoteUpdates(self *CRDTDag, sub *pubsub.Subscription, c context.Context) {
	go func() {
		for {
			msg, err := sub.Next(c)
			if err != nil {

				panic(fmt.Errorf("Check For remote update failed, message not received\nError: %s", err))
			} else if msg.ReceivedFrom == (*self).GetSys().Cr.Host.ID() {
				fmt.Println("Received message from myself")
				continue
			} else {
				fmt.Println("Received message from", msg.ReceivedFrom,
					"data:", string(msg.Data))
				fileName := (*self).GetCRDTManager().NextRemoteFileName()
				if _, err := os.Stat(fileName); !errors.Is(err, os.ErrNotExist) {
					os.Remove(fileName)
				}
				// Write the received CID in a file
				fil, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0755)
				if err != nil {
					panic(fmt.Errorf("error in checkForRemoteUpdate, Could not open the sub file\nError: %s", err))
				}
				_, err = fil.Write(msg.GetData())
				if err != nil {
					panic(fmt.Errorf("error in checkForRemoteUpdate, Could not write the sub file\nError: %s", err))
				}
				err = fil.Close()
				if err != nil {
					panic(fmt.Errorf("error in checkForRemoteUpdate, Could not close the sub file\nError: %s", err))
				}

				// Then mark arrival time To compute pubsub time
				fil, err = os.OpenFile(fileName+".ArrivalTime", os.O_CREATE|os.O_WRONLY, 0755)
				if err != nil {
					panic(fmt.Errorf("error in checkForRemoteUpdate, Could not open the sub file\nError: %s", err))
				}
				t_arrival := time.Now().UnixNano()
				_, err = fil.WriteString(fmt.Sprintf("%d", t_arrival))
				if err != nil {
					panic(fmt.Errorf("error in checkForRemoteUpdate, Could not write the sub file\nError: %s", err))
				}
				err = fil.Close()
				if err != nil {
					panic(fmt.Errorf("error in checkForRemoteUpdate, Could not close the sub file\nError: %s", err))
				}
			}
		}
	}()
}

func (self *CRDTManager) getTopic() string {

	return self.pubsubTopic
}

func (self *CRDTManager) ToString() string {

	str := ""
	str += "nodes : {\n    "
	for s := range self.nodesId {
		str += string(self.nodesId[s]) + " - dd:{"
		for sons := range (*self.nodesInterface[s]).GetDirect_dependency() {
			str += string((*self.nodesInterface[s]).GetDirect_dependency()[sons].Str) + " "
		}
		str += "}\n    "
	}
	str += "}\nRoot_Nodes : {"
	for s := range self.Root_nodes {
		str += string(self.Root_nodes[s].Str) + " "
	}
	str += "}\n"
	return str
}

//TODO : Specify that CID must not be Clear but encoded, so it can be well decrypted by others. ( the only good construction method of Node i Found)
