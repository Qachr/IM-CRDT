package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
)

/* The code in main.go deals with the replica interaction / connection in our system.
For the moment, it is based on a server as connection is established through tcp but
can be extended or modified to mimic peer to peer network with no server.
NB: server does not make the decisions, coinflict resolution is based on fs.go with Mehdi Ahmed-Nacer
logic, not on a server acting as a master.  */

// Overall logic of code
// Provides command-line interface for interacting with replica
func main() {
	port := flag.String("port", "8001", "Port to listen on")
	peersCSV := flag.String("peers", "", "Comma-separated peer ports or addresses (e.g. 8002,8003)")
	flag.Parse()

	host := "127.0.0.1"
	peers := []string{}
	if *peersCSV != "" {
		for _, p := range strings.Split(*peersCSV, ",") {
			p = strings.TrimSpace(p)
			if p != "" && !strings.Contains(p, ":") {
				p = host + ":" + p
			}
			peers = append(peers, p)
		}
	}

	replica := &Replica{
		ID:       uuid.New().String(),
		NodeSet:  NewNodeSetCRDT(),
		LastSeen: make(map[string]int64),
		OpLog:    []Operation{},
		Peers:    peers,
		Addr:     host + ":" + *port,
	}

	fmt.Println("Replica ID:", replica.ID)

	replica.AddNode("/", "root", "", Dir)

	// Start server to receive merges
	go startServer(*port, replica)

	// Start periodic merge with peer
	go func() {
		for {
			time.Sleep(120 * time.Second) // gossip interval
			gossipToPeers(replica, "")
		}
	}()

	// CLI input
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Enter command (add <path> <type> | remove <path> <type> | print | final (+policy)): ")
		scanner.Scan()
		input := scanner.Text()
		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {

		case "add":
			if len(parts) < 3 {
				fmt.Println("Usage: add <path> <dir|file>")
				continue
			}
			pathStr := parts[1]
			typeStr := parts[2]
			var nodeType NodeType
			if strings.EqualFold(typeStr, "dir") {
				nodeType = Dir
			} else {
				nodeType = File
			}
			parent := path.Dir(pathStr)
			name := path.Base(pathStr)
			replica.AddNode(pathStr, name, parent, nodeType)
			fmt.Printf("[ADD] Added %s of type %s under parent %s: %s \n", name, nodeType, parent, pathStr)
			gossipToPeers(replica, "")

		case "remove":
			if len(parts) < 3 {
				fmt.Println("Usage: remove <path> <dir|file>")
				continue
			}
			pathStr := parts[1]
			typeStr := parts[2]
			var nodeType NodeType
			if typeStr == "dir" {
				nodeType = Dir
			} else {
				nodeType = File
			}
			replica.RemoveNode(pathStr, nodeType)
			fmt.Printf("[REMOVE] Removed %s of type %s\n", pathStr, nodeType)
			// gossipToPeers(replica, "")

		case "print":
			tree := BuildTree(replica.NodeSet, "root")
			PrintTree(tree, "")

		case "final":
			var policy string
			if len(parts) == 2 {
				policy = parts[1]
				fmt.Printf("Orphan node policy: %s\n", policy)
			} else {
				fmt.Println("Default orphan node policy: skip")
				policy = "skip"
			}
			fmt.Println("\n------------------FINAL TREE---------------")
			gossipToPeers(replica, "")
			time.Sleep(500 * time.Millisecond)
			tree := FinalTree(replica.NodeSet, policy)
			PrintTree(tree, "")
			os.Exit(0)

		// Can be added to synchronize peers that join directly instead of waiting for them to send their operations in order to sync.
		// This can be done using the merge method in fs.go which sends the whole state instead of doing operation based.
		// case "syncAll":

		default:
			fmt.Println("Unknown command")
		}
	}
}

/* ------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------- TCP CONNECTION + EXCHANGES--------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------- */

// Server / Listener
// startServer starts a TCP server to accept connections from peers.
// Each connection is handled concurrently via handleConnection.
func startServer(port string, replica *Replica) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleConnection(conn, replica)
	}
}

// Server side request handler
// handleConnection processes a single peer connection.
// It receives operations from the peer, applies them locally,
// And sends back missing operations with peer list.
func handleConnection(conn net.Conn, replica *Replica) {
	// Set up the connection for communication, decode the incoming SyncRequest from the peer
	defer conn.Close()
	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)

	var request SyncRequest
	if err := dec.Decode(&request); err != nil {
		log.Println("Failed to decode SyncRequest:", err)
		return
	}

	// Add new peer if not already known
	if request.ListenAddr != "" && !contains(replica.Peers, request.ListenAddr) {
		replica.Peers = append(replica.Peers, request.ListenAddr)
		fmt.Println("[PEERS] Added new peer:", request.ListenAddr)
	}

	// Apply operations that the requester had
	if len(request.Ops) > 0 {
		replica.Receive(request.Ops)
	}

	// Send operations that requester does not yet have, and current peer list (in case it has been updated)
	opsToSend := replica.OpsToSend(request.LastSeen)
	resp := SyncResponse{
		Ops:   opsToSend,
		Peers: replica.Peers,
	}
	if err := enc.Encode(&resp); err != nil {
		log.Println("Failed to send SyncResponse:", err)
		return
	}

	fmt.Printf("[SYNC] served %d operations to replica: %s\n", len(opsToSend), request.ReplicaID)
}

// Client / Peer Synchronizer
// mergeWithPeer connects to a remote peer (client-side) and synchronizes state.
// Sends local operations, receives missing operations, and updates peer list.
// Newly received operations can be gossiped to other known peers.
func mergeWithPeer(net_addr string, replica *Replica) {
	// Dial the remote peer to establish TCP connection
	conn, err := net.Dial("tcp", net_addr)
	if err != nil {
		log.Println("Dial error:", err)
		return
	}

	defer conn.Close()
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	// Send SyncRequest containing info needed by requester
	request := SyncRequest{
		ReplicaID:  replica.ID,
		LastSeen:   replica.LastSeen,
		Ops:        replica.OpLog,
		ListenAddr: replica.Addr,
	}

	if err := enc.Encode(&request); err != nil {
		log.Println("Failed to send SyncRequest:", err)
		return
	}

	// Receive  SyncResponse containing any new operations and updated peer list
	var resp SyncResponse
	if err := dec.Decode(&resp); err != nil {
		log.Println("Failed to send SyncResponse:", err)
		return
	}

	if len(resp.Ops) > 0 {
		replica.Receive(resp.Ops)
		gossipToPeers(replica, net_addr)
	}

	// Merge peer lists
	for _, p := range resp.Peers {
		if !contains(replica.Peers, p) && p != replica.Addr {
			replica.Peers = append(replica.Peers, p)
			fmt.Println("[PEERS] Learned new peer:", p)
		}
	}

	fmt.Printf("[SYNC] from %s applied %d ops\n", net_addr, len(resp.Ops))
}

/* ------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------ HELPERS ------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------- */

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Gossip to all known peers
func gossipToPeers(replica *Replica, exclude string) {
	for _, addr := range replica.Peers {
		if addr == "" || addr == replica.Addr || addr == exclude {
			continue
		}
		go mergeWithPeer(addr, replica)
	}
}
