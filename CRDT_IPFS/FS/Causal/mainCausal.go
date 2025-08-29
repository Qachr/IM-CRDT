package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

type OperationType string

const (
	AddOp    OperationType = "add"
	DeleteOp OperationType = "delete"
)

type NodeType string

const (
	File NodeType = "file"
	Dir  NodeType = "dir"
)

type VersionVector map[string]int64

// LWW Timestamped Entry
type Timestamped struct {
	Timestamp int64
	ReplicaID string
}

type Line struct {
	Index     int
	Content   string
	Timestamp int64
	ReplicaID string
}

type FileCRDT struct {
	Lines   map[int]Line        // key is line index
	Deleted map[int]Timestamped // Tombstone
}

// AppendLine(index, ...) adds a line at a specific index.
// If two replicas both write to the same index, the Last-Writer-Wins (LWW) logic chooses the line with the latest timestamp (or higher replicaID if timestamps are equal).
// If each call to AppendLine uses a different index, then lines accumulate and get printed in order later via PrintContent

func (f *FileCRDT) AppendLine(index int, content string, replicaID string) {
	ts := time.Now().UnixNano()
	if f.Lines == nil {
		f.Lines = make(map[int]Line)
	}
	if f.Deleted == nil {
		f.Deleted = make(map[int]Timestamped)
	}
	if del, exists := f.Deleted[index]; exists && del.Timestamp >= ts {
		return // Line is deleted more recently
	}

	existing, exists := f.Lines[index]
	if !exists || ts > existing.Timestamp || (ts == existing.Timestamp && replicaID > existing.ReplicaID) {
		f.Lines[index] = Line{
			Index:     index,
			Content:   content,
			Timestamp: ts,
			ReplicaID: replicaID,
		}
	}
}

func (f *FileCRDT) RemoveLine(index int, replicaID string) {
	ts := time.Now().UnixNano()
	if f.Deleted == nil {
		f.Deleted = make(map[int]Timestamped)
	}

	existing, exists := f.Deleted[index]
	if !exists || ts > existing.Timestamp || (ts == existing.Timestamp && replicaID > existing.ReplicaID) {
		f.Deleted[index] = Timestamped{
			Timestamp: ts,
			ReplicaID: replicaID,
		}
	}
}

func (f *FileCRDT) Merge(other *FileCRDT) {
	if f.Lines == nil {
		f.Lines = make(map[int]Line)
	}
	if f.Deleted == nil {
		f.Deleted = make(map[int]Timestamped)
	}

	// Merge deletions
	for index, remoteDel := range other.Deleted {
		localDel, exists := f.Deleted[index]
		if !exists || remoteDel.Timestamp > localDel.Timestamp ||
			(remoteDel.Timestamp == localDel.Timestamp && remoteDel.ReplicaID > localDel.ReplicaID) {
			f.Deleted[index] = remoteDel
		}
	}

	for index, remoteLine := range other.Lines {
		del, deleted := f.Deleted[index]
		if deleted && del.Timestamp >= remoteLine.Timestamp {
			continue // deletion wins
		}
		localLine, exists := f.Lines[index]
		if !exists || remoteLine.Timestamp > localLine.Timestamp ||
			(remoteLine.Timestamp == localLine.Timestamp && remoteLine.ReplicaID > localLine.ReplicaID) {
			f.Lines[index] = remoteLine
		}
	}
}

// type FileCRDT struct {
// 	Ops []string //this slice holds each line (or operation) apeded to file
// }

// func (f *FileCRDT) Merge(other *FileCRDT) {
// 	f.Ops = append(f.Ops, other.Ops...)
// }

// func (f *FileCRDT) AppendLine(line string) {
// 	f.Ops = append(f.Ops, line)
// }

type Node struct {
	ID        string
	Name      string
	ParentID  string
	Type      NodeType
	Content   *FileCRDT
	Created   int64
	ReplicaID string
}

type NodeSetCRDT struct {
	AddSet       map[string]Node
	RemoveSet    map[string]Timestamped
	VersionClock VersionVector
}

// Constructor
func NewNodeSetCRDT() *NodeSetCRDT {
	return &NodeSetCRDT{
		AddSet:       make(map[string]Node),
		RemoveSet:    make(map[string]Timestamped),
		VersionClock: make(VersionVector),
	}
}

// This function updates the version vector of the replica. Allows us to ensure causal consistency.
// It updates the version vector for a specific replicaID only if:
// - There's no entry yet, or
// - The new timestamp is greater than the existing one.
func (vv VersionVector) Update(replicaID string, timestamp int64) {
	if ts, ok := vv[replicaID]; !ok || timestamp > ts {
		vv[replicaID] = timestamp
	}
}

// Function used to compare two version vectors to determine if one dominates (is causally equal to or ahead of) the other.
func (vv VersionVector) Dominates(other VersionVector) bool {
	for rid, ts := range other {
		if vv[rid] < ts {
			return false
		}
	}
	return true
}

func (ns *NodeSetCRDT) Add(node Node) {
	existing, ok := ns.AddSet[node.ID]
	if !ok || node.Created > existing.Created ||
		(node.Created == existing.Created && node.ReplicaID > existing.ReplicaID) {
		ns.AddSet[node.ID] = node
		ns.VersionClock.Update(node.ReplicaID, node.Created)
	}
}

func (ns *NodeSetCRDT) Remove(nodeID string, ts Timestamped) {
	existingDel, exists := ns.RemoveSet[nodeID]
	if !exists || ts.Timestamp > existingDel.Timestamp ||
		(ts.Timestamp == existingDel.Timestamp && ts.ReplicaID > existingDel.ReplicaID) {
		ns.RemoveSet[nodeID] = ts
		ns.VersionClock.Update(ts.ReplicaID, ts.Timestamp)
	}
}

func (ns *NodeSetCRDT) Exists(nodeID string) bool {
	added, exists := ns.AddSet[nodeID]
	if !exists {
		return false
	}
	removed, wasRemoved := ns.RemoveSet[nodeID]
	if !wasRemoved {
		return true
	}
	return added.Created > removed.Timestamp ||
		(added.Created == removed.Timestamp && added.ReplicaID > removed.ReplicaID)
}

func (ns *NodeSetCRDT) Merge(remote *NodeSetCRDT) {

	// Obsolete optimization
	// If local dominates remote, no-op
	// if ns.VersionClock.Dominates(remote.VersionClock) {
	// 	return
	// }

	// // If remote dominates local, apply all remote ops
	// if remote.VersionClock.Dominates(ns.VersionClock) {
	// 	for _, node := range remote.AddSet {
	// 		ns.Add(node)
	// 	}
	// 	for id, del := range remote.RemoveSet {
	// 		ns.Remove(id, del)
	// 	}
	// 	ns.VersionClock = remote.VersionClock
	// 	return
	// }

	// // Concurrent case: merge selectively
	// // Merge version vectors
	// for rep, ts := range remote.VersionClock {
	// 	if localTS, ok := ns.VersionClock[rep]; !ok || ts > localTS {
	// 		ns.VersionClock.Update(rep, ts)
	// 	}
	// }

	for _, remoteNode := range remote.AddSet {
		localNode, exists := ns.AddSet[remoteNode.ID]

		if exists {
			// Only merge content if it's a file
			if localNode.Type == File && remoteNode.Type == File {
				if localNode.Content == nil {
					localNode.Content = &FileCRDT{}
				}
				localNode.Content.Merge(remoteNode.Content)
				// remoteNode.Content = localNode.Content
				// ns.AddSet[remoteNode.ID] = localNode // update the node with merged content
			}
			ns.Add(localNode)
			// Add will still run, and will handle LWW metadata comparison
		} else {
			ns.Add(remoteNode)
		}

	}

	// Merge additions
	// for _, node := range remote.AddSet {
	// 	ns.Add(node)
	// }

	// Merge deletions
	for id, remoteDel := range remote.RemoveSet {
		ns.Remove(id, remoteDel)
	}
}

// Merges all replicas, like send the changes to next replicas
func SyncAll(replicas ...*NodeSetCRDT) {
	for i := 0; i < len(replicas); i++ {
		for j := 0; j < len(replicas); j++ {
			if i != j {
				replicas[i].Merge(replicas[j])
			}
		}
	}
}

// Reattach orphans to root node logic
/* func ReattachOrphans(ns *NodeSetCRDT) {
	for id, node := range ns.AddSet {
		if node.ParentID != "" {
			if _, ok := ns.AddSet[node.ParentID]; !ok {
				node.ParentID = "root"
				ns.AddSet[id] = node
			}
		}
	}
} */

// Reattach orphans in the logic of Mehdi Ahmed-Nacer et al: File system on CRDT paper
// Allows for hierarchical layer conflict resolution
func ReattachOrphans(ns *NodeSetCRDT, policy string) {
	for id, node := range ns.AddSet {
		if node.ParentID != "" && !ns.Exists(node.ParentID) {
			switch policy {
			case "rootPol":
				node.ParentID = "root"
			case "skip":
				// delete(ns.AddSet, id) // Or mark as skipped
				ts := time.Now().UnixNano()
				ns.Remove(node.ID, Timestamped{
					Timestamp: ts,
					ReplicaID: node.ReplicaID,
				})
				continue
			// ************************* WORKS BUT TRY TO OPTIMIZE LOOKING FOR THE PARENT NAME **********
			// ************************* POTENTIALLY ANOTHER WAY TO TO MAP THE NODES ******
			case "compact":
				parentID := node.ParentID
				// fmt.Println(parentID, node.Name)
				// // Check if parentID is actually an ID — if not found, try to find by name
				// fmt.Println(ns.Exists(parentID))
				if !ns.Exists(parentID) {
					// Inline search: find node by name
					for id, n := range ns.AddSet {
						// fmt.Println("First confition", id, n)
						if n.Name == parentID {
							parentID = id
							break
						}
					}
				}
				// fmt.Println("new parentid", parentID)
				// Walk up ancestors until we find an existing one
				for parentID != "" && !ns.Exists(parentID) {
					parentNode, ok := ns.AddSet[parentID]
					// fmt.Println(parentNode)
					if !ok {
						break
					}
					parentID = parentNode.ParentID
				}

				if parentID == "" {
					node.ParentID = "root"
				} else {
					node.ParentID = parentID
				}
			case "reappear":
				// re-create ghost parent with dummy metadata
				parentID := node.ParentID
				// fmt.Println(parentID)
				// If the parent ID doesn't exist, we try to find its real ID (if parentID is a name)
				if !ns.Exists(parentID) {
					// Look up by name if necessary
					for id, n := range ns.AddSet {
						if n.Name == parentID {
							parentID = id
							break
						}
					}
				}

				if ghost, ok := ns.AddSet[parentID]; ok && !ns.Exists(parentID) {
					// Only re-add if it was actually deleted
					// fmt.Println("Ghost", ghost, ghost.ParentID)
					removal, removed := ns.RemoveSet[parentID]
					created := ghost.Created
					if removed {
						created = removal.Timestamp + 1 // Make it later than the removal
					} else {
						created = node.Created + 1 // fallback
					}
					ns.Add(Node{
						ID:        ghost.ID, // It matches original ID so children can link back to it
						Name:      ghost.Name,
						Type:      ghost.Type,
						ParentID:  ghost.ParentID, // Preserve tree structure
						Created:   created,        // must be older than delete action since it is LWW so it can overwrite that
						ReplicaID: node.ReplicaID,
					})
				}
			}
			ns.AddSet[id] = node
			// fmt.Println(ns, id)
		}
	}
}

func ResolveNameConflicts(ns *NodeSetCRDT) {
	seen := make(map[string]map[string]Node)
	// fmt.Println(seen)
	for _, node := range ns.AddSet {
		if !ns.Exists(node.ID) {
			continue
		}

		parentMap, ok := seen[node.ParentID]
		if !ok {
			parentMap = make(map[string]Node)
			seen[node.ParentID] = parentMap
		}

		existing, exists := parentMap[node.Name]
		if exists {
			if node.Type == existing.Type {
				// Case two files, same name, same place, contents of files merged. Keep one of the files only.
				if node.Type == File && node.Content != nil && existing.Content != nil {
					node.Content.Merge(existing.Content)

					ts := time.Now().UnixNano()
					ns.Remove(existing.ID, Timestamped{
						Timestamp: ts,
						ReplicaID: existing.ReplicaID,
					})
					continue
				}
			} else {

				// Case of one file, one dir, rename the file by adding an extension with the origin (here the replica)
				if node.Type == File {
					node.Name = node.Name + "[" + node.ReplicaID + "]"
				} else if existing.Type == File {
					existing.Name = existing.Name + "[" + existing.ReplicaID + "]"
					ns.AddSet[existing.ID] = existing
				}
			}
		}

		parentMap[node.Name] = node
		ns.AddSet[node.ID] = node
	}
}

func PrintContent(file Node) {
	var keys []int
	for k, line := range file.Content.Lines {
		// Check if line is tombstoned
		if tombstone, deleted := file.Content.Deleted[k]; deleted {
			if tombstone.Timestamp >= line.Timestamp {
				continue // skip this line
			}
		}
		keys = append(keys, k)
	}
	sort.Ints(keys)

	// Print all lines
	fmt.Printf("Content %s\n", file.Name)
	for _, k := range keys {
		line := file.Content.Lines[k]
		fmt.Println(line.Content)
	}
}

func BuildTree(ns *NodeSetCRDT) map[string][]Node {
	tree := make(map[string][]Node)
	for _, node := range ns.AddSet {
		if ns.Exists(node.ID) {
			tree[node.ParentID] = append(tree[node.ParentID], node)
		}
	}
	return tree
}

func PrintTree(tree map[string][]Node) {
	fmt.Println()
	for parent, children := range tree {
		if parent == "" {
			continue
		}
		fmt.Printf("Parent: %s\n", parent)
		for _, child := range children {
			fmt.Printf("  - %s (%s)\n", child.Name, child.Type)
			if child.Type == File && child.Content != nil {
				PrintContent(child)
			}
		}
	}
}

// Helper to create node
func NewNode(name, parentID string, t NodeType, replicaID string) Node {
	var content *FileCRDT
	if t == File {
		content = &FileCRDT{}
	}
	return Node{
		ID:        uuid.NewString(),
		Name:      name,
		ParentID:  parentID,
		Type:      t,
		Content:   content,
		Created:   time.Now().UnixNano(),
		ReplicaID: replicaID,
	}
}

func (ns *NodeSetCRDT) GetNodeByName(name string) *Node {
	for _, node := range ns.AddSet {
		if node.Name == name && ns.Exists(node.ID) {
			return &node
		}
	}
	return nil
}

func main() {
	replicaID1 := "replica1"
	replicaID2 := "replica2"

	replica1 := NewNodeSetCRDT()
	replica2 := NewNodeSetCRDT()

	root := Node{
		ID:        "root",
		Name:      "/",
		Type:      Dir,
		ParentID:  "",
		Created:   time.Now().UnixNano(),
		ReplicaID: "init",
	}

	replica1.Add(root)
	replica2.Add(root)

	// DIR1 - Created by replica1
	dir1 := NewNode("dir1", "root", Dir, replicaID1)
	// file1.Content.AppendLine(0, "Line 0 - Initial text in file 1", replicaID1)
	// file1.Content.AppendLine(1, "Line 1 - Second line", replicaID1)
	replica1.Add(dir1)

	// FILE2 - Created by replica2
	file2 := NewNode("file2", "root", File, replicaID2)
	file2.Content.AppendLine(0, "Initial text in file 2", replicaID2)
	replica2.Add(file2)

	dir2 := NewNode("dir2", "dir1", Dir, replicaID2)
	replica2.Add(dir2)

	dir3 := NewNode("file2", "root", Dir, replicaID1)
	replica1.Add(dir3)

	// FILE3 - Created by replica 1, nested under DIR1
	file3 := NewNode("file3", "dir2", File, replicaID1)
	file3.Content.AppendLine(0, "Initial text in file 3", replicaID1)
	replica1.Add(file3)

	// FILE4 - Created by replica2
	file4 := NewNode("file4", "root", File, replicaID2)
	file4.Content.AppendLine(0, "Initial text in file 4", replicaID2)
	replica2.Add(file4)

	// FILE5 - Created by rpelica 1 in same place and with same name as file4
	// Used to test the resolve name conflicts fucntion
	file5 := NewNode("file4", "root", File, replicaID1)
	file5.Content.AppendLine(1, "Initial text in file 5, which has the same name as file 4", replicaID1)
	replica1.Add(file5)

	// First sync
	SyncAll(replica1, replica2)

	// Lines added from different replicas
	// file1rep2 := replica2.GetNodeByName("file1")
	// file1rep2.Content.AppendLine(2, "Edits to file 1 by replica 2", replicaID2)

	file3rep2 := replica2.GetNodeByName("file3")
	file3rep2.Content.AppendLine(1, "Edits to file 3 by replica 2", replicaID2)

	file4rep1 := replica1.GetNodeByName("file4")
	file4rep1.Content.AppendLine(2, "Edits to file 4 by replica 1", replicaID1)

	// ---- TOMBSTONE DELETE ----
	// Replica2 deletes line 1 from file1
	file3rep2.Content.RemoveLine(1, replicaID2)
	// replica2.Remove()

	// Sync again
	SyncAll(replica1, replica2)

	// tree1 := BuildTree(replica1)
	// PrintTree(tree1)

	// ------ ORPHAN NODES -----
	// ************ seems to get applied even without syncing again, check that out
	replica1.Remove(dir2.ID, Timestamped{
		Timestamp: time.Now().UnixNano() + 1,
		ReplicaID: replicaID1,
	})

	// SyncAll(replica1, replica2)
	// fmt.Println(replica1)
	ReattachOrphans(replica1, "rootPol")
	ResolveNameConflicts(replica1)

	// fmt.Println("== Before Reattach ==")
	fmt.Println("\n===== Tree structure after layers' conflicts resolution =====")
	tree := BuildTree(replica1)
	PrintTree(tree)

	// // TEST 1: Root Policy -- reattach to root directly
	// fmt.Println("\n== After Reattach (rootPol) ==")
	// ReattachOrphans(replica1, "rootPol")
	// tree1 := BuildTree(replica1)
	// PrintTree(tree1)

	// // TEST 2: Skip Policy -- priority to remove
	// fmt.Println("\n== After Reattach (skip) ==")
	// ReattachOrphans(replica1, "skip")
	// tree2 := BuildTree(replica1)
	// PrintTree(tree2)

	// // TEST 3: Compact Policy
	// fmt.Println("\n== After Reattach (compact) ==")
	// ReattachOrphans(replica1, "compact")
	// tree3 := BuildTree(replica1)
	// PrintTree(tree3)

	// // TEST 4: Reappear Policy
	// fmt.Println("\n== After Reattach (reappear) ==")
	// ReattachOrphans(replica1, "reappear")
	// tree4 := BuildTree(replica1)
	// PrintTree(tree4)

}
