package main

import (
	"fmt"
	"time"
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

// type DirCRDT struct{}

type Node struct {
	Path       string
	Name       string
	ParentPath string
	Type       NodeType
	Content    *FileCRDT
	Created    int64
	ReplicaID  string
}

type NodeKey struct {
	Path string
	Type NodeType
}

type NodeSetCRDT struct {
	AddSet    map[NodeKey]Node
	RemoveSet map[NodeKey]Timestamped
	// VersionClock VersionVector
}

type TreeNode struct {
	Node     Node
	Children []*TreeNode
}

type Replica struct {
	ID      string
	Clock   int64
	NodeSet *NodeSetCRDT
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

/* ------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ REPLICATION LAYER --------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------- */

func (r *Replica) NextTimestamp() int64 {
	r.Clock++
	return r.Clock
}

// Constructor
func NewNodeSetCRDT() *NodeSetCRDT {
	return &NodeSetCRDT{
		AddSet:    make(map[NodeKey]Node),
		RemoveSet: make(map[NodeKey]Timestamped),
		// VersionClock: make(VersionVector),
	}
}

func (ns *NodeSetCRDT) Add(node Node) {
	// LWW semantics for add vs. delete.
	// If it has already been deleted at a later timestamp, then do not even do the add.
	key := NodeKey{Path: node.Path, Type: node.Type}
	existingTomb, deleted := ns.RemoveSet[key]
	if deleted && existingTomb.Timestamp >= node.Created {
		return // Deletion dominates — ignore add
	}

	existing, exists := ns.AddSet[key]
	if !exists || node.Created > existing.Created {
		ns.AddSet[key] = node
	}
}

func (ns *NodeSetCRDT) Remove(path string, typ NodeType, timestamp int64, replicaID string) {
	// LWW tombstone logic
	key := NodeKey{Path: path, Type: typ}

	existing, exists := ns.RemoveSet[key]
	if !exists || timestamp > existing.Timestamp {
		ns.RemoveSet[key] = Timestamped{
			Timestamp: timestamp,
			ReplicaID: replicaID,
		}
	}
}

func (ns *NodeSetCRDT) Exists(path string, typ NodeType) bool {
	key := NodeKey{Path: path, Type: typ}
	node, exists := ns.AddSet[key]
	if !exists {
		return false
	}

	tombstone, deleted := ns.RemoveSet[key]
	return !deleted || node.Created > tombstone.Timestamp
}

func (ns *NodeSetCRDT) Lookup() map[NodeKey]Node {
	result := make(map[NodeKey]Node)
	for key, node := range ns.AddSet {
		if ns.Exists(key.Path, key.Type) {
			result[key] = node
		}
	}
	return result
}

func (ns *NodeSetCRDT) Merge(remote *NodeSetCRDT) {
	for key, remoteNode := range remote.AddSet {
		localNode, exists := ns.AddSet[key]
		if !exists || remoteNode.Created > localNode.Created ||
			(remoteNode.Created == localNode.Created && remoteNode.ReplicaID > localNode.ReplicaID) {
			ns.AddSet[key] = remoteNode
		}
	}
	for key, remoteTomb := range remote.RemoveSet {
		localTomb, exists := ns.RemoveSet[key]
		if !exists || remoteTomb.Timestamp > localTomb.Timestamp ||
			(remoteTomb.Timestamp == localTomb.Timestamp && remoteTomb.ReplicaID > localTomb.ReplicaID) {
			ns.RemoveSet[key] = remoteTomb
		}
	}
}

// This prints the lookup of the replica for the replication layer
func PrintVisibleNodes(ns *NodeSetCRDT) {
	for _, node := range ns.Lookup() {
		fmt.Printf("- %s [%s]\n", node.Path, node.Type)
	}
}

/* ------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ HIERARCHY LAYER ----------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------- */

// Reattach orphans in the logic of Mehdi Ahmed-Nacer et al: File system on CRDT paper
// Allows for hierarchical layer conflict resolution
func ReattachOrphans(ns *NodeSetCRDT, policy string) {
	for key, node := range ns.AddSet {
		// The parent is always a Dir, a file cannot contain another file
		if node.ParentPath != "" && !ns.Exists(node.ParentPath, Dir) {
			switch policy {
			case "root":
				// newPath := strings.Replace(node.Path, node.ParentPath, "", -1)
				// // fmt.Print(newPath)
				// node.Path = newPath
				// fmt.Print(node.Path)
				node.ParentPath = "/"
				node.Path = node.ParentPath + node.Name
				ns.AddSet[key] = node

			case "skip":
				// delete(ns.AddSet, id) // Or mark as skipped
				ns.Remove(node.Path, node.Type, node.Created+1, node.ReplicaID)
				fmt.Println(node.Path, "removed by reattach orphans")
				continue

			case "compact":
				key := NodeKey{Path: node.Path, Type: node.Type}
				parentPath := node.ParentPath
				parentKey := NodeKey{Path: parentPath, Type: Dir}
				// fmt.Println(parentPath, parentKey)
				// Walk up ancestors until we find an existing one
				for parentPath != "" && !ns.Exists(parentPath, Dir) {
					parentNode, ok := ns.AddSet[parentKey]
					fmt.Println(parentNode, ok)
					if !ok {
						break
					}
					parentPath = parentNode.ParentPath
				}
				// fmt.Println(parentPath)
				if parentPath == "" {
					node.ParentPath = "/"
				} else {
					node.ParentPath = parentPath
				}
				node.Path = node.ParentPath + "/" + node.Name
				// fmt.Println("Last", parentKey, node.Path)
				ns.AddSet[key] = node

			case "reappear":
				// re-create ghost parent with dummy metadata
				parentPath := node.ParentPath
				parentKey := NodeKey{Path: parentPath, Type: Dir}
				if ghost, ok := ns.AddSet[parentKey]; ok && !ns.Exists(parentPath, Dir) {
					// Only re-add if it was actually deleted
					// fmt.Println("Ghost", ghost, ghost.ParentID)
					removal, removed := ns.RemoveSet[parentKey]
					// created := ghost.Created
					if removed {
						ghost.Created = removal.Timestamp + 1 // Make it later than the removal
					} else {
						ghost.Created = node.Created + 1 // fallback
					}
					ns.Add(Node{
						Path:       ghost.Path, // It matches original ID so children can link back to it
						Name:       ghost.Name,
						Type:       ghost.Type,
						ParentPath: ghost.ParentPath, // Preserve tree structure
						Created:    ghost.Created,    // must be older than delete action since it is LWW so it can overwrite that
						ReplicaID:  node.ReplicaID,
					})
				}
			}
		}
	}
}

// Used as the lookup for the hierarchy layer, to be able to see, use printtree instead
func BuildTree(ns *NodeSetCRDT, policy string) *TreeNode {
	// Apply orphan resolution to the actual CRDT state
	ReattachOrphans(ns, policy)

	// Proceed with tree building from current visible state
	visibleNodes := ns.Lookup()

	nodeMap := make(map[NodeKey]*TreeNode)
	for key, node := range visibleNodes {
		n := node // copy to avoid issues with pointer reuse
		nodeMap[key] = &TreeNode{Node: n}
	}

	var root *TreeNode
	for key, treeNode := range nodeMap {
		parentPath := treeNode.Node.ParentPath
		parentKey := NodeKey{Path: parentPath, Type: Dir}
		if parent, ok := nodeMap[parentKey]; ok && key != parentKey {
			parent.Children = append(parent.Children, treeNode)
		} else if treeNode.Node.Path == "/" || parentPath == "/" {
			root = treeNode
		}
	}

	return root
}

/* ------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------- NAMING LAYER ----------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------- */

func ResolveNameConflicts(tree *TreeNode, ns *NodeSetCRDT) {
	if tree == nil || len(tree.Children) == 0 {
		return
	}

	nameMap := make(map[string]*TreeNode)

	for i := 0; i < len(tree.Children); i++ {
		child := tree.Children[i]

		originalName := child.Node.Name
		originalKey := NodeKey{Path: child.Node.Path, Type: child.Node.Type}
		// Conflict: another child with same name under this parent
		if existing, exists := nameMap[originalName]; exists {
			if child.Node.Type == existing.Node.Type {
				if child.Node.Type == File &&
					child.Node.Content != nil && existing.Node.Content != nil {

					child.Node.Content.Merge(existing.Node.Content)

					// Remove the existing duplicate from CRDT (node ID)
					ns.Remove(existing.Node.Path, existing.Node.Type, existing.Node.Created+1, existing.Node.ReplicaID)
					fmt.Println(existing.Node.Path, "removed by resolve name conflicts")
				}
			} else {
				// fmt.Println("file vs dir")
				// File vs Dir → Rename file
				if child.Node.Type == File {
					child.Node.Name += "[" + child.Node.ReplicaID + "]"
					// fmt.Print(child.Node.Name)
				} else if existing.Node.Type == File {
					existing.Node.Name += "[" + existing.Node.ReplicaID + "]"
					// fmt.Print(existing.Node.Name)
					existingKey := NodeKey{Path: existing.Node.Path, Type: existing.Node.Type}
					ns.AddSet[existingKey] = existing.Node
				}
			}
		}

		// Save to map
		nameMap[child.Node.Name] = child
		ns.AddSet[originalKey] = child.Node // Update name if it changed

		// Recursively resolve in subtree
		ResolveNameConflicts(child, ns)
	}
	// fmt.Print(nameMap)
}

// This function serves as the lookup of the naming layer which is the final lookup, that returns the final view to the user
func FinalTree(ns *NodeSetCRDT, policy string) *TreeNode {
	tree := BuildTree(ns, policy)
	ResolveNameConflicts(tree, ns)
	return tree
}

/* ------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------ HELPERS ------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------- */

func PrintTree(node *TreeNode, indent string) {
	if node == nil {
		return
	}
	fmt.Printf("%s- %s [%s] (from %s) %s %s\n", indent, node.Node.Path, node.Node.Type, node.Node.ReplicaID, node.Node.Name, node.Node.ParentPath)
	for _, child := range node.Children {
		PrintTree(child, indent+"  ")
	}
}

// Helper to create node
func NewNode(path, name, parentID string, t NodeType, timestamp int64, replicaID string) Node {
	var content *FileCRDT
	if t == File {
		content = &FileCRDT{}
	}
	return Node{
		Path:       path,
		Name:       name,
		ParentPath: parentID,
		Type:       t,
		Content:    content,
		Created:    timestamp,
		ReplicaID:  replicaID,
	}
}

func (r *Replica) AddNode(path, name, parent string, t NodeType) {
	ts := r.NextTimestamp()
	node := NewNode(path, name, parent, t, ts, r.ID)
	r.NodeSet.Add(node)
}

func (r *Replica) RemoveNode(path string, typ NodeType) {
	ts := r.NextTimestamp()
	r.NodeSet.Remove(path, typ, ts, r.ID)
}
