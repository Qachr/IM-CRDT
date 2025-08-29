package main

import (
	"fmt"
	"strings"
	"time"
)

/*  The code in fs.go implements the CRDT solution for file systems introduced by Mehdi Ahmed-Nacer et al. in the "File system on CRDT" paper
It mimics the general behavior and followsthe guidelines for conflict resolution with some adaptations to make the code more seamless.
It is working on a operation-based CRDT logic where the operations are saved into logs and only sent when they are applied or missing
*/

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
}

type TreeNode struct {
	Node     Node
	Children []*TreeNode
}

type Replica struct {
	ID       string
	Clock    int64
	NodeSet  *NodeSetCRDT
	OpLog    []Operation
	LastSeen map[string]int64
	Peers    []string
	Addr     string
}

type Operation struct {
	Type      OperationType
	Node      Node
	Timestamp int64
	ReplicaID string
}

type SyncRequest struct {
	ReplicaID  string
	LastSeen   map[string]int64 // version vector of the replica requesting sync
	Ops        []Operation      // ops sent from replica requesting sync
	ListenAddr string
}

type SyncResponse struct {
	Ops   []Operation // ops other party has that requesting replica does not have
	Peers []string
}

/* ------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------- FILE OPERATIONS ---------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------- */

// AppendLine adds a line at a specific index in file.
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

// RemoveLine marks a line at the given index as deleted.
// Instead of immediately removing the lines, it records a "tombstone"
// This ensures that deletions are consistent across replicas during merging.
func (f *FileCRDT) RemoveLine(index int, replicaID string) {
	ts := time.Now().UnixNano()
	if f.Deleted == nil {
		f.Deleted = make(map[int]Timestamped)
	}

	// Only update the deletion record if:
	// - No prior deletion exists, OR
	// - This deletion has a newer timestamp, OR
	// - The timestamps are equal but this replicaID is lexicographically greater (conflict resolution).
	existing, exists := f.Deleted[index]
	if !exists || ts > existing.Timestamp || (ts == existing.Timestamp && replicaID > existing.ReplicaID) {
		f.Deleted[index] = Timestamped{
			Timestamp: ts,
			ReplicaID: replicaID,
		}
	}
}

// Merge resolves conflicts between replicas by merging both deletions and lines, using timestamps and replica IDs.
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
		// if line has been deleted, and deletion has higher timestamp, deletion wins.
		if deleted && del.Timestamp >= remoteLine.Timestamp {
			continue
		}
		// otherwise, we keep newer line
		localLine, exists := f.Lines[index]
		if !exists || remoteLine.Timestamp > localLine.Timestamp ||
			(remoteLine.Timestamp == localLine.Timestamp && remoteLine.ReplicaID > localLine.ReplicaID) {
			f.Lines[index] = remoteLine
		}
	}
}

/* ------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------- NODE OPERATIONS ---------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------- */

// NextTimestamp increases replica's logical clock and returns new value.
func (r *Replica) NextTimestamp() int64 {
	r.Clock++
	return r.Clock
}

// Constructor - local state of each replica
func NewNodeSetCRDT() *NodeSetCRDT {
	return &NodeSetCRDT{
		AddSet:    make(map[NodeKey]Node),
		RemoveSet: make(map[NodeKey]Timestamped),
	}
}

// Add adds a new node to the file system
// parameter is of type Node (defined above), instead of individually passing path, name and type like in MEhdi Ahmed-Nacer et al. paper.
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

// Remove deletes a node from the file system
// The deletion works similarly to a tombstone logic rather than being definitive in case of reapplication
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

// Exists checks if a specific node (identified by the combination of its path and type) currently exists in the file system
func (ns *NodeSetCRDT) Exists(path string, typ NodeType) bool {
	key := NodeKey{Path: path, Type: typ}
	node, exists := ns.AddSet[key]
	if !exists {
		return false
	}

	tombstone, deleted := ns.RemoveSet[key]

	// The node exists if:
	// - There is no deletion record, OR
	// - The node's creation happened after the deletion (meaning it was re-added).
	return !deleted || node.Created > tombstone.Timestamp
}

/* ------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ REPLICATION LAYER --------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------- */

// ApplyOperation applies a single CRDT operation to the replica.
// It ensures operations are only applied once by tracking the latest timestamp seen from each replica.
// If an operation is older or equal to what has already been applied from that replica, it is skipped.
// Otherwise, the operation is applied (either Add or Delete), and replica's state is updated (LastSeen and OpLog).
// It enforces idempotence and ensures causal progress.
func (r *Replica) ApplyOperation(op Operation) {
	// check if operation has already been seen, if not already seen apply, otherwise, skip
	if last, ok := r.LastSeen[op.ReplicaID]; ok && op.Timestamp <= last {
		return
	}

	switch op.Type {
	case AddOp:
		r.NodeSet.Add(op.Node)
	case DeleteOp:
		r.NodeSet.Remove(op.Node.Path, op.Node.Type, op.Timestamp, op.ReplicaID)
	}

	// mark operation as seen (by setting new timestamp in last seen) and remember op in OpLog
	// records highest timestamp from replica you have applied
	r.LastSeen[op.ReplicaID] = op.Timestamp
	r.OpLog = append(r.OpLog, op)
}

// Receive applies a batch of operations received from another replica.
func (r *Replica) Receive(ops []Operation) {
	for _, op := range ops {
		r.ApplyOperation(op)
	}
}

// Merge ensures local NodeSetCRDT incorporates changes from given remote NodeSetCRDT, resolving conflicts deterministically (LWW).
// This guarantees eventual consistency: after all replicas exchange updates, they will converge to the same state.
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

// Lookup of the replication layer returns a map (path, type -> content).
// The map represents the file system after resolution of add(a) || remove(a) conflicts dealt with by replication layer (cf. MAN et al. paper)
// To be able to view resulting tree, use PrintVisibleNodes instead.
func (ns *NodeSetCRDT) Lookup() map[NodeKey]Node {
	result := make(map[NodeKey]Node)
	for key, node := range ns.AddSet {
		if ns.Exists(key.Path, key.Type) {
			result[key] = node
		}
	}
	return result
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

// Reattach orphans in the logic of MAN et al. paper
// Allows for hierarchical layer conflict resolution
// Hierarchical layer conflict: add(a) || remove(b), where b ancestor of a
func ReattachOrphans(ns *NodeSetCRDT, policy string) {
	for key, node := range ns.AddSet {
		// The parent is always a Dir, a file cannot contain another file
		if node.ParentPath != "" && !ns.Exists(node.ParentPath, Dir) {
			switch policy {
			// Reattach orphan node to the root.
			case "root":
				node.ParentPath = "/"
				node.Path = node.ParentPath + node.Name
				ns.AddSet[key] = node

			// Remove orphan node as its ancestor has also been removed, remove wins logic.
			case "skip":
				// delete(ns.AddSet, id) // Or mark as skipped
				ns.Remove(node.Path, node.Type, node.Created+1, node.ReplicaID)
				fmt.Println(node.Path, "removed by reattach orphans")
				continue

			// Reattach orphan node to its closest ancestor known.
			case "compact":
				key := NodeKey{Path: node.Path, Type: node.Type}
				parentPath := node.ParentPath
				parentKey := NodeKey{Path: parentPath, Type: Dir}
				// Walk up ancestors until we find an existing one
				for parentPath != "" && !ns.Exists(parentPath, Dir) {
					parentNode, ok := ns.AddSet[parentKey]
					fmt.Println(parentNode, ok)
					if !ok {
						break
					}
					parentPath = parentNode.ParentPath
				}
				if parentPath == "" {
					node.ParentPath = "/"
				} else {
					node.ParentPath = parentPath
				}
				node.Path = node.ParentPath + "/" + node.Name
				ns.AddSet[key] = node

			// Reintegrate deleted ancestor, add wins logic.
			case "reappear":
				// re-create ghost parent with dummy metadata
				parentPath := node.ParentPath
				parentKey := NodeKey{Path: parentPath, Type: Dir}
				if ghost, ok := ns.AddSet[parentKey]; ok && !ns.Exists(parentPath, Dir) {
					// Only re-add if it was actually deleted
					removal, removed := ns.RemoveSet[parentKey]
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

// Used as the lookup for the hierarchy layer
// Returns a tree where add(a) || remove(b) conflicts with b ancestor are resolved
// To visualize, use printTree instead
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

// ResoleNameConflicts deals with the naming conflicts in the file system.
//  1. Same-name, same-type, both files with content →
//     Merge the file contents, then remove the duplicate from CRDT.
//  2. Same-name but different types (file vs dir) →
//     Rename the file by appending the first part of its ReplicaID in brackets.
//  3. If a rename occurs, update the CRDT AddSet with the new node name.
//
// Updates the CRDT (ns) as it resolves conflicts, and recurses into child subtrees to handle deeper levels.
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
				// File vs Dir → Rename file
				if child.Node.Type == File {

					extension := strings.SplitN(child.Node.ReplicaID, "-", 2)[0]
					child.Node.Name += "[" + extension + "]"

					// Update the path to reflect new file name
					lastSlash := strings.LastIndex(child.Node.Path, "/")
					if lastSlash != -1 {
						child.Node.Path = child.Node.Path[:lastSlash+1] + child.Node.Name
					} else {
						// If no "/", just replace whole path with new name
						child.Node.Path = child.Node.Name
					}

					// Update the state in CRDT
					key := NodeKey{Path: child.Node.Path, Type: child.Node.Type}
					ns.AddSet[key] = child.Node

					// In case changing the node path was not the goal, only changing the name, remove above code
					// and uncomment following line.
					// child.Node.Name += "[" + child.Node.ReplicaID + "]"
				} else if existing.Node.Type == File {
					existing.Node.Name += "[" + existing.Node.ReplicaID + "]"
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
}

// FinalTree serves as the lookup of the naming layer which is the final lookup, that returns the final view to the user
// Here all other layers' conflicts  are resolved
func FinalTree(ns *NodeSetCRDT, policy string) *TreeNode {
	tree := BuildTree(ns, policy)
	ResolveNameConflicts(tree, ns)
	return tree
}

/* ------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------ HELPERS ------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------- */

// Prints the resulting trees in a more structured and understandable way.
func PrintTree(node *TreeNode, indent string) {
	if node == nil {
		return
	}
	fmt.Printf("%s- Path: %s [type: %s] with parent %s (from replica: %s)\n", indent, node.Node.Path, node.Node.Type, node.Node.ParentPath, node.Node.ReplicaID)
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

// Helper to add node
func (r *Replica) AddNode(path, name, parent string, t NodeType) {
	ts := r.NextTimestamp()
	node := NewNode(path, name, parent, t, ts, r.ID)

	op := Operation{
		Type:      AddOp,
		Node:      node,
		Timestamp: ts,
		ReplicaID: r.ID,
	}

	r.ApplyOperation(op)
}

// Helper to remove node
func (r *Replica) RemoveNode(path string, typ NodeType) {
	ts := r.NextTimestamp()

	op := Operation{
		Type: DeleteOp,
		Node: Node{
			Path:      path,
			Type:      typ,
			Created:   ts,
			ReplicaID: r.ID,
		},
		Timestamp: ts,
		ReplicaID: r.ID,
	}

	r.ApplyOperation(op)

}

// OpsToSend determines which operations should be sent to another replica, based on what that replica has already seen.
//   - If repLastSeen is nil: remote replica has no knowledge of our operations => send the entire OpLog.
//   - Otherwise, send only those operations whose timestamp is newer than  last one acknowledged by the remote replica for each ReplicaID.
//
// It ensures replicas exchange only missing operations, instead of resending everything.
func (r *Replica) OpsToSend(repLastSeen map[string]int64) []Operation {
	if repLastSeen == nil {
		// if there is no info on what has been seen, send everything
		return append([]Operation(nil), r.OpLog...)
	}

	// create new slice for not yet seen operations
	out := make([]Operation, 0, len(r.OpLog))
	for _, op := range r.OpLog {
		if last, ok := repLastSeen[op.ReplicaID]; !ok || op.Timestamp > last {
			out = append(out, op)
		}
	}
	return out
}
