package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type ReplicaProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *os.File
}

func startReplica(port, peerList string, logFile string) *ReplicaProcess {
	cmd := exec.Command("go", "run", "main.go", "fs.go", "-port", port, "-peers", peerList)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		panic(err)
	}

	logPath := filepath.Join("logs", logFile)
	f, err := os.Create(logPath)
	if err != nil {
		panic(err)
	}

	cmd.Stdout = f
	cmd.Stderr = f

	err = cmd.Start()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Started replica on port %s\n", port)
	return &ReplicaProcess{cmd: cmd, stdin: stdin, stdout: f}
}

func (rp *ReplicaProcess) sendCommand(cmd string) {
	fmt.Fprintf(rp.stdin, "%s\n", cmd)
}

func (rp *ReplicaProcess) close() {
	rp.stdout.Close()
	rp.cmd.Wait()
}

// Test hierarchical layer
// add(a) || remove(b) conflict where b is ancestor of a by treating orphan nodes
// Orphan node policy : skip
func TestReplicaScenario1(t *testing.T) {
	os.MkdirAll("logs", 0755)

	r1 := startReplica("8001", "8002", "r1_scenario1.log")
	r2 := startReplica("8002", "8001", "r2_scenario1.log")

	defer r1.close()
	defer r2.close()

	time.Sleep(1 * time.Second)

	r1.sendCommand("add /home dir")
	r2.sendCommand("add /user dir")
	time.Sleep(500 * time.Millisecond)

	r1.sendCommand("add /home/docs dir")
	r2.sendCommand("add /home/docs/private dir")

	r1.sendCommand("add /home/docs/private/file1.txt file")
	r2.sendCommand("remove /home/docs/private dir")

	time.Sleep(2 * time.Second)

	r1.sendCommand("final")
	r2.sendCommand("final")

	time.Sleep(2 * time.Second)
}

// Test hierarchical layer
// add(a) || remove(b) conflict where b is ancestor of a by treating orphan nodes
// Orphan node policy : reappear
func TestReplicaScenario2(t *testing.T) {
	os.MkdirAll("logs", 0755)

	r1 := startReplica("8001", "8002", "r1_scenario2.log")
	r2 := startReplica("8002", "8001", "r2_scenario2.log")

	defer r1.close()
	defer r2.close()

	time.Sleep(1 * time.Second)

	r1.sendCommand("add /home dir")
	r2.sendCommand("add /user dir")
	time.Sleep(500 * time.Millisecond)

	r1.sendCommand("add /home/docs dir")
	r2.sendCommand("add /home/docs/private dir")

	r1.sendCommand("add /home/docs/private/file1.txt file")
	r2.sendCommand("remove /home/docs/private dir")

	time.Sleep(2 * time.Second)

	r1.sendCommand("final reappear")
	r2.sendCommand("final reappear")

	time.Sleep(2 * time.Second)
}

// Test hierarchical layer
// add(a) || remove(b) conflict where b is ancestor of a by treating orphan nodes
// Orphan node policy : root
func TestReplicaScenario3(t *testing.T) {
	os.MkdirAll("logs", 0755)

	r1 := startReplica("8001", "8002", "r1_scenario3.log")
	r2 := startReplica("8002", "8001", "r2_scenario3.log")

	defer r1.close()
	defer r2.close()

	time.Sleep(1 * time.Second)

	r1.sendCommand("add /home dir")
	r2.sendCommand("add /user dir")
	time.Sleep(500 * time.Millisecond)

	r1.sendCommand("add /home/docs dir")
	r2.sendCommand("add /home/docs/private dir")

	r1.sendCommand("add /home/docs/private/file1.txt file")
	r2.sendCommand("remove /home/docs/private dir")

	time.Sleep(2 * time.Second)

	r1.sendCommand("final root")
	r2.sendCommand("final root")

	time.Sleep(2 * time.Second)
}

// Test hierarchical layer
// add(a) || remove(b) conflict where b is ancestor of a by treating orphan nodes
// Orphan node policy : compact
func TestReplicaScenario4(t *testing.T) {
	os.MkdirAll("logs", 0755)

	r1 := startReplica("8001", "8002", "r1_scenario4.log")
	r2 := startReplica("8002", "8001", "r2_scenario4.log")

	defer r1.close()
	defer r2.close()

	time.Sleep(1 * time.Second)

	r1.sendCommand("add /home dir")
	r2.sendCommand("add /user dir")
	time.Sleep(500 * time.Millisecond)

	r1.sendCommand("add /home/docs dir")
	r2.sendCommand("add /home/docs/private dir")

	r1.sendCommand("add /home/docs/private/file1.txt file")
	r2.sendCommand("remove /home/docs/private dir")

	time.Sleep(2 * time.Second)

	r1.sendCommand("final compact")
	r2.sendCommand("final compact")

	time.Sleep(2 * time.Second)
}

// Test naming layer
// add(a) || add(a) conflict where elements are of type file
// files created concurrently in the same place
func TestReplicaScenario5(t *testing.T) {
	os.MkdirAll("logs", 0755)

	r1 := startReplica("8001", "8002", "r1_scenario5.log")
	r2 := startReplica("8002", "8001", "r2_scenario5.log")

	defer r1.close()
	defer r2.close()

	time.Sleep(1 * time.Second)

	r1.sendCommand("add /home dir")
	r2.sendCommand("add /user dir")
	time.Sleep(500 * time.Millisecond)

	r1.sendCommand("add /home/docs dir")
	r2.sendCommand("add /home/docs/private dir")

	r1.sendCommand("add /home/docs/private/rename file")
	r2.sendCommand("remove /home/docs/private dir")

	r2.sendCommand("add /home/docs/private/rename file")

	time.Sleep(2 * time.Second)

	r1.sendCommand("final reappear")
	r2.sendCommand("final reappear")

	time.Sleep(2 * time.Second)
}

// Test naming layer
// add(a) || add(a) conflict where elements are of different types
// elements created concurrently in the same place, from different replicas
func TestReplicaScenario6(t *testing.T) {
	os.MkdirAll("logs", 0755)

	r1 := startReplica("8001", "8002", "r1_scenario6.log")
	r2 := startReplica("8002", "8001", "r2_scenario6.log")

	defer r1.close()
	defer r2.close()

	time.Sleep(1 * time.Second)

	r1.sendCommand("add /home dir")
	r2.sendCommand("add /user dir")
	time.Sleep(500 * time.Millisecond)

	r1.sendCommand("add /home/docs dir")
	r2.sendCommand("add /home/docs/private dir")

	r1.sendCommand("add /home/docs/private/rename file")
	r2.sendCommand("remove /home/docs/private dir")

	r2.sendCommand("add /home/docs/private/rename dir")

	time.Sleep(2 * time.Second)

	r1.sendCommand("final reappear")
	r2.sendCommand("final reappear")

	time.Sleep(2 * time.Second)
}
