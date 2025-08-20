package Config

import (
	"encoding/json"
	"fmt"
	"os"
)

type IM_CRDTConfig struct {
	PeerName         string
	Mode             string
	UpdatesNB        int
	TotalNbUpdate    int
	WaitTime         int
	SyncTime         int
	Updating         bool
	Measurement      bool
	NtpServ          string
	Encode           string
	BootstrapPeer    string
	IPFSbootstrap    string
	SwarmKey         bool
	ParallelRetrieve bool
	TestMode         bool
}

func ToFile(cfg IM_CRDTConfig, file string) {
	// compressing data into byte
	b, err := json.Marshal(cfg)
	if err != nil {
		fmt.Printf("Error to marshall config : %s\n", err)
	}

	// Writing bytes in the file @file
	fil, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		panic(fmt.Errorf("ToFile - could Not Open file to write config\nerror: %s", err))
	}
	_, err = fil.Write(b)

	if err != nil {
		panic(fmt.Errorf("could Not write in config file - ToFile - \nerror: %s", err))
	}
	err = fil.Close()
	if err != nil {
		panic(fmt.Errorf("could Not Close - ToFile\nerror: %s", err))
	}
}

func FromFile(cfg *IM_CRDTConfig, file string) {
	// compressing data into byte
	fileInfo, err := os.Stat(file)
	if err != nil {
		panic(fmt.Errorf("FromFile - could Not Open RootNode to update rootnodefolder\nerror: %s", err))
	}
	b := make([]byte, fileInfo.Size())

	// Writing bytes in the file @file
	fil, err := os.OpenFile(file, os.O_RDONLY, 0755)
	if err != nil {
		panic(fmt.Errorf("FromFile - could Not Open config file to read it\nerror: %s", err))
	}
	_, err = fil.Read(b)
	if err != nil {
		panic(fmt.Errorf("could Not read config file - FromFile - \nerror: %s", err))
	}
	err = fil.Close()
	if err != nil {
		panic(fmt.Errorf("could Not Close - FromFile\nerror: %s", err))
	}

	json.Unmarshal(b, cfg)
}

func PrintConfig(cfg IM_CRDTConfig) {

	fmt.Printf("PeerName: %s\n", cfg.PeerName)
	fmt.Printf("Mode: %s\n", cfg.Mode)
	fmt.Printf("UpdatesNB: %d\n", cfg.UpdatesNB)
	fmt.Printf("Updating: %t\n", cfg.Updating)
	fmt.Printf("Measurement: %t\n", cfg.Measurement)
	fmt.Printf("NtpServ: %s\n", cfg.NtpServ)
	fmt.Printf("Encode: %s\n", cfg.Encode)
	fmt.Printf("BootstrapPeer: %s\n", cfg.BootstrapPeer)
	fmt.Printf("IPFSbootstrap: %s\n", cfg.IPFSbootstrap)
	fmt.Printf("SwarmKey: %t\n", cfg.SwarmKey)
	fmt.Printf("ParallelRetrieve: %t\n", cfg.ParallelRetrieve)
}
