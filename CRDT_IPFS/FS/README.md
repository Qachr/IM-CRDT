# CRONUS PROJECT - FILE SYSTEM

This part of the project is designed to integrate file systems as mutable data in the context of the integration of Merkle-CRDT into IPFS. 
It uses the logic suggested by Mehdi Ahmed-Nacer et al. from the paper 'File system on CRDT" : https://inria.hal.science/hal-00720681/file/RR-8027.pdf

It includes various files within Interactive_FS folder, designed this way to facilitate structure and logic:
- `fs.go`: holds the file structure representation and the logic required to put it in place in accordance with the paper. 
- `main.go`: holds the replica connection logic through tcp connection. It should be modified in order to cater to a more realistic peer to peer environment. 
- `scenario_test.go`: holds several automated tests to see the behavior of the replicas given a fixed set of operations. 

## INTERACTIVE STYLE 
The code can be run in an interactive manner, with a command line interface for interacting with the replica.

To run: 
1. Launch replicas on various terminals:
    ```bash
    go run main.go fs.go -port <portNumber> -peers <peer1,peer2,...>
    ```

2. Apply the desired operations: 
- add `<path>` `<type>` : add node with given path of given type to the file system. 
- remove `<path>` `<type>`: remove node with given path of given type from file system.
- print: print the tree for visualization purposes. 
- final (+policy): print the final tree without naming conflicts, which applies the chosen policy for orphan resolving nodes conflicts. 
    - if no policy is chosen, the default *skip* (remove wins) is applied. 
- syncAll: **TO BE DONE** to sync peers that join in later direclty by sending whole state once to catch them up.
    - can be done using merge fucntion in fs.go 
    - currently, a peer that joins late, receives all missed operations after having applied a chosen operation
    - when the new peer applies this operation (add or remove), it is registered by previous peers who will communicate their state. 

## AUTOMATED TESTS
Here the code just runs a number of scenarios and lets you know whether they passed or not. 

To run: 
Launch replicas on various terminals:
    ```bash
    go test -v
    ```

The produced logs are going to be saved in the `logs` folder within the `Interactive_FS` folder. 

  <u>**NB:**</u> The following code has been written to work on a Windows terminal, some adaptations might be required depending on OS. 