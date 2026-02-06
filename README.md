# ORAM

Two branches, main and Online-version 

Main for DuetORAM with preprocess, and Online-version for DuetORAM without preprocess

## Required Libraries
[ZeroMQ](https://zeromq.org/download/)

[emp-tool](https://github.com/emp-toolkit/emp-tool)



## Configuration
All configurations are described in ```ORAM/include/config.h```, and followings are explanations of some parameters.
```
#define BLOCK_SIZE 128                                                                      --> Block size (in  bytes)
#define HEIGHT 10                                                                           --> Tree Height
#define BUCKET_SIZE 333                                                                     --> Bucket size
#define EVICT_RATE 280                                                                      --> Eviction frequency
#define DATA_CHUNKS BLOCK_SIZE/sizeof(TYPE_DATA)                                            --> Data chunks
const TYPE_ID NUM_BLOCK = ((int) (pow(2,HEIGHT-1))*EVICT_RATE-1);                           --> Number of blocks
const TYPE_INDEX N_leaf = pow(2,H);                                                         --> Number of leaf nodes
const TYPE_INDEX NUM_NODES = (int) (pow(2,HEIGHT+1)-1);                                     --> Number of nodes of the tree

```

## Build & Compile
Goto to folder ```ORAM/``` and execute

```
mkdir build
cd build
cmake ..
make
```
which produces the binary executable file named ```main``` in ```build/```.


## Usage
Run the binary executable file ```main```, which will ask for either Client or Server mode. In our implement, we run our scheme under two servers.

**Note**: Please run three instances, two for servers and one for client.
