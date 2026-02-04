# ORAM
<!-- The implementation of XXX appeared in XXX. -->
Two branches, main and Online-version Main for DuetORAM with preprocess, and Online-version for DuetORAM without preprocess

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
,which produces the binary executable file named ```main``` in ```build/```.


## Usage
Run the binary executable file ```main```, which will ask for either Client or Server mode. In our implement, we run our scheme under two servers.

**Note**: Please run three instances, two for servers and one for client.

<!-- ## ⚠️ Important Notice

**Bucket Overflow Probability**: In the implementation process, the probability of overflow is even more related to the randomness of the random number. We encountered a problem where overflow occurred because the write-back block was randomly assigned to a bad path, even though the bucket size and eviction frequency met the requirements.

**Performance Optimization**: The current version of the codebase has been optimized for performance using manual SIMD intrinsics (AVX2, AES-NI) and low-level optimizations. While this provides speedup, it may be less readable for educational purposes.

### Hardware Requirements (Optimized Version)

The current optimized version requires modern CPU hardware with the following instruction set support:

**Minimum Requirements:**
- **CPU**: Intel Core (4th Gen Haswell or newer) or AMD Ryzen (1st Gen or newer)
- **Required Instruction Sets**:
  - **AES-NI**: Hardware AES encryption acceleration (mandatory for optimal performance)
  - **AVX2**: 256-bit SIMD vector operations (mandatory for matrix optimizations)
  - **BMI2**: Bit manipulation instructions (optional, minor performance benefit)
- **Memory**: 4GB RAM minimum (8GB+ recommended for large datasets)
- **Threads**: Multi-core CPU recommended (optimization uses OpenMP parallelization)

**Tested Hardware:**
- Intel Core i7-1360P (Raptor Lake, 16 threads) - Fully supported ✅

**To Check Your CPU Support:**
```bash
# Check for AES-NI support
grep -m1 aes /proc/cpuinfo

# Check for AVX2 support
grep -m1 avx2 /proc/cpuinfo
```

**Note**: If your CPU does not support AES-NI or AVX2, the code will automatically fall back to slower software implementations. In this case, we recommend using the readable version (see below).

**For Better Readability**: If you prefer a more readable version of the code without heavy optimizations (or if your hardware lacks AES-NI/AVX2), you can revert to the commit where the matrix expansion operation was moved offline:

```bash
git checkout 25373c33b649da9ee4a98291c3ba6c44887740c7
```

**Commit Message**: "The matrix expansion operation has been moved offline"

This earlier version provides clearer code structure and is recommended for:
- Understanding the algorithm implementation
- Educational purposes
- Code auditing and verification -->


<!-- ## Citing
If the code is found useful, we would be appreciated if our paper can be cited with the following bibtex format

```

``` -->
