#ifndef SERVER_DUET_ORAM_HPP_
#define SERVER_DUET_ORAM_HPP_

#include "config.h"
#include <zmq.hpp>
#include <pthread.h>
#include "SecretSharedShuffle.hpp"

class ServerDuetORAM {
private:
    // local variable
    std::string CLIENT_ADDR;
    TYPE_INDEX serverNo;
    TYPE_INDEX others;          // other server index
  //  TYPE_DATA*** ownShares; 

    // variables for retrieval
    unsigned char* select_buffer_in;            // receiving logical vector buffer
    TYPE_DATA** dot_product_vector;             // load bucket data from disk
    TYPE_DATA* sumBlock;                        // dot product result: computed block share to send to client
    //TYPE_DATA** dot_product_vector_in;          // xored vector that from another server
    block mask_key;                       

    //thread
    int numThreads;
    pthread_t* thread_compute;
    pthread_t thread_recv;
    pthread_t thread_send;


    // socket
    unsigned char* bucket_buffer;        // receiving oram data buffer
    unsigned char* iv_buffer;            // receiving iv buffer
    unsigned char* key_buffer;          // receiving mask key buffer
    unsigned char* block_buffer_out;      // sending computed block share to client; copy sumBlock to block_buffer_out
    unsigned char* block_buffer_in;       // receiving writen block share from client，block
    unsigned char* evict_buffer_in;
    zmq::socket_t* sendSocket;
    zmq::socket_t* recvSocket;
    unsigned char* path_vector_in_for_identical_copy; // receiving path vector from another server for identical copy
    unsigned char* path_vector_out_for_identical_copy; // sending path vector to another server for identical copy

    // variables for eviction
    TYPE_DATA** delta;
    TYPE_DATA** a;
    TYPE_DATA** b;

    block* fullPermutation;             //recoverd full permutation
    block keytoPermutation;             //received key to generate full permutation
    block* key_permutation_buffer_in;   //received information from client, key and punctured permutation
    block* receivedPuncturedPermutation;//received punctured permutation

    TYPE_DATA** fullPermutationExpansion;
    TYPE_DATA** puncturedPermutationExpansion;
    TYPE_INDEX SIZE_PI;

    TYPE_INDEX* sub_pi;
    block seed_iv;
    TYPE_INDEX* circularShift_1;
    TYPE_INDEX* circularShift_2;



public:
    ServerDuetORAM(TYPE_INDEX serverNo, int selectedThreads);
    ~ServerDuetORAM();

    int start();

    //main functions
    int retrieve(zmq::socket_t& socket);
    int recvORAMTree(zmq::socket_t& socket);
    int recvORAMKey(zmq::socket_t& socket);
    int recvBlock(zmq::socket_t& socket);
    int recvInitialPermutation(zmq::socket_t& socket);
    int evict(zmq::socket_t& socket);

    //eviction
    int generatePermutationOffline(const block& keytoPermutation, block* fullPermutation);


    // retrieval subroutine
    static void* thread_dotProduct_func(void* args);
    static void* thread_xorProduct_func(void* args);


    // thread functions
    static void* thread_loadRetrievalData_func(void* args);
    static void* thread_socket_func(void* args);      

    static int send(std::string ADDR, unsigned char* input, TYPE_INDEX inputSize, zmq::socket_t*);
    static int recv(std::string ADDR, unsigned char* output, TYPE_INDEX outputSize, zmq::socket_t*);

    static int send(std::string ADDR, unsigned char* input, TYPE_INDEX inputSize);
    static int recv(std::string ADDR, unsigned char* output, TYPE_INDEX outputSize);

    // Auxiliary functions
    void transpose_parallel(TYPE_DATA** src, TYPE_DATA** dst, int R, int C);

    void xor_vectors_optimized(TYPE_DATA** vec_a, TYPE_DATA** vec_b, int rows, int cols);

    void efficient_rotate();


    static unsigned long int server_logs[27]; 
    static unsigned long int thread_max;
    static char timestamp[16];
};


#endif // SERVER_DUET_ORAM_HPP_