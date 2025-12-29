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
    TYPE_DATA*** ownShares; 

    // variables for retrieval
    unsigned char* select_buffer_in;            // receiving logical vector buffer
    TYPE_DATA** dot_product_vector;             // load bucket data from disk
    TYPE_DATA* sumBlock;                        // dot product result: computed block share to send to client
    TYPE_DATA** dot_product_vector_in;          // xored vector that from another server
    block mask_key;

    //thread
    int numThreads;
    pthread_t* thread_compute;

    // socket
    unsigned char* bucket_buffer;        // receiving oram data buffer
    unsigned char* iv_buffer;            // receiving iv buffer
    unsigned char* key_buffer;          // receiving mask key buffer
    unsigned char* block_buffer_out;      // sending computed block share to client; copy sumBlock to block_buffer_out
    unsigned char* block_buffer_in;       // receiving writen block share from client，block，读的次数和iv


public:
    ServerDuetORAM(TYPE_INDEX serverNo, int selectedThreads);
    ~ServerDuetORAM();

    int start();

    //main functions
    int retrieve(zmq::socket_t& socket);
    int recvORAMTree(zmq::socket_t& socket);
    int recvORAMKey(zmq::socket_t& socket);
    int recvBlock(zmq::socket_t& socket);


    // retrieval subroutine
    static void* thread_dotProduct_func(void* args);


    // thread functions
    static void* thread_loadRetrievalData_func(void* args);


    static unsigned long int server_logs[13]; 
    static unsigned long int thread_max;
    static char timestamp[16];
};


#endif // SERVER_DUET_ORAM_HPP_