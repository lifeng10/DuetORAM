#include "ServerDuetORAM.hpp"
#include "config.h"
#include "Utils.hpp"
#include "struct_socket.h"

#include "DuetORAM.hpp"
#include <iostream>
#include <sys/socket.h>
#include <sys/types.h>

#include "struct_thread_computation.h"
#include "struct_thread_loadData.h"

unsigned long int ServerDuetORAM::server_logs[13];
unsigned long int ServerDuetORAM::thread_max = 0;
char ServerDuetORAM::timestamp[16];

struct ThreadSocketArgs {
    zmq::socket_t* socket; 
    struct_socket* sockData;
};

ServerDuetORAM::ServerDuetORAM(TYPE_INDEX serverNo, int selectedThreads) {
    this->CLIENT_ADDR = "tcp://*:" + std::to_string(SERVER_PORT+(serverNo)*NUM_SERVERS+serverNo);

    this->numThreads = selectedThreads;
    this->thread_compute = new pthread_t[this->numThreads];

    cout<<endl;
	cout << "=================================================================" << endl;
	cout<< "Starting Server-" << serverNo+1 <<endl;
	cout << "=================================================================" << endl;
	this->serverNo = serverNo;

    this->others = 1 - serverNo;

    // variables for retrieval


    // receiving oram data buffer，接收一个bucket的ORAM Tree
    bucket_buffer = new unsigned char[BUCKET_SIZE*sizeof(TYPE_DATA)*DATA_CHUNKS];
    // receiving iv buffer，接收一个bucket的IV
    iv_buffer = new unsigned char[BUCKET_SIZE*sizeof(block)];
    // receiving mask key buffer，接收mask key
    key_buffer = new unsigned char[sizeof(block)];

    // 接收查询向量
    this->select_buffer_in = new unsigned char[sizeof(TYPE_INDEX)+(H+1)*BUCKET_SIZE*sizeof(uint8_t)];

    // 用于存储从磁盘加载的bucket数据, 参与检索运算的路经上的所有blocks
    this->dot_product_vector = new TYPE_DATA*[DATA_CHUNKS];
	for (TYPE_INDEX k = 0 ; k < DATA_CHUNKS; k++)
	{
		this->dot_product_vector[k] = new TYPE_DATA[BUCKET_SIZE*(H+1)];
	}
    
    // 用于存储PIR计算的结果，用于返回给客户端
    sumBlock = new TYPE_DATA[DATA_CHUNKS];



    // socket
    this->SIZE_PI = (H+2)*BUCKET_SIZE;
    this->block_buffer_out = new unsigned char[sizeof(TYPE_DATA)*DATA_CHUNKS];
    this->block_buffer_in = new unsigned char[sizeof(TYPE_DATA)*DATA_CHUNKS+sizeof(TYPE_INDEX)+sizeof(block)];
    this->evict_buffer_in = new unsigned char[sizeof(TYPE_INDEX) + sizeof(block) + 3 * sizeof(TYPE_INDEX) * SIZE_PI];
    this->path_vector_in_for_identical_copy = new unsigned char[DATA_CHUNKS * SIZE_PI * sizeof(TYPE_DATA)];
    this->path_vector_out_for_identical_copy = new unsigned char[DATA_CHUNKS * SIZE_PI * sizeof(TYPE_DATA)];

    // variables for eviction
    // 接收从客户端发送的密钥和矩阵信息
    this->key_permutation_buffer_in = new block[1 + (H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE];

    // 接收到的被穿刺的矩阵
    this-> receivedPuncturedPermutation = new block[(H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE];

    // 用于恢复完整矩阵
    this->fullPermutation = new block[(H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE];

    // 用于扩展完整的矩阵
    this->fullPermutationExpansion = new TYPE_DATA*[DATA_CHUNKS];
    for (TYPE_INDEX i = 0; i < DATA_CHUNKS; i++)
    {
        this->fullPermutationExpansion[i] = new TYPE_DATA[(H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE];
    }

    // 用于扩展穿刺的矩阵
    this->puncturedPermutationExpansion = new TYPE_DATA*[DATA_CHUNKS];
    for (TYPE_INDEX i = 0; i < DATA_CHUNKS; i++)
    {
        this->puncturedPermutationExpansion[i] = new TYPE_DATA[(H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE];
    }
    
    // eviction, Share Translation
    this->sub_pi = new TYPE_INDEX[SIZE_PI];
    this->circularShift_1 = new TYPE_INDEX[SIZE_PI];
    this->circularShift_2 = new TYPE_INDEX[SIZE_PI];

    this->a = new TYPE_DATA*[DATA_CHUNKS];
    this->b = new TYPE_DATA*[DATA_CHUNKS];
    this->delta = new TYPE_DATA*[DATA_CHUNKS];
    for (TYPE_INDEX i = 0; i < DATA_CHUNKS; i++)
    {
        this->a[i] = new TYPE_DATA[SIZE_PI];
        this->b[i] = new TYPE_DATA[SIZE_PI];
        this->delta[i] = new TYPE_DATA[SIZE_PI];
        memset(this->a[i],0,sizeof(TYPE_DATA)*SIZE_PI);
        memset(this->b[i],0,sizeof(TYPE_DATA)*SIZE_PI);
        memset(this->delta[i],0,sizeof(TYPE_DATA)*SIZE_PI);
    }
    

    
}

ServerDuetORAM::~ServerDuetORAM() {
    
}

int ServerDuetORAM::start() {
    int CMD;
    unsigned char buffer[sizeof(CMD)];
    zmq::context_t context(1);
    zmq::socket_t socket(context,ZMQ_REP);

    cout<< "[Server] Socket is OPEN on " << this->CLIENT_ADDR << endl;
    socket.bind(this->CLIENT_ADDR.c_str());

    while(true)
    {
        cout<< "[Server] Waiting for a Command..." <<endl;
        socket.recv(buffer,sizeof(CMD));

        memcpy(&CMD, buffer, sizeof(CMD));
		cout<< "[Server] Command RECEIVED!" <<endl;

        socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS));

        switch (CMD)
        {
            case CMD_SEND_ORAM_TREE:
                cout<<endl;
				cout << "=================================================================" << endl;
				cout<< "[Server] Receiving ORAM Data..." <<endl;
				cout << "=================================================================" << endl;
				this->recvORAMTree(socket);
				cout << "=================================================================" << endl;
				cout<< "[Server] ORAM Data RECEIVED!" <<endl;
				cout << "=================================================================" << endl;
				cout<<endl;
				break;
            case CMD_SEND_KEYS:
                cout<<endl;
				cout << "=================================================================" << endl;
				cout<< "[Server] Receiving ORAM Key..." <<endl;
				cout << "=================================================================" << endl;
				this->recvORAMKey(socket);
				cout << "=================================================================" << endl;
				cout<< "[Server] ORAM Key RECEIVED!" <<endl;
				cout << "=================================================================" << endl;
				cout<<endl;
				break;
            case CMD_REQUEST_BLOCK:
				cout<<endl;
				cout << "=================================================================" << endl;
				cout<< "[Server] Receiving Logical Vector..." <<endl;
				cout << "=================================================================" << endl;
				this->retrieve(socket);
				cout << "=================================================================" << endl;
				cout<< "[Server] Block Share SENT" <<endl;
				cout << "=================================================================" << endl;
				cout<<endl;
				break;
            case CMD_SEND_BLOCK:
				cout<<endl;
            	cout << "=================================================================" << endl;
				cout<< "[Server] Receiving Block Data..." <<endl;
				cout << "=================================================================" << endl;
				this->recvBlock(socket);
				cout << "=================================================================" << endl;
				cout<< "[Server] Block Data RECEIVED!" <<endl;
				cout << "=================================================================" << endl;
				cout<<endl;
				break;
            case CMD_SEND_INITIALIZATION_PERMUTATION:
                cout << endl;
                cout << "=================================================================" << endl;
                cout<< "[Server] Receiving Initial Random Secret Key and Punctured Permutation..." <<endl;
                cout << "=================================================================" << endl;
                this->recvInitialPermutation(socket);
                cout << "=================================================================" << endl;
                cout<< "[Server] Initial Random Secret Key and Punctured Permutation RECEIVED!" <<endl;
                cout << "=================================================================" << endl;
                generatePermutationOffline(keytoPermutation, fullPermutation);
                cout << "=================================================================" << endl;
                cout<< "[Server] Full Permutation is Recovered!" <<endl;
                cout << "=================================================================" << endl;
                cout<<endl;
                break;
            case CMD_SEND_EVICT:
				cout<<endl;
				cout << "=================================================================" << endl;
				cout<< "Receiving Eviction Matrix..." <<endl;
				cout << "=================================================================" << endl;
				this->evict(socket);
				cout << "=================================================================" << endl;
				cout<< "[Server] EVICTION and DEGREE REDUCTION DONE!" <<endl;
				cout << "=================================================================" << endl;
				cout<<endl;
				break;
            
            default:
                break;
        }
    }

    return 0;
}

int ServerDuetORAM::recvORAMTree(zmq::socket_t& socket) {
    for (int i = 0; i < NUM_NODES; i++)
    {
        socket.recv(this->bucket_buffer, BUCKET_SIZE*sizeof(TYPE_DATA)*DATA_CHUNKS, 0);
        string path = rootPath + to_string(this->serverNo) + "/" + to_string(i);

        FILE* file_out = NULL;
        if ((file_out = fopen(path.c_str(), "wb+")) == NULL)
        {
            cout<< "	[recvORAMTree] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        fwrite(this->bucket_buffer, 1, BUCKET_SIZE*sizeof(TYPE_DATA)*DATA_CHUNKS, file_out);
        fclose(file_out);
        socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS), 0);
        
        socket.recv(this->iv_buffer, BUCKET_SIZE*sizeof(block), 0);
        string path_iv = rootPath + "client_iv" + to_string(this->serverNo+1) + "/" + to_string(i);
        FILE* file_iv_out = NULL;
        if ((file_iv_out = fopen(path_iv.c_str(), "wb+")) == NULL)
        {
            cout<< "	[recvORAMTree] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        fwrite(this->iv_buffer, 1, BUCKET_SIZE*sizeof(block), file_iv_out);
        fclose(file_iv_out);
        socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS), 0);
    }

    socket.recv(this->key_buffer, sizeof(block), 0);
    memcpy(&this->mask_key, key_buffer, sizeof(block));
    string path_key = rootPath + "keys/" + to_string(this->serverNo+1);
    FILE* file_key_out = NULL;
    if ((file_key_out = fopen(path_key.c_str(), "wb+")) == NULL)
    {
        cout<< "	[recvORAMTree] File Cannot be Opened!!" <<endl;
        exit(0);
    }
    fwrite(this->key_buffer, 1, sizeof(block), file_key_out);
    fclose(file_key_out);
    socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS),0);
    cout<< "	[recvORAMTree] ACK is SENT!" <<endl;

    return 0;
}

int ServerDuetORAM::recvORAMKey(zmq::socket_t& socket){
    socket.recv((unsigned char*)&mask_key,sizeof(block),0);
    cout<< "	[recvORAMKey] ORAM Key RECEIVED!" <<endl;
    cout << "	[recvORAMKey] ORAM Key is: " << mask_key << endl;
    socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS),0);

    cout<< "	[recvORAMTree] ACK is SENT!" <<endl;

    return 0;
}

int ServerDuetORAM::retrieve(zmq::socket_t& socket)
{
    Utils::write_list_to_file(to_string(HEIGHT) + "_" + to_string(BLOCK_SIZE) + "_server" + to_string(serverNo)+ "_" + timestamp + ".txt",logDir, server_logs, 13);
	memset(server_logs, 0, sizeof(unsigned long int)*13);

    int ret = 1;
	
	auto start = time_now;
	socket.recv(select_buffer_in,sizeof(TYPE_INDEX)+(H+1)*BUCKET_SIZE*sizeof(TYPE_DATA),0);
	auto end = time_now;
	cout<< "	[SendBlock] PathID and Logical Vector RECEIVED in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() << " ns" <<endl;
    server_logs[0] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    TYPE_INDEX pathID;
	memcpy(&pathID, select_buffer_in, sizeof(pathID));

    uint8_t sharedVector[(H+1)*BUCKET_SIZE];
    memcpy(sharedVector, &select_buffer_in[sizeof(pathID)], (H+1)*BUCKET_SIZE*sizeof(uint8_t));

    cout << "    [SendBlock] Retrieval request for pathID " << pathID << endl;

    DuetORAM ORAM;
	TYPE_INDEX fullPathIdx[H+1];
    ORAM.getFullPathIdx(fullPathIdx, pathID);
    
    cout << "    [SendBlock] Full path: ";
    for (int i = 0; i <= H; i++) {
        cout << fullPathIdx[i] << " ";
    }
    cout << endl;

    // 1. use thread to load data from files
    start = time_now;
    int step = ceil((double)DATA_CHUNKS/(double)numThreads);
    int endIdx;

    THREAD_LOADDATA loadData_args[numThreads];
    for(int i = 0, startIdx = 0; i < numThreads , startIdx < DATA_CHUNKS; i ++, startIdx+=step)
    {
        if(startIdx+step > DATA_CHUNKS)
            endIdx = DATA_CHUNKS;
        else
            endIdx = startIdx+step;
            
        loadData_args[i] = THREAD_LOADDATA(this->serverNo, startIdx, endIdx, this->dot_product_vector, fullPathIdx,H+1);
        pthread_create(&thread_compute[i], NULL, &ServerDuetORAM::thread_loadRetrievalData_func, (void*)&loadData_args[i]);
        
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(thread_compute[i], sizeof(cpu_set_t), &cpuset);
    }
    for(int i = 0; i < numThreads; i++)
    {
        pthread_join(thread_compute[i],NULL);
    }
    end = time_now;
	long load_time = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    cout<< "	[SendBlock] Path Nodes READ from Disk in " << load_time << " ns"<<endl;
    server_logs[1] = load_time;

    // 2. compute dot product, Multithread for dot product computation
    start = time_now;
    THREAD_COMPUTATION dotProduct_args[numThreads];
    endIdx = 0;
    step = ceil((double)DATA_CHUNKS/(double)numThreads);

    for(int i = 0, startIdx = 0 ; i < numThreads , startIdx < DATA_CHUNKS; i ++, startIdx+=step)
    {
        if(startIdx+step > DATA_CHUNKS)
            endIdx = DATA_CHUNKS;
        else
            endIdx = startIdx+step;
        
        dotProduct_args[i] = THREAD_COMPUTATION( startIdx, endIdx, this->dot_product_vector, sharedVector, sumBlock);
        pthread_create(&thread_compute[i], NULL, &ServerDuetORAM::thread_dotProduct_func, (void*)&dotProduct_args[i]);

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(thread_compute[i], sizeof(cpu_set_t), &cpuset);
    }
    
    for(int i = 0; i < numThreads; i++)
    {
        pthread_join(thread_compute[i],NULL);
    }

    end = time_now;
    cout<< "	[SendBlock] Block Share CALCULATED in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;
    server_logs[2] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    memcpy(block_buffer_out,sumBlock,sizeof(TYPE_DATA)*DATA_CHUNKS);

    start = time_now;
    cout<< "	[SendBlock] Sending Block Share with ID-" << sumBlock[0] <<endl;
    socket.send(block_buffer_out,sizeof(TYPE_DATA)*DATA_CHUNKS);
    end = time_now;
    cout<< "	[SendBlock] Block Share SENT in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;
    server_logs[3] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    

    return 0;
}

void* ServerDuetORAM::thread_loadRetrievalData_func(void* args)
{
    THREAD_LOADDATA* opt = (THREAD_LOADDATA*) args;

    unsigned long int load_time = 0;
    FILE* file_in = NULL;
    string path;

    for (int i = 0; i < opt->fullPathIdx_length; i++)
    {
        file_in = NULL;
        path = rootPath + to_string(opt->serverNo) + "/" + to_string(opt->fullPathIdx[i]);
        if((file_in = fopen(path.c_str(),"rb")) == NULL){
            cout<< "	[SendBlock] File cannot be opened!!" <<endl;
            exit(0);
        }

        fseek(file_in,BUCKET_SIZE*(opt->startIdx)*sizeof(TYPE_DATA),SEEK_SET);

        for (int k = opt->startIdx ; k < opt->endIdx; k++)
        {
            for(int j = 0 ; j < BUCKET_SIZE; j ++)
            {
                fread(&opt->data_vector[k][i*BUCKET_SIZE+j],1,sizeof(TYPE_DATA),file_in);
            }
        }
        fclose(file_in);
    }
    
}

void* ServerDuetORAM::thread_dotProduct_func(void* args)
{
    THREAD_COMPUTATION* opt = (THREAD_COMPUTATION*) args;

    for (int i = opt->start; i < opt->end; i++)
    {
        opt->dot_product_output[i] = 0;
        for (int j = 0; j < (BUCKET_SIZE*(H+1)); j++)
        {
            
            if (opt->sharedVector[j] == 1)
            {
                opt->dot_product_output[i] = opt->dot_product_output[i] ^ opt->data_vector[i][j];
            }
        }
    }
}

void* ServerDuetORAM::thread_xorProduct_func(void* args)
{
    THREAD_COMPUTATION* opt = (THREAD_COMPUTATION*) args;

    for (int i = opt->start; i < opt->end; i++)
    {
        for (int j = 0; j < BUCKET_SIZE*(H+2); j++)
        {
            opt->dot_product_vector_xored[i][j] = opt->dot_product_vector_xored[i][j] ^ opt->dot_product_vector_xored_in[i][j];
        }
    }
}

int ServerDuetORAM::recvBlock(zmq::socket_t& socket)
{
    cout<< "	[recvBlock] Receiving Block Data..." <<endl;
	auto start = time_now;
	socket.recv(block_buffer_in, sizeof(TYPE_DATA)*DATA_CHUNKS+sizeof(TYPE_INDEX)+sizeof(block), 0);
    auto end = time_now;
    TYPE_INDEX slotIdx;
    TYPE_DATA recvBlock[DATA_CHUNKS];
    block recv_iv;
    memcpy(recvBlock, &block_buffer_in[0], sizeof(TYPE_DATA)*DATA_CHUNKS);
    memcpy(&slotIdx, &block_buffer_in[sizeof(TYPE_DATA)*DATA_CHUNKS], sizeof(TYPE_INDEX));
    memcpy(&recv_iv, &block_buffer_in[sizeof(TYPE_INDEX)+sizeof(TYPE_DATA)*DATA_CHUNKS], sizeof(block));

    cout<< "	[recvBlock] Block Data RECV in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;
    server_logs[4] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    start = time_now;

    // Update root bucket
    FILE *file_update;
    string path = rootPath + to_string(this->serverNo) + "/0";

    if((file_update = fopen(path.c_str(),"r+b")) == NULL)
    {
        cout<< "	[recvBlock] File Cannot be Opened!!" <<endl;
        exit(0);
    }
    fseek(file_update, slotIdx*sizeof(TYPE_DATA), SEEK_SET);
    for (int u = 0; u < DATA_CHUNKS; u++)
    {
        fwrite(&recvBlock[u], 1, sizeof(TYPE_DATA), file_update);
        fseek(file_update, (BUCKET_SIZE -1)*sizeof(TYPE_DATA), SEEK_CUR);
    }
    fclose(file_update);

    FILE* file_update_iv;
    string path_iv = rootPath + "client_iv" + to_string(this->serverNo+1) + "/0";
    if((file_update_iv = fopen(path_iv.c_str(),"r+b")) == NULL)
    {
        cout<< "	[recvBlock] File Cannot be Opened!!" <<endl;
        exit(0);
    }

    fseek(file_update_iv, slotIdx*sizeof(block), SEEK_SET);
    fwrite(&recv_iv, 1, sizeof(block), file_update_iv);
    fclose(file_update_iv);

    end = time_now;
	cout<< "	[recvBlock] Block STORED in Disk in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;
	server_logs[5] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
	
    socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS));
	cout<< "	[recvBlock] ACK is SENT!" <<endl;

    return 0;
}

int ServerDuetORAM::recvInitialPermutation(zmq::socket_t& socket)
{
    int ret = 1;
    socket.recv(key_permutation_buffer_in, sizeof(block)*(1+ (H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE),0);
    socket.send((unsigned char*)CMD_SUCCESS, sizeof(CMD_SUCCESS), 0);
    cout<< "	[recvInitialPermutation] ACK is SENT!" <<endl;

    memcpy(&this->keytoPermutation, &key_permutation_buffer_in[0], sizeof(block));
    memcpy(this->receivedPuncturedPermutation, &key_permutation_buffer_in[1], sizeof(block)*((H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE));
    cout<< "	[recvInitialPermutation] Sparsing key_permutation_buffer_in into key and punctured permutation is finished!" <<endl;

    ret = 0;

    return ret;
}

int ServerDuetORAM::generatePermutationOffline(const block& keytoPermutation, block* fullPermutation)
{
    PRG prg;
    prg.reseed(&keytoPermutation);
    prg.random_block(fullPermutation, (H+2)*BUCKET_SIZE * (H+2)*BUCKET_SIZE);

    return 0;
}

int ServerDuetORAM::evict(zmq::socket_t& socket)
{
    DuetORAM ORAM;
    SecretSharedShuffle sss;
    TYPE_INDEX evict_pathID;
    PRG prg;

    cout<< "	[evict] Receiving Evict Information..." <<endl;;
	auto start = time_now;
    socket.recv(evict_buffer_in, sizeof(TYPE_INDEX) + sizeof(block) + 3 * sizeof(TYPE_INDEX) * SIZE_PI,0);
    auto end = time_now;
    cout<< "	[evict] RECEIVED! in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;
	server_logs[6] = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();

    memcpy(&evict_pathID, &evict_buffer_in[0], sizeof(TYPE_INDEX));
    memcpy(&this->seed_iv, &evict_buffer_in[sizeof(TYPE_INDEX)], sizeof(block));
    memcpy(this->sub_pi, &evict_buffer_in[sizeof(TYPE_INDEX)+sizeof(block)], sizeof(TYPE_INDEX)*SIZE_PI);
    memcpy(this->circularShift_1, &evict_buffer_in[sizeof(TYPE_INDEX)+sizeof(block)+sizeof(TYPE_INDEX)*SIZE_PI], sizeof(TYPE_INDEX)*SIZE_PI);
    memcpy(this->circularShift_2, &evict_buffer_in[sizeof(TYPE_INDEX)+sizeof(block)+2*sizeof(TYPE_INDEX)*SIZE_PI], sizeof(TYPE_INDEX)*SIZE_PI);

    TYPE_INDEX fullPathIdx[H+2];
    ORAM.getFullPathIdx(fullPathIdx, evict_pathID);
    fullPathIdx[H+1] = sss.getSibling(evict_pathID);

    // 1. use thread to load data from files
    start = time_now;
    int step = ceil((double)DATA_CHUNKS/(double)numThreads);
    int endIdx;

        // evict_vector就是驱逐路径上的元素
    THREAD_LOADDATA loadData_args[numThreads];
    TYPE_DATA** evict_vector = new TYPE_DATA*[DATA_CHUNKS];
    for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
    {
        evict_vector[k] = new TYPE_DATA[BUCKET_SIZE*(H+2)];
    }
    
    for(int i = 0, startIdx = 0; i < numThreads , startIdx < DATA_CHUNKS; i ++, startIdx+=step)
    {
        if(startIdx+step > DATA_CHUNKS)
            endIdx = DATA_CHUNKS;
        else
            endIdx = startIdx+step;
        
        loadData_args[i] = THREAD_LOADDATA(this->serverNo, startIdx, endIdx, evict_vector, fullPathIdx, H+2);
        pthread_create(&thread_compute[i], NULL, &ServerDuetORAM::thread_loadRetrievalData_func, (void*)&loadData_args[i]);

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(thread_compute[i], sizeof(cpu_set_t), &cpuset);
    }
    for(int i = 0; i < numThreads; i++)
    {
        pthread_join(thread_compute[i],NULL);
    }

    end = time_now;
	long load_time = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    cout<< "	[evict] Path Nodes READ from Disk in " << load_time << " ns"<<endl;

    // 2. read iv from files
    block* ivs = new block[BUCKET_SIZE*(H+2)];
    FILE* file_iv = NULL;
    string path_iv;
    for (int i = 0; i < H+2; i++)
    {
        file_iv = NULL;
        path_iv = rootPath + "client_iv" + to_string(this->serverNo+1) + "/" + to_string(fullPathIdx[i]);
        if((file_iv = fopen(path_iv.c_str(),"rb")) == NULL){
            cout<< "	[evict] File cannot be opened!!" <<endl;
            exit(0);
        }
        long lSize_iv;
        fseek(file_iv, 0, SEEK_END);
        lSize_iv = ftell(file_iv);
        rewind(file_iv);
        if(fread(&ivs[i*BUCKET_SIZE],1 , BUCKET_SIZE*sizeof(block), file_iv) != sizeof(char)*lSize_iv){
            cout<< "	[evict] File loading error be Read!!" <<endl;
            exit(0);
        }
        fclose(file_iv);
    }
    
    // 3. 根据iv计算密钥流 //NOTE:注意，这里生成的密钥流矩阵和元素矩阵是互为转置的 (optimized with OpenMP parallelization)
    TYPE_DATA** keystream = new TYPE_DATA* [BUCKET_SIZE*(H+2)];
    for (int i = 0 ; i < BUCKET_SIZE*(H+2); i++)
    {
        keystream[i] = new TYPE_DATA[DATA_CHUNKS];
    }
    
    // Parallelize keystream generation - each thread generates keystream independently
    #pragma omp parallel for schedule(static)
    for (int i = 0 ; i < BUCKET_SIZE*(H+2); i++)
    {
        memset(keystream[i],0,sizeof(TYPE_DATA)*DATA_CHUNKS);
        aes_128_ctr<TYPE_DATA>(this->mask_key, ivs[i], nullptr, (uint8_t*)keystream[i], sizeof(TYPE_DATA)*DATA_CHUNKS);
    }
    

    // 4. 计算用于shuffle的路径元素。服务器1选择r1,服务器2选择m+r1
    TYPE_DATA** evict_vector_for_shuffle = new TYPE_DATA*[DATA_CHUNKS];
    for(TYPE_INDEX i = 0; i < DATA_CHUNKS; i++)
    {
        evict_vector_for_shuffle[i] = new TYPE_DATA[BUCKET_SIZE*(H+2)];
        memset(evict_vector_for_shuffle[i],0,sizeof(TYPE_DATA)*BUCKET_SIZE*(H+2));
    }


    if (this->serverNo == 0)
    {
        transpose_parallel(keystream, evict_vector_for_shuffle, BUCKET_SIZE*(H+2), DATA_CHUNKS);
    }
    if (this->serverNo == 1)
    {
        transpose_parallel(keystream, evict_vector_for_shuffle, BUCKET_SIZE*(H+2), DATA_CHUNKS);
        xor_vectors_optimized(evict_vector_for_shuffle, evict_vector, DATA_CHUNKS, BUCKET_SIZE*(H+2));
    }

    
    
    // 5. Circular Shift
    efficient_rotate();
    
    // 6. Expansion (optimized with OpenMP parallelization)
    // Note: Each thread needs its own PRG instance to avoid race conditions
    #pragma omp parallel
    {
        PRG prg_local;
        TYPE_DATA rand_data[DATA_CHUNKS];
        
        #pragma omp for schedule(static)
        for (int i = 0; i < SIZE_PI * SIZE_PI; i++)
        {
            prg_local.reseed(&fullPermutation[i]);
            prg_local.random_data_unaligned(rand_data, sizeof(TYPE_DATA)*DATA_CHUNKS);
            for (int j = 0; j < DATA_CHUNKS; j++)
            {
                fullPermutationExpansion[j][i] = rand_data[j];
            }

            prg_local.reseed(&receivedPuncturedPermutation[i]);
            prg_local.random_data_unaligned(rand_data, sizeof(TYPE_DATA)*DATA_CHUNKS);
            for (int j = 0; j < DATA_CHUNKS; j++)
            {
                puncturedPermutationExpansion[j][i] = rand_data[j];
            }
        }
    }

    //FIXME: Clear a, b, delta before computation to avoid using old values from previous eviction
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        memset(this->a[i], 0, sizeof(TYPE_DATA)*SIZE_PI);
        memset(this->b[i], 0, sizeof(TYPE_DATA)*SIZE_PI);
        memset(this->delta[i], 0, sizeof(TYPE_DATA)*SIZE_PI);
    }

    // 6.1 compute a (optimized with OpenMP and reduced array access)
    #pragma omp parallel for collapse(2) schedule(static)
    for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
    {
        for (TYPE_INDEX j = 0; j < SIZE_PI; j++)
        {
            TYPE_DATA result = 0;
            TYPE_DATA* expansion_row = this->fullPermutationExpansion[k];
            for (TYPE_INDEX i = 0; i < SIZE_PI; i++)
            {
                result ^= expansion_row[j + i*SIZE_PI];
            }
            this->a[k][j] = result;
        }
    }
    

    // 6.2 compute b (optimized with OpenMP and reduced array access)
    #pragma omp parallel for collapse(2) schedule(static)
    for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
    {
        for (TYPE_INDEX i = 0; i < SIZE_PI; i++)
        {
            TYPE_DATA result = 0;
            TYPE_DATA* expansion_row = this->fullPermutationExpansion[k];
            TYPE_INDEX base_idx = i * SIZE_PI;
            for (TYPE_INDEX j = 0; j < SIZE_PI; j++)
            {
                result ^= expansion_row[base_idx + j];
            }
            this->b[k][i] = result;
        }
    }
    

    // 6.3 compute delta (optimized with OpenMP and reduced array access)
    #pragma omp parallel for collapse(2) schedule(static)
    for (TYPE_INDEX k = 0; k < DATA_CHUNKS; k++)
    {
        for (TYPE_INDEX i = 0; i < SIZE_PI; i++)
        {
            TYPE_DATA result = 0;
            TYPE_DATA* punctured_row = this->puncturedPermutationExpansion[k];
            TYPE_INDEX base_idx = i * SIZE_PI;
            TYPE_INDEX sub_pi_i = sub_pi[i];
            
            for (TYPE_INDEX j = 0; j < SIZE_PI; j++)
            {
                result ^= punctured_row[base_idx + j];
            }
            
            for (TYPE_INDEX j = 0; j < SIZE_PI; j++)
            {
                result ^= punctured_row[j * SIZE_PI + sub_pi_i];
            }
            
            this->delta[k][i] = result;
        }
    }

    
    
    // 7. secret shared shuffle
    TYPE_DATA** resultShare = new TYPE_DATA*[DATA_CHUNKS];
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        resultShare[i] = new TYPE_DATA[SIZE_PI];
        memset(resultShare[i],0,sizeof(TYPE_DATA)*SIZE_PI);
    }
    
    sss.secretShare(this->serverNo, numThreads, this->sub_pi, evict_vector_for_shuffle, this->delta, this->a, this->b, resultShare);

    
    
    // 8. 服务器之间交换mask后的数据，得到相同的copy
    block* list_iv = new block[SIZE_PI];
    prg.reseed(&this->seed_iv);
    prg.random_block(list_iv, SIZE_PI);
    cout << "    [evict] Server-" << this->serverNo << " generated " << SIZE_PI << " new IVs from seed: " << this->seed_iv << endl;
    cout << "    [evict] First new IV: " << list_iv[0] << ", Last new IV: " << list_iv[SIZE_PI-1] << endl;

    // 8.1 dot_product_vector_xored用于存放服务器之间交互的信息，即异或后的路径信息
    TYPE_DATA** dot_product_vector_xored_in = new TYPE_DATA*[DATA_CHUNKS];      // 存放从其他服务器发送的经过XOR的路径信息
    TYPE_DATA** dot_product_vector_xored = new TYPE_DATA*[DATA_CHUNKS];
    for (TYPE_INDEX i = 0; i < DATA_CHUNKS; i++)
    {
        dot_product_vector_xored[i] = new TYPE_DATA[SIZE_PI];
        dot_product_vector_xored_in[i] = new TYPE_DATA[SIZE_PI];
        memset(dot_product_vector_xored[i],0,sizeof(TYPE_DATA)*SIZE_PI);
        memset(dot_product_vector_xored_in[i],0,sizeof(TYPE_DATA)*SIZE_PI);
    }

    // 8.2 计算密钥流 (optimized with OpenMP parallelization)
    TYPE_DATA ** key_stream_for_exchange = new TYPE_DATA*[SIZE_PI];
    for (TYPE_INDEX i = 0; i < SIZE_PI; i++)
    {
        key_stream_for_exchange[i] = new TYPE_DATA[DATA_CHUNKS];
    }
    
    // Parallelize keystream generation
    #pragma omp parallel for schedule(static)
    for (TYPE_INDEX i = 0; i < SIZE_PI; i++)
    {
        memset(key_stream_for_exchange[i],0,sizeof(TYPE_DATA)*DATA_CHUNKS);
        aes_128_ctr<TYPE_DATA>(this->mask_key, list_iv[i], nullptr, (uint8_t*)key_stream_for_exchange[i], sizeof(TYPE_DATA)*DATA_CHUNKS);
    }
    
    // 8.3 计算用于交换的信息 (optimized with OpenMP parallelization)
    #pragma omp parallel for schedule(static)
    for (TYPE_INDEX i = 0; i < DATA_CHUNKS; i++)
    {
        TYPE_DATA* result_row = resultShare[i];
        TYPE_DATA* xored_row = dot_product_vector_xored[i];
        for (TYPE_INDEX j = 0; j < SIZE_PI; j++)
        {
            xored_row[j] = result_row[j] ^ key_stream_for_exchange[j][i];
        }
    }

    // 9. distribute xored dot_product_vector to another server
    start = time_now;

    //===========FOR LISTENING=======================================================
    struct_socket recvSocket_arg;
    cout << "   [server listens dot product vector] Creating Threads for Receiving vector..." << endl;
    recvSocket_arg = struct_socket("tcp://*:" + std::to_string(SERVER_PORT+(serverNo)*(NUM_SERVERS)+this->others), NULL, 0, this->path_vector_in_for_identical_copy, DATA_CHUNKS * SIZE_PI * sizeof(TYPE_DATA), NULL, false);
    pthread_create(&thread_recv, NULL, &ServerDuetORAM::thread_socket_func, (void*)&recvSocket_arg);
    cout << "   [server listens dot product vector] Received xored dot product vector!" << endl;
    //===============================================================================

    //===========FOR SENDING=========================================================
    struct_socket sendSocket_arg;
    cout << "   [server sends dot product vector] Creating Threads for Sending vector..." << endl;
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        memcpy(&this->path_vector_out_for_identical_copy[i*SIZE_PI*sizeof(TYPE_DATA)], dot_product_vector_xored[i], SIZE_PI*sizeof(TYPE_DATA));
    }
    
    sendSocket_arg = struct_socket("tcp://localhost:" + to_string(SERVER_PORT+this->others*NUM_SERVERS+this->serverNo), this->path_vector_out_for_identical_copy, DATA_CHUNKS*SIZE_PI*sizeof(TYPE_DATA), NULL, 0, NULL, true);
    pthread_create(&thread_send, NULL, &ServerDuetORAM::thread_socket_func, (void*)&sendSocket_arg);
    cout << "   [server sends dot product vector] Sent xored dot product vector!" << endl;
    //===============================================================================

    cout << "   [server] Waiting for Threads..." << endl;
    pthread_join(thread_send, NULL);
    pthread_join(thread_recv, NULL);

    cout<< "	[server] DONE!" <<endl;
	server_logs[3] = thread_max;
	thread_max = 0;
    end = time_now;
    cout<< "	[EVICT] Xored Dot Product Vector SENT and RECV in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() << " ns"<<endl;

    cout << "   [server] Sparsing received xored dot product vector" << endl;
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        memcpy(dot_product_vector_xored_in[i], &path_vector_in_for_identical_copy[i*SIZE_PI*sizeof(TYPE_DATA)], SIZE_PI*sizeof(TYPE_DATA));
    }
    cout << "   [server] Sparsed!" << endl;

    // Multithread for dot product computation
    start = time_now;
    THREAD_COMPUTATION dotProduct_args[numThreads];
    endIdx = 0;
    step = ceil((double)DATA_CHUNKS/(double)numThreads);
    for(int i = 0, startIdx = 0 ; i < numThreads , startIdx < DATA_CHUNKS; i ++, startIdx+=step)
    {
        if (startIdx + step > DATA_CHUNKS)
            endIdx = DATA_CHUNKS;
        else
            endIdx = startIdx + step;
        
        dotProduct_args[i] = THREAD_COMPUTATION(startIdx, endIdx, dot_product_vector_xored, dot_product_vector_xored_in);
        pthread_create(&thread_compute[i], NULL, &ServerDuetORAM::thread_xorProduct_func, (void*)&dotProduct_args[i]);

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(thread_compute[i], sizeof(cpu_set_t), &cpuset);
    }

    for(int i = 0, startIdx = 0 ; i < numThreads , startIdx < DATA_CHUNKS; i ++, startIdx+=step)
    {
        pthread_join(thread_compute[i],NULL);
    }

    end = time_now;
    cout<< "	[EVICT] GET IDENTICAL PATH CALCULATED in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;

    // 10. write back to files
    start = time_now;
    FILE* file_out_block = NULL;
    string path_out_block;
    FILE* file_out_iv = NULL;
    string path_out_iv;
    TYPE_INDEX num_bucket;

    cout << "    [evict] Writing back shuffled data to disk..." << endl;
    for (int l = 0; l < H; l++)
    {
        num_bucket = sss.getSibling(fullPathIdx[l+1]);
        // cout << "    [evict] Layer " << l << ": Writing to bucket " << num_bucket << " (sibling of " << fullPathIdx[l+1] << ")" << endl;
        path_out_block = rootPath + to_string(this->serverNo) + "/" + to_string(num_bucket);
        // cout << "    [evict] Layer " << l << ": Writing to bucket " << num_bucket << " (sibling of " << fullPathIdx[l+1] << ")" << endl;
        path_out_iv = rootPath + "client_iv" + to_string(this->serverNo+1) + "/" + to_string(num_bucket);
        if((file_out_block = fopen(path_out_block.c_str(),"wb+")) == NULL)
        {
            cout<< "	[evict] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        for (int i = 0; i < DATA_CHUNKS; i++)
        {
            fwrite(&dot_product_vector_xored[i][l*BUCKET_SIZE], 1, sizeof(TYPE_DATA)*BUCKET_SIZE, file_out_block);
        }
        fclose(file_out_block);

        if((file_out_iv = fopen(path_out_iv.c_str(),"wb+")) == NULL)
        {
            cout<< "	[evict] File Cannot be Opened!!" <<endl;
            exit(0);
        }
        // write newly generated IVs for the permuted slots
        fwrite(&list_iv[l*BUCKET_SIZE], 1, sizeof(block)*BUCKET_SIZE, file_out_iv);
        fclose(file_out_iv);
    }

    // 写叶子节点，叶子节点的数据仍然在自己上第H个上，即不像前面的数据，需要把父节点的数据移动到孩子节点的兄弟节点上
    num_bucket = fullPathIdx[H];
    path_out_block = rootPath + to_string(this->serverNo) + "/" + to_string(num_bucket);
    // cout << "    [evict] Leaf (layer " << H << "): Writing to bucket " << num_bucket << endl;
    path_out_iv = rootPath + "client_iv" + to_string(this->serverNo+1) + "/" + to_string(num_bucket);
    if((file_out_block = fopen(path_out_block.c_str(),"wb+")) == NULL)
    {
        cout<< "	[evict] File Cannot be Opened!!" <<endl;
        exit(0);
    }
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        fwrite(&dot_product_vector_xored[i][H*BUCKET_SIZE], 1, sizeof(TYPE_DATA)*BUCKET_SIZE, file_out_block);
    }
    fclose(file_out_block);
    if((file_out_iv = fopen(path_out_iv.c_str(),"wb+")) == NULL)
    {
        cout<< "	[evict] File Cannot be Opened!!" <<endl;
        exit(0);
    }
    // write new IVs for the leaf bucket
    fwrite(&list_iv[H*BUCKET_SIZE], 1, sizeof(block)*BUCKET_SIZE, file_out_iv);
    fclose(file_out_iv);

    // 写根节点，根节点是置换的最后一个节点的数据（第H+1层）
    num_bucket = fullPathIdx[0];
    path_out_block = rootPath + to_string(this->serverNo) + "/" + to_string(num_bucket);
    cout << "    [evict] Root (layer 0): Writing to bucket " << num_bucket << " using shuffle layer " << (H+1) << endl;
    path_out_iv = rootPath + "client_iv" + to_string(this->serverNo+1) + "/" + to_string(num_bucket);
    if((file_out_block = fopen(path_out_block.c_str(),"wb+")) == NULL)
    {
        cout<< "	[evict] File Cannot be Opened!!" <<endl;
        exit(0);
    }
    for (int i = 0; i < DATA_CHUNKS; i++)
    {
        fwrite(&dot_product_vector_xored[i][(H+1)*BUCKET_SIZE], 1, sizeof(TYPE_DATA)*BUCKET_SIZE, file_out_block);
    }
    fclose(file_out_block);
    if((file_out_iv = fopen(path_out_iv.c_str(),"wb+")) == NULL)
    {
        cout<< "	[evict] File Cannot be Opened!!" <<endl;
        exit(0);
    }
    // write new IVs for the root bucket (from shuffle layer H+1)
    fwrite(&list_iv[(H+1)*BUCKET_SIZE], 1, sizeof(block)*BUCKET_SIZE, file_out_iv);
    fclose(file_out_iv);
    
    end = time_now;
    cout<< "	[SendBlock] Eviction time in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() <<endl;

    socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS));
	cout<< "	[evict] ACK is SENT!" <<endl;

    return 0;
}

void ServerDuetORAM::transpose_parallel(TYPE_DATA** src, TYPE_DATA** dst, int R, int C)
{
    const int BLOCK_SIZE_CPU = 32;

    // OpenMP 并行化外层循环
    // collapse(2) 表示把两层循环合并并行处理，增加并行度
    #pragma omp parallel for collapse(2) schedule(static)
    for (int i = 0; i < R; i += BLOCK_SIZE_CPU) {
        for (int j = 0; j < C; j += BLOCK_SIZE_CPU) {
            
            int i_limit = std::min(i + BLOCK_SIZE_CPU, R);
            int j_limit = std::min(j + BLOCK_SIZE_CPU, C);

            // 内部小块依然串行，利用 L1 Cache
            for (int ii = i; ii < i_limit; ++ii) {
                for (int jj = j; jj < j_limit; ++jj) {
                    dst[jj][ii] = src[ii][jj];
                }
            }
        }
    }
}

void ServerDuetORAM::xor_vectors_optimized(TYPE_DATA** vec_a, TYPE_DATA** vec_b, int rows, int cols)
{
    // 1. OpenMP 并行：将不同的行分配给不同的 CPU 核心
    // schedule(static) 对于这种负载均衡的任务最高效
    #pragma omp parallel for schedule(static)
    for (int i = 0; i < rows; i++) {
        
        TYPE_DATA* row_a = vec_a[i];
        TYPE_DATA* row_b = vec_b[i];
        
        int j = 0;

        // 2. AVX2 核心优化部分
        // 每次处理 4 个 unsigned long long (4 * 64bit = 256bit)
        // 只要剩余元素 >= 4 个，就用 SIMD 指令轰炸
        for (; j <= cols - 4; j += 4) {
            
            // Step A: 加载数据 (使用 loadu 处理非对齐内存，防止 new 出来的地址不是32字节对齐)
            // 将 4 个 unsigned long long 当作一个整体加载到寄存器
            __m256i va = _mm256_loadu_si256((__m256i*)&row_a[j]);
            __m256i vb = _mm256_loadu_si256((__m256i*)&row_b[j]);

            // Step B: 执行并行异或
            __m256i res = _mm256_xor_si256(va, vb);

            // Step C: 存回结果
            _mm256_storeu_si256((__m256i*)&row_a[j], res);
        }

        // 3. 处理尾巴 (Tail Case)
        // 如果列数不是 4 的倍数，剩下这 1~3 个元素用普通方法处理
        for (; j < cols; j++) {
            row_a[j] = row_a[j] ^ row_b[j];
        }
    }
}

void ServerDuetORAM::efficient_rotate()
{
    // 1. 定义别名，消除重复逻辑
    // 如果是 Server0: arr1 是 received, arr2 是 full
    // 如果是 Server1: arr1 是 full,     arr2 是 received
    auto* arr1 = (this->serverNo == 0) ? receivedPuncturedPermutation : fullPermutation;
    auto* arr2 = (this->serverNo == 0) ? fullPermutation : receivedPuncturedPermutation;

    // 2. 并行处理第一个数组 (基于 circularShift_1)
    // 使用 static 调度，因为每次旋转的工作量是相同的（假设 SIZE_PI 固定）
    #pragma omp parallel for schedule(static)
    for (int i = 0; i < SIZE_PI; i++)
    {
        // 算出当前行的起始和结束指针
        auto start = arr1 + i * SIZE_PI;
        auto end   = start + SIZE_PI;
        auto mid   = end - circularShift_1[i]; // 右移 k 位 = rotate(start, end-k, end)
        
        std::rotate(start, mid, end);
    }

    // 3. 并行处理第二个数组 (基于 circularShift_2)
    #pragma omp parallel for schedule(static)
    for (int j = 0; j < SIZE_PI; j++)
    {
        auto start = arr2 + j * SIZE_PI;
        auto end   = start + SIZE_PI;
        auto mid   = end - circularShift_2[j];
        
        std::rotate(start, mid, end);
    }
}

// void* ServerDuetORAM::thread_socket_func(void* args)
// {

//     ThreadSocketArgs* arg = (ThreadSocketArgs*)args;
//     zmq::socket_t* socket = arg->socket;
//     struct_socket* sockData = arg->sockData;
	
// 	if(sockData->isSend)
// 	{
// 		auto start = time_now;
// 		send(sockData->ADDR, sockData->data_out, sockData->data_out_size, socket);
// 		auto end = time_now;
// 		if(thread_max < std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count())
// 			thread_max = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
// 	}
// 	else
// 	{
// 		recv(sockData->ADDR, sockData->data_in, sockData->data_in_size, socket);
// 	}
//     pthread_exit((void*)arg);
// }

int ServerDuetORAM::send(std::string ADDR, unsigned char* input, TYPE_INDEX inputSize, zmq::socket_t* socket)
{
    // zmq::context_t context(1);
    // zmq::socket_t socket(context,ZMQ_REQ);

    // socket.connect(ADDR.c_str());
	
    unsigned char buffer_in[sizeof(CMD_SUCCESS)];

    try
    {
		cout<< "	[ThreadedSocket] Sending to " << ADDR << endl;
		// socket.send (input, inputSize);
        socket->send (input, inputSize);
		cout<< "	[ThreadedSocket] Data SENT!" << ADDR << endl;
        
        // socket.recv(buffer_in, sizeof(CMD_SUCCESS));
        socket->recv(buffer_in, sizeof(CMD_SUCCESS));
        cout<< "	[ThreadedSocket] ACK RECEIVED!" << ADDR << endl;
    }
    catch (exception &ex)
    {
        goto exit;
    }

exit:
	// socket.disconnect(ADDR.c_str());
	// socket.close();
	return 0;
}

int ServerDuetORAM::recv(std::string ADDR, unsigned char* output, TYPE_INDEX outputSize, zmq::socket_t* socket)
{
    // zmq::context_t context(1);
    // zmq::socket_t socket(context,ZMQ_REP);
	
    // socket.bind(ADDR.c_str());

    try
    {
		cout<< "	[ThreadedSocket] Waiting Client on " << ADDR << endl;
		// socket.recv (output, outputSize);
        socket->recv (output, outputSize);
		cout<< "	[ThreadedSocket] Data RECEIVED! " << ADDR <<endl;
        
        // socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS));
        socket->send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS));
        cout<< "	[ThreadedSocket] ACK SENT! "  << ADDR<<endl;
    }
    catch (exception &ex)
    {
        cout<<"Socket error!";
        goto exit;
    }
    
exit:
	// socket.close();
	return 0;
}

void* ServerDuetORAM::thread_socket_func(void* args)
{
    struct_socket* opt = (struct_socket*) args;

    if (opt->isSend) 
    {
        auto start = time_now;
        send(opt->ADDR, opt->data_out, opt->data_out_size);
        auto end = time_now;
        if(thread_max < std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count())
			thread_max = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    }
    else
    {
        recv(opt->ADDR, opt->data_in, opt->data_in_size);
    }
    pthread_exit((void*)opt);
}

int ServerDuetORAM::send(std::string ADDR, unsigned char* input, TYPE_INDEX inputSize)
{
    zmq::context_t context(1);
    zmq::socket_t socket(context,ZMQ_REQ);

    socket.connect(ADDR.c_str());
	
    unsigned char buffer_in[sizeof(CMD_SUCCESS)];

    try
    {
		cout<< "	[ThreadedSocket] Sending to " << ADDR << endl;
		socket.send (input, inputSize);
		cout<< "	[ThreadedSocket] Data SENT!" << ADDR << endl;
        
        socket.recv(buffer_in, sizeof(CMD_SUCCESS));
        cout<< "	[ThreadedSocket] ACK RECEIVED!" << ADDR << endl;
    }
    catch (exception &ex)
    {
        goto exit;
    }

exit:
	socket.disconnect(ADDR.c_str());
	socket.close();
	return 0;
}

int ServerDuetORAM::recv(std::string ADDR, unsigned char* output, TYPE_INDEX outputSize)
{
    zmq::context_t context(1);
    zmq::socket_t socket(context,ZMQ_REP);
	
    socket.bind(ADDR.c_str());

    try
    {
		cout<< "	[ThreadedSocket] Waiting Client on " << ADDR << endl;
		socket.recv (output, outputSize);
		cout<< "	[ThreadedSocket] Data RECEIVED! " << ADDR <<endl;
        
        socket.send((unsigned char*)CMD_SUCCESS,sizeof(CMD_SUCCESS));
        cout<< "	[ThreadedSocket] ACK SENT! "  << ADDR<<endl;
    }
    catch (exception &ex)
    {
        cout<<"Socket error!";
        goto exit;
    }
    
exit:
	socket.close();
	return 0;
}
