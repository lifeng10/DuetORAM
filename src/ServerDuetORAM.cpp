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

ServerDuetORAM::ServerDuetORAM(TYPE_INDEX serverNo, int selectedThreads) {
    this->CLIENT_ADDR = "tcp://*:" + std::to_string(SERVER_PORT+(serverNo)*NUM_SERVERS+serverNo);

    this->numThreads = selectedThreads;
    this->thread_compute = new pthread_t[numThreads];

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
    this->block_buffer_out = new unsigned char[sizeof(TYPE_DATA)*DATA_CHUNKS];
    this->block_buffer_in = new unsigned char[sizeof(TYPE_DATA)*DATA_CHUNKS+sizeof(TYPE_INDEX)+sizeof(block)];
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

    DuetORAM ORAM;
	TYPE_INDEX fullPathIdx[H+1];
    ORAM.getFullPathIdx(fullPathIdx, pathID);

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

    for(int i = 0, startIdx = 0 ; i < numThreads , startIdx < DATA_CHUNKS; i ++, startIdx+=step)
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
    
    for(int i = 0, startIdx = 0 ; i < numThreads , startIdx < DATA_CHUNKS; i ++, startIdx+=step)
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
            exit;
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
        for (int j = 0; j < (BUCKET_SIZE*(H+1)); j++)
        {
            if (opt->sharedVector[j] == 1)
            {
                opt->dot_product_output[i] = opt->dot_product_output[i] ^ opt->data_vector[i][j];
            }
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
    memcpy(&slotIdx, &block_buffer_in[0], sizeof(TYPE_INDEX));
    memcpy(recvBlock, &block_buffer_in[sizeof(TYPE_INDEX)], sizeof(TYPE_DATA)*DATA_CHUNKS);
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





