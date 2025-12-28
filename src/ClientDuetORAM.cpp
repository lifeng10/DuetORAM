#include "ClientDuetORAM.hpp"
#include "DuetORAM.hpp"

unsigned long int ClientDuetORAM::exp_logs[9];
unsigned long int ClientDuetORAM::thread_max = 0; //用于记录执行线程的时间。因为有多个线程，只记录耗时最长的线程所用的时间
char ClientDuetORAM::timestamp[16];

ClientDuetORAM::ClientDuetORAM() {
    this->pos_map = new TYPE_POS_MAP[NUM_BLOCK+1];
    this->metaData = new TYPE_DATA*[NUM_NODES];

    for (int i = 0; i < NUM_NODES; i++)
    {
        this->metaData[i] = new TYPE_ID[BUCKET_SIZE];
    }

    // 生成的查询向量
    this->sharedVector = new uint8_t*[NUM_SERVERS];     // vector for retrieval, XOR {0,1}
	for(int i = 0; i < NUM_SERVERS; i++)
	{
		this->sharedVector[i] = new uint8_t[(H+1)*BUCKET_SIZE];
	}

    // 收两个服务器返回的检索结果
    retrievedShare = new TYPE_DATA*[NUM_SERVERS];       // retrieval result: block shares from servers
	for(int k = 0 ; k < NUM_SERVERS; k++)
    {
        retrievedShare[k] = new TYPE_DATA[DATA_CHUNKS];
    }

    // 恢复查询block结果
    recoveredBlock = new TYPE_DATA[DATA_CHUNKS];
}

ClientDuetORAM::~ClientDuetORAM() {
    
}

int ClientDuetORAM::init() {
    this->numRead = 0;
    this->numEvict = 0;
    PRG().random_block(&this->k1, 1);
    PRG().random_block(&this->k2, 1);

    auto start = time_now;
    auto end = time_now;

    for (TYPE_INDEX i = 1; i <= NUM_BLOCK; i++) {
        this->pos_map[i].pathID = -1;
        this->pos_map[i].pathIdx = -1;
    }

    start = time_now;
    DuetORAM ORAM;
    ORAM.build(this->pos_map, this->metaData, this->k1, this->k2);
    end = time_now;

}