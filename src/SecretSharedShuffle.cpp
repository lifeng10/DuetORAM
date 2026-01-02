#include "SecretSharedShuffle.hpp"

SecretSharedShuffle::SecretSharedShuffle() {
    
}
SecretSharedShuffle::~SecretSharedShuffle() {

}

int SecretSharedShuffle::generateShuffle(vector<struct_block> blocks, TYPE_INDEX pathID, TYPE_INDEX* pi, TYPE_POS_MAP* pos_map, TYPE_ID** metaData, block& seed_iv1, block& seed_iv2)
{
    std::fill(pi, pi + (BUCKET_SIZE*(H+2)), -1);

    PRG prg;
    prg.random_block(&seed_iv1, 1);
    prg.random_block(&seed_iv2, 1);

    block* list_iv1 = new block[BUCKET_SIZE*(H+2)];
    block* list_iv2 = new block[BUCKET_SIZE*(H+2)];
    prg.reseed(&seed_iv1);
    prg.random_block(list_iv1, BUCKET_SIZE*(H+2));
    prg.reseed(&seed_iv2);
    prg.random_block(list_iv2, BUCKET_SIZE*(H+2));

    for (int l = H; l >= 0; l--)
    {
        if (l == H)
        {
            vector<struct_block> blocks_evicted = vector<struct_block>();
            TYPE_INDEX pIDl = P(pathID, l);
            int counter = 0;
            memset(metaData[pIDl], 0, sizeof(TYPE_ID)*BUCKET_SIZE);

            for (struct_block block_inS: blocks)
            {
                if(counter >= BUCKET_SIZE)
                {
                    cout << "       [SecretSharedShuffle] Error: Too many blocks to evict to leaf bucket!" << endl;
                    return 0;
                }
                if (pIDl == P(block_inS.pathID, l))
                {
                    blocks_evicted.push_back(block_inS);
                    pi[BUCKET_SIZE * l + counter] = block_inS.pathIdx;
                    pos_map[block_inS.blockID].pathID = block_inS.pathID;
                    pos_map[block_inS.blockID].pathIdx = BUCKET_SIZE * l + counter;
                    pos_map[block_inS.blockID].iv1 = list_iv1[BUCKET_SIZE * l + counter];
                    pos_map[block_inS.blockID].iv2 = list_iv2[BUCKET_SIZE * l + counter];
                    metaData[pIDl][counter] = block_inS.blockID;
                    counter++;
                }
                
            }

            //remove blocks that in blocks_evicted from S
            for (int i = 0; i < blocks_evicted.size(); i++)
            {
                for (TYPE_INDEX j = 0; j < blocks.size(); j++)
                {
                    struct_block block_inS = blocks.at(j);
                    if (block_inS.blockID == blocks_evicted.at(i).blockID)
                    {
                        blocks.erase(blocks.begin() + j);
                        break;
                    }
                }
            }
            
            
        }
        else
        {
            vector<struct_block> blocks_evicted = vector<struct_block>();
            TYPE_INDEX pIDl = P(pathID, l);
            TYPE_INDEX pIDl_plus_1 = P(pathID, l+1);
            TYPE_INDEX pIDl_plus_1_sibling = getSibling(pIDl_plus_1);
            int counter = 0;
            memset(metaData[pIDl], 0, sizeof(TYPE_ID)*BUCKET_SIZE);
            memset(metaData[pIDl_plus_1_sibling], 0, sizeof(TYPE_ID)*BUCKET_SIZE);
            for (struct_block block_inS : blocks)
            {
                if(counter >= BUCKET_SIZE)
                {
                    cout << "       [SecretSharedShuffle] Error: Too many blocks to evict to leaf bucket!" << endl;
                    return 0;
                }
                if (pIDl == P(block_inS.pathID, l))
                {
                    blocks_evicted.push_back(block_inS);
                    pi[BUCKET_SIZE * l + counter] = block_inS.pathIdx;
                    pos_map[block_inS.blockID].pathID = block_inS.pathID;
                    pos_map[block_inS.blockID].pathIdx = BUCKET_SIZE * l + counter;
                    pos_map[block_inS.blockID].iv1 = list_iv1[BUCKET_SIZE * l + counter];
                    pos_map[block_inS.blockID].iv2 = list_iv2[BUCKET_SIZE * l + counter];
                    metaData[pIDl_plus_1_sibling][counter] = block_inS.blockID;
                    counter++;
                }
                
            }

            //remove blocks that in blocks_evicted from S
            for (int i = 0; i < blocks_evicted.size(); i++)
            {
                for (TYPE_INDEX j = 0; j < blocks.size(); j++)
                {
                    struct_block block_inS = blocks.at(j);
                    if (block_inS.blockID == blocks_evicted.at(i).blockID)
                    {
                        blocks.erase(blocks.begin() + j);
                        break;
                    }
                }
            }
        }
        
    }

    // complete the pi for empty slots
    std::random_device rd;
    std::mt19937 gen(rd());
    const TYPE_INDEX PERM_SIZE = (H+2)*BUCKET_SIZE;
    std::vector<bool> is_used(PERM_SIZE, false);
    for (int j = 0; j < PERM_SIZE; j++)
    {
        TYPE_INDEX target_value = pi[j];;
        if (target_value != -1)
        {
            is_used[target_value] = true;
        }
    }
    std::vector<TYPE_INDEX> unused_values;
    for (TYPE_INDEX val = 0; val < PERM_SIZE; val++)
    {
        if (!is_used[val])
        {
            unused_values.push_back(val);
        }
    }
    std::shuffle(unused_values.begin(), unused_values.end(), gen);
    auto unused_iter = unused_values.begin();

    for (int j = 0; j < PERM_SIZE; j++)
    {
        if (pi[j] == -1)
        {
            if (unused_iter == unused_values.end())
            {
                std::cerr << "Permutation logic error: unused values count mismatch." << std::endl;
                exit(1);
            }
            pi[j] = *unused_iter;
            unused_iter++;
        }
        
    }
    
    return 0;
}


TYPE_INDEX SecretSharedShuffle::P(TYPE_INDEX pathID, int l)
{
    return (TYPE_INDEX)((1<<l) - 1 + (((pathID+1) % N_leaf) >> (H - l)));
}

TYPE_INDEX SecretSharedShuffle::getSibling(TYPE_INDEX pIDl)
{
    return (pIDl % 2) ? (pIDl + 1) : (pIDl - 1);
}

int SecretSharedShuffle::initialize(block& key3, block& key4, block* permutationAa, block* permutationBb, TYPE_INDEX* u1, TYPE_INDEX* u2)
{
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<TYPE_INDEX> distribution(0, BUCKET_SIZE*(H+2) - 1);

    generate(u1, u1+BUCKET_SIZE*(H+2), [&](){
        return distribution(gen);
    });

    generate(u2, u2+BUCKET_SIZE*(H+2), [&](){
        return distribution(gen);
    });

    PRG prg;
    prg.reseed(&key3);
    prg.random_block(permutationAa, BUCKET_SIZE*(H+2)*BUCKET_SIZE*(H+2));
    prg.reseed(&key4);
    prg.random_block(permutationBb, BUCKET_SIZE*(H+2)*BUCKET_SIZE*(H+2));

    for (TYPE_INDEX i = 0; i < BUCKET_SIZE*(H+2); i++)
    {
        permutationAa[i * BUCKET_SIZE*(H+2) + u1[i]] = zero_block;
        permutationBb[i * BUCKET_SIZE*(H+2) + u2[i]] = zero_block;
    }

    return 0;
}

int SecretSharedShuffle::secretShare(TYPE_INDEX serverNo, int numThreads, TYPE_INDEX* sub_pi, TYPE_DATA** dot_product_vector, TYPE_DATA** delta_extension, TYPE_DATA** a_extension, TYPE_DATA** b_extension, TYPE_DATA** resultShare)
{
    TYPE_INDEX SIZE_PI = BUCKET_SIZE * (H + 2);

    // 0.0. multithread for computation (masked data to another server)
    THREAD_COMPUTATION dotProduct_mask_args[numThreads];
    pthread_t* thread_compute_mask = new pthread_t[numThreads];
    int endIdx;
    int step = ceil((double)DATA_CHUNKS/(double)numThreads);

    // 0.1. multithread for computation (local computation)
    THREAD_COMPUTATION dotProduct_local_args[numThreads];
    pthread_t* thread_compute_local = new pthread_t[numThreads];

    // 2. Interact with another server
    unsigned char* message_send = new unsigned char[sizeof(TYPE_DATA)*DATA_CHUNKS*SIZE_PI];
    unsigned char* message_recv = new unsigned char[sizeof(TYPE_DATA)*DATA_CHUNKS*SIZE_PI];
    memset(message_send, 0, sizeof(TYPE_DATA)*DATA_CHUNKS*SIZE_PI);
    memset(message_recv, 0, sizeof(TYPE_DATA)*DATA_CHUNKS*SIZE_PI);

    if (serverNo == 0)
    {
        int CMD;
        unsigned char buffer[sizeof(CMD)];

        zmq::context_t context(1);
        zmq::socket_t socket(context,ZMQ_REP);
        
        string CLIENT_ADDR = "tcp://*:" + to_string(SERVER_PORT+10);
        cout<< "    [Secret Shared Shuflle] Socket is OPEN on " << CLIENT_ADDR << endl;
        socket.bind(CLIENT_ADDR.c_str());

        cout << "   [Secret Shared Shuflle] Waiting for a Command..." << endl;
        socket.recv(buffer, sizeof(CMD));

        memcpy(&CMD, buffer, sizeof(CMD));
        cout << "   [Secret Shared Shuflle] Command Received!" << endl;

        socket.send((unsigned char*)CMD_SUCCESS, sizeof(CMD_SUCCESS));

        // receive shares
        socket.recv(message_recv, sizeof(TYPE_DATA)*DATA_CHUNKS*SIZE_PI, 0);

        //parse shares into TYPE_DATA
        TYPE_DATA** masked_data_recv = new TYPE_DATA*[DATA_CHUNKS];
        for (int i = 0; i < DATA_CHUNKS; i++)
        {
            masked_data_recv[i] = new TYPE_DATA[SIZE_PI];
            memcpy(masked_data_recv[i], &message_recv[i*sizeof(TYPE_DATA)*SIZE_PI], sizeof(TYPE_DATA)*SIZE_PI);
        }

        // local computation
        TYPE_DATA** local_data = new TYPE_DATA*[DATA_CHUNKS];
        for (int i = 0; i < DATA_CHUNKS; i++)
        {
            local_data[i] = new TYPE_DATA[SIZE_PI];
        }

        int threads_created_count1 = 0;
        for (int i = 0, startIdx = 0; i < numThreads, startIdx < DATA_CHUNKS; i++, startIdx+=step)
        {
            if (startIdx+step > DATA_CHUNKS)
                endIdx = DATA_CHUNKS;
            else
                endIdx = startIdx + step;
            
            dotProduct_local_args[i] = THREAD_COMPUTATION(startIdx, endIdx, sub_pi, masked_data_recv, delta_extension, dot_product_vector, local_data);
            pthread_create(&thread_compute_local[i], NULL, &SecretSharedShuffle::thread_dotProduct_local_func, (void*)&dotProduct_local_args[i]);
            threads_created_count1++;

            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i, &cpuset);
            pthread_setaffinity_np(thread_compute_local[i], sizeof(cpu_set_t), &cpuset);
        }

        for (int i = 0; i < threads_created_count1; i++)
        {
            pthread_join(thread_compute_local[i], NULL);
        }

        // compute shares & send
        TYPE_DATA** masked_data_send = new TYPE_DATA*[DATA_CHUNKS];
        for (int i = 0; i < DATA_CHUNKS; i++)
        {
            masked_data_send[i] = new TYPE_DATA[SIZE_PI];
        }

        for (int i = 0, startIdx = 0; i < numThreads, startIdx < DATA_CHUNKS; i++, startIdx+=step)
        {
            if (startIdx+step > DATA_CHUNKS)
                endIdx = DATA_CHUNKS;
            else
                endIdx = startIdx + step;
            
            dotProduct_mask_args[i] = THREAD_COMPUTATION(startIdx, endIdx, a_extension, local_data, masked_data_send);
            pthread_create(&thread_compute_mask[i], NULL, &SecretSharedShuffle::thread_dotProduct_mask_func, (void*)&dotProduct_mask_args[i]);

            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i, &cpuset);
            pthread_setaffinity_np(thread_compute_mask[i], sizeof(cpu_set_t), &cpuset);
        }

        for (int i = 0; i < threads_created_count1; i++)
        {
            pthread_join(thread_compute_mask[i], NULL);
        }

        for (int i = 0; i < DATA_CHUNKS; i++)
        {
            memcpy(&message_send[i*sizeof(TYPE_DATA)*SIZE_PI], masked_data_send[i], sizeof(TYPE_DATA)*SIZE_PI);
        }

        socket.send(message_send, sizeof(TYPE_DATA)*DATA_CHUNKS*SIZE_PI);
        socket.recv(buffer, sizeof(CMD_SUCCESS));
        cout << "   [Secret Shared Shuflle] Secret Shared Shuffle is Finished!" << endl;

        //result share
        for (size_t i = 0; i < DATA_CHUNKS; i++)
        {
            memcpy(resultShare[i], b_extension[i], sizeof(TYPE_DATA)*BUCKET_SIZE*2);
        }

        socket.close();

        for (size_t i = 0; i < DATA_CHUNKS; i++)
        {
            delete[] masked_data_recv[i];
            delete[] masked_data_send[i];
            delete[] local_data[i];
        }
        delete[] masked_data_recv;
        delete[] masked_data_send;
        delete[] local_data;

    }

    if (serverNo == 1)
    {
        int cmd_share = CMD_SHARE;
        unsigned char buffer_in[sizeof(CMD_SUCCESS)];
	    unsigned char buffer_out[sizeof(cmd_share)];
        memcpy(buffer_out, &cmd_share,sizeof(cmd_share));

        zmq::context_t context(1);
        zmq::socket_t socket(context,ZMQ_REQ);
        struct_socket thread_args;

        string ADDR = "tcp://localhost:" + to_string(SERVER_PORT+10);
        cout<< "	[Secret Shared Shuflle] Connecting to " << ADDR <<endl;
        socket.connect( ADDR.c_str());

        socket.send(buffer_out, sizeof(cmd_share));
        cout << "   [Secret Shared Shuflle] Command Sent!" << endl;
        socket.recv(buffer_in, sizeof(CMD_SUCCESS));

        // compute shares & send
        TYPE_DATA** masked_data_send = new TYPE_DATA*[DATA_CHUNKS];
        for (int i = 0; i < DATA_CHUNKS; i++)
        {
            masked_data_send[i] = new TYPE_DATA[SIZE_PI];
            memset(masked_data_send[i], 0, sizeof(TYPE_DATA)*SIZE_PI);
        }

        int threads_created_count = 0;
        for (int i = 0, startIdx = 0; i < numThreads, startIdx < DATA_CHUNKS; i++, startIdx+=step)
        {
            if (startIdx+step > DATA_CHUNKS)
                endIdx = DATA_CHUNKS;
            else
                endIdx = startIdx + step;
            
            dotProduct_mask_args[i] = THREAD_COMPUTATION(startIdx, endIdx, a_extension, dot_product_vector, masked_data_send);
            pthread_create(&thread_compute_mask[i], NULL, &SecretSharedShuffle::thread_dotProduct_mask_func, (void*)&dotProduct_mask_args[i]);
            threads_created_count++;

            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i, &cpuset);
            pthread_setaffinity_np(thread_compute_mask[i], sizeof(cpu_set_t), &cpuset);
        }

        for (int i = 0; i < threads_created_count; i++)
        {
            pthread_join(thread_compute_mask[i], NULL);
        }

        for (TYPE_DATA i = 0; i < DATA_CHUNKS; i++)
        {
            memcpy(&message_send[i*sizeof(TYPE_DATA)*SIZE_PI], masked_data_send[i], sizeof(TYPE_DATA)*SIZE_PI);
        }
        socket.send(message_send, sizeof(TYPE_DATA)*DATA_CHUNKS*SIZE_PI);

        //receive shares
        socket.recv(message_recv, sizeof(TYPE_DATA)*DATA_CHUNKS*SIZE_PI, 0);

        //parse shares into TYPE_DATA
        TYPE_DATA** masked_data_recv = new TYPE_DATA*[DATA_CHUNKS];
        for (int i = 0; i < DATA_CHUNKS; i++)
        {
            masked_data_recv[i] = new TYPE_DATA[SIZE_PI];
            memcpy(masked_data_recv[i], &message_recv[i*sizeof(TYPE_DATA)*SIZE_PI], sizeof(TYPE_DATA)*SIZE_PI);
        }


        //local computation (result share)
        for (int i = 0, startIdx = 0; i < numThreads, startIdx < DATA_CHUNKS; i++, startIdx+=step)
        {
            if (startIdx+step > DATA_CHUNKS)
                endIdx = DATA_CHUNKS;
            else
                endIdx = startIdx + step;
            
            dotProduct_local_args[i] = THREAD_COMPUTATION(startIdx, endIdx, sub_pi, masked_data_recv, delta_extension, b_extension, resultShare);
            pthread_create(&thread_compute_local[i], NULL, &SecretSharedShuffle::thread_dotProduct_local_func, (void*)&dotProduct_local_args[i]);

            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i, &cpuset);
            pthread_setaffinity_np(thread_compute_local[i], sizeof(cpu_set_t), &cpuset);
        }

        for (int i = 0; i < threads_created_count; i++)
        {
            pthread_join(thread_compute_local[i], NULL);
        }

        socket.send((unsigned char*)CMD_SUCCESS, sizeof(CMD_SUCCESS));
        cout << "   [Secret Shared Shuflle] Secret Shared Shuffle is Finished!" << endl;

        socket.disconnect(ADDR.c_str());
        socket.close();

        for (size_t i = 0; i < DATA_CHUNKS; i++)
        {
            delete[] masked_data_recv[i];
            delete[] masked_data_send[i];
        }
        delete[] masked_data_recv;
        delete[] masked_data_send;
    }

    delete[] thread_compute_local;
    delete[] thread_compute_mask;
    delete[] message_recv;
    delete[] message_send;
    

    return 0;
}

void* SecretSharedShuffle::thread_dotProduct_mask_func(void* args) {
    THREAD_COMPUTATION* opt = (THREAD_COMPUTATION*) args;
    for (int k = opt->start; k < opt->end; k++)
    {
        for (TYPE_INDEX i = 0; i < BUCKET_SIZE*(H+2); i++)
        {
            opt->masked_data_send[k][i] = opt->a_extension[k][i] ^ opt->dot_product_vector[k][i];
        }
    }
    pthread_exit((void*)opt);
}

void* SecretSharedShuffle::thread_dotProduct_local_func(void* args) {
    THREAD_COMPUTATION* opt = (THREAD_COMPUTATION*) args;
    for (int k = opt->start; k < opt->end; k++)
    {
        for (TYPE_INDEX i = 0; i < BUCKET_SIZE*(H+2); i++)
        {
            opt->local_data[k][i] = opt->delta_extension[k][i] ^ opt->masked_data_recv[k][opt->sub_pi[i]] ^ opt->dot_product_vector[k][opt->sub_pi[i]];
        }
    }
    pthread_exit((void*)opt);
}

