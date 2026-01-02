#ifndef SECRETSHAREDSHUFFLE_HPP
#define SECRETSHAREDSHUFFLE_HPP

#include "config.h"
#include "struct_block.h"
#include "struct_socket.h"
#include "struct_thread_computation.h"
#include "zmq.hpp"

class SecretSharedShuffle {
public:
    SecretSharedShuffle();
    ~SecretSharedShuffle();

    int generateShuffle(vector<struct_block> blocks, TYPE_INDEX pathID, TYPE_INDEX* pi, TYPE_POS_MAP* pos_map, TYPE_ID** metaData, block& seed_iv1, block& seed_iv2);
    int shareTranslation();
    int initialize(block& key3, block& key4, block* permutationAa, block* permutationBb, TYPE_INDEX* u1, TYPE_INDEX* u2);
    int secretShare(TYPE_INDEX serverNo, int numThreads, TYPE_INDEX* sub_pi, TYPE_DATA** dot_product_vector, TYPE_DATA** delta_extension, TYPE_DATA** a_extension, TYPE_DATA** b_extension, TYPE_DATA** resultShare);
    TYPE_INDEX P(TYPE_INDEX pathID, int l);
    TYPE_INDEX getSibling(TYPE_INDEX pIDl);

    static void* thread_dotProduct_mask_func(void* args);
    static void* thread_dotProduct_local_func(void* args);
};

#endif