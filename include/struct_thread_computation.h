#ifndef STRUCT_THREAD_CROSSPRODUCT_H
#define  STRUCT_THREAD_CROSSPRODUCT_H

#include "config.h"
typedef struct struct_thread_computation
{
    //thread
    TYPE_INDEX start,end;
	
    //retrieval
    TYPE_DATA** data_vector;                            // load bucket data from disk       
    uint8_t* sharedVector;                                 //selected query
    TYPE_DATA* dot_product_output;                       //result, DATA_CHUNKS

    // variables for masking data to another server, XOR operation, without permutation
    TYPE_DATA** a_extension;
    TYPE_DATA** dot_product_vector;
    TYPE_DATA** masked_data_send;

    //variables for masking data to another server, with permutation
    TYPE_INDEX* sub_pi;
    TYPE_DATA** masked_data_recv;
    TYPE_DATA** delta_extension;
    TYPE_DATA** local_data;

    // XOR operation for dot_product_vector_xored
    TYPE_DATA** dot_product_vector_xored;
    TYPE_DATA** dot_product_vector_xored_in;


    
    struct_thread_computation(TYPE_INDEX start, TYPE_INDEX end, TYPE_DATA** input, uint8_t* sharedVector, TYPE_DATA* dot_product_output)
    {
        this->start = start;
        this->end = end;
        this->data_vector = input;
        this->sharedVector = sharedVector;
        this->dot_product_output = dot_product_output;
    }

    struct_thread_computation(TYPE_INDEX start, TYPE_INDEX end, TYPE_DATA** dot_product_vector_xored, TYPE_DATA** dot_product_vector_xored_in)
    {
        this->start = start;
        this->end = end;
        this->dot_product_vector_xored = dot_product_vector_xored;
        this->dot_product_vector_xored_in = dot_product_vector_xored_in;
    }

    struct_thread_computation(TYPE_INDEX start, TYPE_INDEX end, TYPE_DATA** a_extension, TYPE_DATA** dot_product_vector, TYPE_DATA** masked_data_send)
    {
        this->start = start;
        this->end = end;
        this->a_extension = a_extension;
        this->dot_product_vector = dot_product_vector;
        this->masked_data_send = masked_data_send;
    }

    struct_thread_computation(TYPE_INDEX start, TYPE_INDEX end, TYPE_INDEX* sub_pi, TYPE_DATA** masked_data_recv, TYPE_DATA** delta_extension, TYPE_DATA** dot_product_vector, TYPE_DATA** local_data)
    {
        this->start = start;
        this->end = end;
        this->sub_pi = sub_pi;
        this->masked_data_recv = masked_data_recv;
        this->delta_extension = delta_extension;
        this->dot_product_vector = dot_product_vector;
        this->local_data = local_data;
    }
    
	
	struct_thread_computation()
	{
	}
	~struct_thread_computation()
	{
	}

}THREAD_COMPUTATION;

#endif