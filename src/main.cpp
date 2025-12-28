#include "config.h"
#include "ClientDuetORAM.hpp"
#include "ServerDuetORAM.hpp"

unsigned int nthreads = std::thread::hardware_concurrency();  // Returns the number of concurrent threads supported by the implementation. The value should be considered only a hint.

int main(){
    // Create directories
    string mkdir_cmd = "mkdir -p ";
    string mkdir_localState = mkdir_cmd + clientLocalDir;
    string mkdir_unsharedData = mkdir_cmd + clientDataDir;
    string mkdir_log = mkdir_cmd + logDir;
    system(mkdir_localState.c_str());
    system(mkdir_unsharedData.c_str());
    system(mkdir_log.c_str());
    for(int i = 0 ; i < NUM_SERVERS; i++)
    {
        string mkdir_sharedData = mkdir_cmd +  rootPath + to_string(i);
        system(mkdir_sharedData.c_str());
    }
    string mkdir_unsharedData_iv1 = mkdir_cmd + clientDataDir_iv1;
    system(mkdir_unsharedData_iv1.c_str());
    string mkdir_unsharedData_iv2 = mkdir_cmd + clientDataDir_iv2;
    system(mkdir_unsharedData_iv2.c_str());



    int choice;

    cout << "CLIENT(1) or SERVER(2): ";
	cin >> choice;
	cout << endl;


    if (choice == 1)
    {
        ClientDuetORAM* client = new ClientDuetORAM();
        int access, start;
        char response = ' ';
        int random_access;
        int subOpt;
        cout<<"LOAD PREBUILT DATA (1) OR CREATE NEW ORAM (2)? "<<endl;
        cin>>subOpt;
        cout<<endl;
        if (subOpt == 1)
        {
            
        }
        else
        {
            client->init();
        }
        
    }
    else if (choice == 2)
    {
        /* code */
    }
    else{
        cout << "Invalid choice. Exiting." << endl;
        return -1;
    }
    

    return 0;
}