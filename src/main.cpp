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
    string mkdir_keys = mkdir_cmd + keysPath;
    system(mkdir_keys.c_str());



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
            client->load();
        }
        else
        {
            client->init();
            do
            {
                cout << "TRANSMIT INITIALIZED DuetORAM DATA TO NON-LOCAL SERVERS? (y/n)";
                cin >> response;
                response = tolower(response);
            } 
            while ( !cin.fail() && response!='y' && response!='n' );
            if (response == 'y')
            {
                auto start = time_now;
                client->sendORAMTree();
                auto end = time_now;
				cout<< "	[main] ORAM Tree Sent to Servers in " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()<< " ns"<<endl;
            }
            
        }
        cout << "SERVERS READY? (Press ENTER to Continue)";
		cin.ignore();
		cin.ignore();
		cin.clear();
		cout << endl<<endl<<endl;

    beginning:
        cout << "SEQUENTIAL WARM-UP(1) OR RANDOM ACCESS(2)?";
		cin >> choice;
		cout << endl;

        if (choice == 1)
        {
            cout << "WARM_UP PROCESSION HAS NOT BEEN IMPLEMENTED YET!!!" << endl;
        }
        else if (choice == 2)
        {
            cout << "HOW MANY RANDOM ACCESS?";
			cin >> access;

            for(int j = 1; j <= access; j++)
            {
                random_access = rand() % NUM_BLOCK + 1; 
                cout << endl;
                cout << "=================================================================" << endl;
                cout << "[main] Random Access for " << random_access << " IS STARTING!" <<endl;
                cout << "=================================================================" << endl;
                    
                    
                client->access(random_access);
                    
                cout << "=================================================================" << endl;
                cout << "[main] Random Access for " << random_access << " IS COMPLETED!" <<endl;
                cout << "=================================================================" << endl;
            }

            cout << endl;
            do
            {
                cout << "DO YOU WANT TO START OVER? (y/n)";
                cin >> response;
                response = tolower(response);
            } while ( !cin.fail() && response!='y' && response!='n' );
            
            if (response == 'y')
            {
                goto beginning;
            }
        }
        else
        {
            cout << "INVALID CHOICE. EXITING." << endl;
            return -1;
        }
        cout << "BYE!" << endl;
    }
    else if (choice == 2)
    {
        int serverNo;
        int selectedThreads;
		cout << "Enter the Server No (1-"<< NUM_SERVERS <<"):";
		cin >> serverNo;
		cin.clear();
		cout << endl;

        do
        {
            cout<< "How many computation threads to use? (1-"<<nthreads<<"): ";
            cin>>selectedThreads;
		}while(selectedThreads>nthreads);
        
		ServerDuetORAM*  server = new ServerDuetORAM(serverNo-1,selectedThreads);
		server->start();
    }
    else{
        cout << "Invalid choice. Exiting." << endl;
        return -1;
    }
    

    return 0;
}