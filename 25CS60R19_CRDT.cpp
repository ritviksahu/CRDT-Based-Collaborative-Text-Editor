#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <fcntl.h>
#include <cstring>
#include <csignal>
#include <pthread.h>
#include <ctime>
#include <cerrno>
#include <map>
#include <algorithm>
#include <atomic>
#include <new>

using namespace std;
using namespace std::chrono;

const char* reg_path = "/tmp";
const int reg_keyID = 2024;
const int MQ_keyID_base = 2025;
const int MAX_USERS = 5;
const int MAX_ID_LEN = 50;
const int poll_sec = 2;
const int max_op_buffer = 5;
#define max_content 256

const long msg_op_update = 1; 
const long msg_hello_rq = 2; 

struct UserSlot {
    atomic<int> state; 
    char user_id[MAX_ID_LEN];
    int msqid;
};

struct Registry {
    atomic<int> active_user_count;
    UserSlot users[MAX_USERS];
};

enum EditType { INSERT, DELETE, REPLACE };

struct DocOp {
    EditType type;
    int line_num;
    char new_content[max_content];
    long timestamp;
    char user_id[MAX_ID_LEN];
};

struct MsgPkt {
    long mtype;
    DocOp op;
};

static Registry* registry = nullptr;
static int shm_id = -1;
static string username = "";
static volatile bool running = true;
static vector<string> doc_lines;
static string filename = "";

static vector<DocOp> pendinops;
static vector<DocOp> remote_ops;
static pthread_t listener_thread;
static int my_mqid = -1;
static key_t my_mqkey = -1;
static int my_shm_slot = -1; 

static atomic_flag remote_lock = ATOMIC_FLAG_INIT;
static atomic_flag sync_lock = ATOMIC_FLAG_INIT;

static vector<string> sync_reqs; 

void clear_screen() {
    cout << "\033[2J\033[1;1H" << flush;
}

string get_time_str() {
    auto now = system_clock::now();
    auto in_time_t = system_clock::to_time_t(now);
    stringstream ss;
    ss << put_time(localtime(&in_time_t), "%Y-%m-%d %X");
    return ss.str();
}

long get_timestamp() {
    return duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()
    ).count();
}

//  File I/O Functions 
vector<string> read_file( string& filename) {
    vector<string> lines;
    ifstream file(filename);
    if (!file.good()) { 
        return lines; 
    }
    string line;
    while (getline(file, line)) {
        lines.push_back(line);
    }
    file.close();
    return lines;
}

void write_file( string& filename,  vector<string>& content) {
    ofstream outfile(filename);
    if (!outfile.is_open()) {
        cerr << "Error: Could not open file for writing: " << filename << endl;
        return;
    }
    for ( auto& line : content) {
        outfile << line << endl;
    }
    outfile.close();
}

time_t get_file_mod_time( string& filename) {
    struct stat file_stat;
    if (stat(filename.c_str(), &file_stat) != 0) {
        return 0;
    }
    return file_stat.st_mtime;
}


void create_default_document_if_needed( string& filename) {
    ifstream f(filename.c_str());
    if (f.good()) { f.close(); return; }
    f.close();
    ofstream outfile(filename);
    outfile << "int x = 10" << endl;
    outfile << "int y = 20" << endl;
    outfile << "int z = 30" << endl;
    outfile.close();
    sleep(1);
}

//  Line-Based Diff Engine 
vector<DocOp> diff_lines( vector<string>& old_c, vector<string>& new_c, string& user_id) {
    vector<DocOp> ops;
    size_t old_len = old_c.size();
    size_t new_len = new_c.size();
    size_t min_len = min(old_len, new_len);
    long timestamp = get_timestamp();

    for (size_t i = 0; i < min_len; ++i) {
        if (old_c[i] != new_c[i]) {
            DocOp op;
            op.type = REPLACE;
            op.line_num = (int)i;
            op.timestamp = timestamp;
            strncpy(op.user_id, user_id.c_str(), MAX_ID_LEN);
            strncpy(op.new_content, new_c[i].c_str(), max_content);
            ops.push_back(op);
        }
    }
    if (new_len > old_len) {
        for (size_t i = old_len; i < new_len; ++i) {
            DocOp op;
            op.type = INSERT;
            op.line_num = (int)i;
            op.timestamp = timestamp;
            strncpy(op.user_id, user_id.c_str(), MAX_ID_LEN);
            strncpy(op.new_content, new_c[i].c_str(), max_content);
            ops.push_back(op);
        }
    }
    else if (old_len > new_len) {
        for (size_t i = new_len; i < old_len; ++i) {
            DocOp op;
            op.type = DELETE;
            op.line_num = (int)i;
            op.timestamp = timestamp;
            strncpy(op.user_id, user_id.c_str(), MAX_ID_LEN);
            ops.push_back(op);
        }
    }
    return ops;
}

//  Display Functions 
void print_op( DocOp& op) {
    switch (op.type) {
        case INSERT:
            cout << "Line " << op.line_num << " INSERTED: \"" << op.new_content << "\"" << endl;
            break;
        case DELETE:
            cout << "Line " << op.line_num << " DELETED" << endl;
            break;
        case REPLACE:
            cout << "Line " << op.line_num << " REPLACED: \"" << op.new_content << "\"" << endl;
            break;
    }
}

void redraw_display( vector<string>& content, string& user_id, string& filename) {
    clear_screen();
    cout << "Document: " << filename << " | User: " << user_id << endl;
    cout << "Last updated: " << get_time_str() << endl;
    cout << "----------------------------------------" << endl;
    
    for (size_t i = 0; i < content.size(); ++i) {
        cout << "Line " << i << ": " << content[i] << endl;
    }
    
    cout << "----------------------------------------" << endl;
    cout << "Active users: ";
    
    int count = 0;
    for (int i = 0; i < MAX_USERS; ++i) {
        if (registry->users[i].state.load(memory_order_acquire) == 2) { 
            if (count > 0) cout << ", ";
            cout << registry->users[i].user_id;
            count++;
        }
    }
    
    if (count == 0) cout << " (None)";
    cout << endl;
}

//  Message Passing Functions (System V) 
void* msg_listener_thread(void* arg) {
    MsgPkt msg;
    
    while (running) {
        if (msgrcv(my_mqid, &msg, sizeof(DocOp), 0, 0) < 0) {
            if (errno == EIDRM || !running) {
                break;
            }
            perror("msgrcv");
            break;
        }

        if (msg.mtype == msg_op_update) {
            while (remote_lock.test_and_set(memory_order_acquire)) {} 
            remote_ops.push_back(msg.op);
            remote_lock.clear(memory_order_release);
        }
        else if (msg.mtype == msg_hello_rq) {
            while (sync_lock.test_and_set(memory_order_acquire)) {} 
            sync_reqs.push_back(msg.op.user_id); 
            sync_lock.clear(memory_order_release);
        }
    }
    return nullptr;
}

void broadcast_ops( vector<DocOp>& ops) {
    vector<int> other_msqids;
    for (int i = 0; i < MAX_USERS; i++) {
        if (registry->users[i].state.load(memory_order_acquire) == 2 && 
            (strcmp(registry->users[i].user_id, username.c_str()) != 0)) {
            other_msqids.push_back(registry->users[i].msqid);
        }
    }

    for (int msqid : other_msqids) {
        if (msqid == -1) continue;
        for ( auto& op : ops) {
            MsgPkt msg;
            msg.mtype = msg_op_update; 
            msg.op = op;
            if (msgsnd(msqid, &msg, sizeof(DocOp), 0) == -1) {
                if (errno != ENOENT) {
                    perror("msgsnd");
                }
            }
        }
    }
}

void broadcast_hello_sync() {
    cout << "Broadcasting HELLO to sync with other users..." << endl;
    vector<int> other_msqids;
    for (int i = 0; i < MAX_USERS; i++) {
        if (registry->users[i].state.load(memory_order_acquire) == 2 && 
            (strcmp(registry->users[i].user_id, username.c_str()) != 0)) {
            other_msqids.push_back(registry->users[i].msqid);
        }
    }

    MsgPkt msg;
    msg.mtype = msg_hello_rq; 
    strncpy(msg.op.user_id, username.c_str(), MAX_ID_LEN); 

    for (int msqid : other_msqids) {
        if (msqid == -1) continue;
        if (msgsnd(msqid, &msg, sizeof(DocOp), 0) == -1) {
            perror("msgsnd (hello)");
        }
    }
}

void handle_sync_reqs() {
    while (sync_lock.test_and_set(memory_order_acquire)) {} // Spin
    if (sync_reqs.empty()) {
        sync_lock.clear(memory_order_release);
        return;
    }

    vector<string> requests = sync_reqs;
    sync_reqs.clear();
    sync_lock.clear(memory_order_release);

    for ( string& requester_id : requests) {
        cout << "\n Received sync request from " << requester_id << ". Sending full state. " << endl;
        
        int requester_msqid = -1;
        for (int i = 0; i < MAX_USERS; i++) {
            if (registry->users[i].state.load(memory_order_acquire) == 2 && 
                strcmp(registry->users[i].user_id, requester_id.c_str()) == 0) {
                requester_msqid = registry->users[i].msqid; 
                break;
            }
        }

        if (requester_msqid == -1) {
            cerr << "Warning: Could not find msqid for requester " << requester_id << endl;
            continue;
        }

        long sync_timestamp = get_timestamp();
        for (int i = 0; i < doc_lines.size(); i++) {
            MsgPkt msg;
            msg.mtype = msg_op_update;
            msg.op.type = REPLACE; 
            msg.op.line_num = i;
            strncpy(msg.op.new_content, doc_lines[i].c_str(), max_content);
            msg.op.timestamp = sync_timestamp;
            strncpy(msg.op.user_id, username.c_str(), MAX_ID_LEN);

            if (msgsnd(requester_msqid, &msg, sizeof(DocOp), 0) == -1) {
                perror("msgsnd (sync reply)");
            }
        }
    }
}


//  Part 3: CRDT Conflict Resolution (LWW) 

DocOp resolve_lww( DocOp& a,  DocOp& b) {
    // Last-Write-Wins: higher timestamp wins
    if (a.timestamp > b.timestamp) {
        return a;
    }
    if (b.timestamp > a.timestamp) {
        return b;
    }
    // Tie-break with user_id
    if (strcmp(a.user_id, b.user_id) < 0) {
        return a;
    }
    return b;
}

void patch_document(vector<string>& document,vector<DocOp>& final_ops) {
    sort(final_ops.begin(), final_ops.end(), 
        []( DocOp& a,  DocOp& b) {
            return a.line_num > b.line_num;
        });

    for ( auto& op : final_ops) {
        switch (op.type) {
            case REPLACE:
                while (document.size() <= op.line_num) {
                    document.push_back("");
                }
                document[op.line_num] = op.new_content;
                break;
            case INSERT:
                if (op.line_num <= document.size()) {
                    document.insert(document.begin() + op.line_num, op.new_content);
                } else {
                    document.push_back(op.new_content);
                }
                break;
            case DELETE:
                if (op.line_num < document.size()) {
                    document.erase(document.begin() + op.line_num);
                }
                break;
        }
    }
}

void merge_state() {
    while (remote_lock.test_and_set(memory_order_acquire)) {} 
    vector<DocOp> remote = remote_ops;
    remote_ops.clear();
    remote_lock.clear(memory_order_release);

    vector<DocOp> local_ops = pendinops;
    
    vector<DocOp> all_ops = local_ops;
    all_ops.insert(all_ops.end(), remote.begin(), remote.end());

    if (all_ops.empty()) {
        return;
    }

    cout << "\n Merging " << all_ops.size() << " total ops (" << local_ops.size() << " local, " << remote.size() << " remote) " << endl;

    map<int, vector<DocOp>> conflict_map;
    for ( auto& op : all_ops) {
        conflict_map[op.line_num].push_back(op);
    }

    vector<DocOp> final_ops;
    for (auto & [line_num, ops_list] : conflict_map) {
        if (ops_list.size() == 1) {
            final_ops.push_back(ops_list[0]);
        } else {
            DocOp winner = ops_list[0];
            for (size_t i = 1; i < ops_list.size(); ++i) {
                winner = resolve_lww(winner, ops_list[i]);
            }
            final_ops.push_back(winner);
            cout << "  Conflict on line " << line_num << " resolved. Winner: " << winner.user_id << endl;
        }
    }

    patch_document(doc_lines, final_ops);

    write_file(filename, doc_lines);

    redraw_display(doc_lines, username, filename);
    cout << "Monitoring for changes... (" << pendinops.size() << "/" << max_op_buffer << ")" << endl;

    if (pendinops.size() >= max_op_buffer) {
        cout << "\nBroadcasting " << pendinops.size() << " local changes..." << endl;
        broadcast_ops(pendinops);
        pendinops.clear();
    }
}
//  IPC Initialization and Cleanup 
void cleanup_mq() {
    if (my_mqid != -1) {
        if (msgctl(my_mqid, IPC_RMID, nullptr) == -1) {
            if (errno != EIDRM && errno != EINVAL) {
                perror("msgctl(IPC_RMID)");
            }
        }
        my_mqid = -1;
    }
}
void init_registry() {
    key_t key = ftok(reg_path, reg_keyID);
    if (key == -1) {
        perror("ftok failed");
        exit(EXIT_FAILURE);
    }

    bool created = false;
    shm_id = shmget(key, sizeof(Registry), IPC_CREAT | IPC_EXCL | 0666);
    
    if (shm_id == -1) {
        if (errno == EEXIST) {
            shm_id = shmget(key, sizeof(Registry), 0666);
            if (shm_id == -1) {
                perror("shmget (existing) failed");
                exit(EXIT_FAILURE);
            }
        } else {
            perror("shmget (create) failed");
            exit(EXIT_FAILURE);
        }
    } else {
        created = true;
    }

    registry = (Registry*)shmat(shm_id, nullptr, 0);
    if (registry == (void*)-1) {
        perror("shmat failed");
        exit(EXIT_FAILURE);
    }

    if (created) {
        cout << "First user. Initializing shared memory (lock-free)..." << endl;
        new (&registry->active_user_count) atomic<int>(0);
        for (int i = 0; i < MAX_USERS; ++i) {
            new (&registry->users[i].state) atomic<int>(0);
            registry->users[i].msqid = -1;
            memset(registry->users[i].user_id, 0, MAX_ID_LEN);
        }
    }
}


int join_registry_mq( string& user_id) {
    int current_user_count = registry->active_user_count.load(memory_order_relaxed);

    if (current_user_count >= MAX_USERS) {
        cerr << "Error: Maximum number of users (" << MAX_USERS << ") reached." << endl;
        exit(EXIT_FAILURE);
    }

    int slot = -1;
    for (int i = 0; i < MAX_USERS; ++i) {
        int expected_empty = 0;
        if (registry->users[i].state.compare_exchange_strong(expected_empty, 1)) 
        {
            slot = i;
            break;
        }
    }

    if (slot == -1) {
        cerr << "Error: Registry is full (could not find slot)." << endl;
        exit(EXIT_FAILURE);
    }

    my_shm_slot = slot; 
    my_mqkey = ftok(reg_path, MQ_keyID_base + slot);
    if (my_mqkey == -1) {
        perror("ftok for mqueue failed");
        registry->users[slot].state.store(0, memory_order_release); 
        exit(EXIT_FAILURE);
    }
    int old_msqid = msgget(my_mqkey, 0666);
    if (old_msqid != -1) {
        msgctl(old_msqid, IPC_RMID, nullptr);
    }

    my_mqid = msgget(my_mqkey, IPC_CREAT | 0666);
    if (my_mqid == -1) {
        perror("msgget failed");
        registry->users[slot].state.store(0, memory_order_release);
        exit(EXIT_FAILURE);
    }
    strncpy(registry->users[slot].user_id, user_id.c_str(), MAX_ID_LEN);
    registry->users[slot].msqid = my_mqid;
    
    registry->users[slot].state.store(2, memory_order_release); 
    
    registry->active_user_count.fetch_add(1, memory_order_relaxed);
    
    return current_user_count;
}

void leave_registry() {
    if (registry == nullptr || username.empty() || my_shm_slot == -1) {
        return;
    }
    registry->users[my_shm_slot].state.store(0, memory_order_release); 
    registry->users[my_shm_slot].msqid = -1;
    
    int remaining_users = registry->active_user_count.fetch_sub(1, memory_order_relaxed) - 1;

    shmdt(registry);

    cleanup_mq();

    if (remaining_users == 0) {
        cout << "\nLast user exiting. Cleaning up shared memory." << endl;
        shmctl(shm_id, IPC_RMID, nullptr);
    }
}

void sig_handler(int signum) {
    if (signum == SIGINT) {
        if (running) {
            cout << "\nCtrl+C detected. Shutting down gracefully..." << endl;
            running = false;
            cleanup_mq();
        }
    }
}

//  Main Function 

int main(int argc, char* argv[]) {
    if (argc != 2) {
        cerr << "Usage: ./editor <user_id>" << endl;
        return 1;
    }

    username = argv[1];
    filename = username + "_doc.txt";

    atexit(leave_registry);
    signal(SIGINT, sig_handler);

    init_registry(); 

    int old_user_count = join_registry_mq(username); 

    if(pthread_create(&listener_thread, nullptr, msg_listener_thread, nullptr) != 0) {
        cerr << "Error creating listener thread." << endl;
        return 1;
    }

    if (old_user_count == 0) {
        cout << "First user. Initializing new document." << endl;
        create_default_document_if_needed(filename); 
    } else {
        cout << "Joining existing session. Requesting sync..." << endl;
    }
    
    doc_lines = read_file(filename); 
    time_t last_mod_time = get_file_mod_time(filename);

    redraw_display(doc_lines, username, filename);
    cout << "Monitoring for changes... (Broadcasts after " << max_op_buffer << " ops)" << endl;

    if (old_user_count > 0) {
        broadcast_hello_sync();
    }

    while (running) {
        // Handle sync requests from new users
        handle_sync_reqs();
        while (remote_lock.test_and_set(memory_order_acquire)) {} 
        bool remote_changes_pending = !remote_ops.empty();
        remote_lock.clear(memory_order_release);
        
        bool local_buffer_full = pendinops.size() >= max_op_buffer;

        if (remote_changes_pending || local_buffer_full) {
            merge_state();
            last_mod_time = get_file_mod_time(filename);
        }

        sleep(poll_sec);
        if (!running) break;

        // Check for local file modifications
        time_t new_mod_time = get_file_mod_time(filename);

        if (new_mod_time > last_mod_time) {
            vector<string> new_content = read_file(filename);
            
            vector<DocOp> ops = diff_lines(doc_lines, new_content, username);
            
            last_mod_time = new_mod_time;

            if (!ops.empty()) {
                cout << "\nDetected " << ops.size() << " local changes:" << endl;
                for ( auto& op : ops) {
                    print_op(op);
                    pendinops.push_back(op);
                }
                
                cout << "Monitoring for changes... (" << pendinops.size() << "/" << max_op_buffer << ")" << endl;
                // If this push filled the buffer, loop again immediately to merge/broadcast
                if (pendinops.size() >= max_op_buffer) {
                    continue; 
                }
            }
        }
    }

    cout << "Waiting for listener thread to exit..." << endl;
    pthread_join(listener_thread, nullptr);
    
    cout << "Main thread exiting." << endl;
    return 0;
}