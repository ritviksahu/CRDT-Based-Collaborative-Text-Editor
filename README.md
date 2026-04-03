# C++ Collaborative CRDT Editor

This project is a real-time collaborative text editor, similar to Google Docs, implemented in C++. It uses a CRDT (Conflict-Free Replicated Data Type) approach, specifically Last-Write-Wins (LWW), to ensure eventual consistency and resolve conflicts without a central server.

The backend relies on System V IPC for inter-process communication:

- **Shared Memory** is used for a lock-free Registry to manage user discovery.

- **Message Queues** are used to broadcast edit operations (DocOp) and sync requests.

The synchronization is built to be lock-free, using std::atomic and compare-and-swap operations for the shared user registry, and std::atomic_flag spinlocks for intra-process thread safety.

## Platform & Dependencies

### Platform

* **OS:** macOS or any modern Linux.
* **Compiler:** g++ ( C++17 or later)

### Dependencies

The program requires g++ and the pthread library.

- pthread: Required for the background message listener thread.

- std=c++17: C++17 is necessary for std::atomic, std::atomic_flag, and structured bindings.

---

## Compilation
1. Clean your IPC resources (if you had any crashes):
```Bash
ipcs -m | grep $USER | awk '{print $2}' | xargs -n 1 ipcrm -m
ipcs -q | grep $USER | awk '{print $2}' | xargs -n 1 ipcrm -q
```

2.  Save the code as `25CS60R19_CRDT.cpp`.
3.  Open your terminal and navigate to the directory.
4.  Run the following command to compile:

```bash
g++ 25CS60R19_CRDT.cpp -o editor -lpthread -std=c++17
```

## Execution

To start the editor, each user must run the program in a separate terminal with a unique user ID.

Terminal 1:

```Bash
./editor user_1
```

Terminal 2:
```Bash
./editor user_2
```
Terminal 3:
```Bash
./editor user_3
```
The program will automatically create a local document for each user (e.g., user_1_doc.txt, user_2_doc.txt).

##  How to Test

This program works by monitoring the user's local document file for changes.

- **Start Users**: Launch two or more users (e.g., user_1 and user_2) in separate terminals as shown above.

- **Initial Sync**: The first user (user_1) will create a default document. The second user (user_2) will automatically send a sync request and display user_1's document content.

- **Make Local Edits**: Open one of the user's document files (e.g., user_1_doc.txt) in a separate text editor (like nano).

- **Add/Modify Lines**: Add a new line or change an existing line and save the file.

- **Observe Sync**:

    - The user_1 terminal will detect the change and show the new ops.

    - After 5 local changes  it will broadcast its updates.

    - The user_2 terminal will then automatically receive the ops, merge them, and refresh its display with the new content.

- **Test Conflicts (LWW)**:

    - Quickly edit the same line in both user_1_doc.txt and user_2_doc.txt using two different text editors.

    -  Save both files before the 5-op buffer is full.

    - When the sync eventually happens, you will see a "Conflict on line X resolved" message in the terminals. The version that wins will be the one with the later save timestamp, demonstrating the Last-Write-Wins logic.