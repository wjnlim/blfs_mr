#ifndef INTERMEDITATE_FILE_STRUCT_H
#define INTERMEDITATE_FILE_STRUCT_H

// #include <sys/queue.h>
#include <linux/limits.h>
#include <netinet/in.h>

typedef struct Intermediate_file {
    // char file_path[PATH_MAX];
    char file_name[NAME_MAX];
    // int size;
    // Worker_conn* location;
    // char location_ip[INET_ADDRSTRLEN];
    
    // location
    char worker_name[NAME_MAX];
    int metadata_size;

    // TAILQ_ENTRY(Intermediate_file) if_link;
} Intermediate_file;
// typedef TAILQ_HEAD(Intermediate_file_Q, Intermediate_file) Intermediate_file_Q;

#endif