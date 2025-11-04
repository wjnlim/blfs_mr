#ifndef SHARED_FILE_STRUCT_H
#define SHARED_FILE_STRUCT_H

#include <linux/limits.h>
#include <sys/queue.h>

typedef struct Shared_file {
    // char path[PATH_MAX];
    char name[NAME_MAX];
    int capacity;
    int metadata_size;

    TAILQ_ENTRY(Shared_file) sf_link;
} Shared_file;

typedef TAILQ_HEAD(Shared_file_list, Shared_file) Shared_file_list;

#endif