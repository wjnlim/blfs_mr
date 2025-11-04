#ifndef WORKER_H
#define WORKER_H

// #include <unistd.h>
#include "worker_context_fwd.h"
#include "task.h"
#include "shared_file_struct.h"
#include "msg_pass/mp_client.h"

void worker_add_launched_task(Worker_context* wctx, pid_t pid, Task* task);
void worker_add_task_conn(Worker_context* wctx, int task_sockfd);
const char* worker_get_name(Worker_context* wctx);
const char* worker_get_mr_exe_dir(Worker_context* wctx);
const char* worker_get_shared_dir(Worker_context* wctx);
const char* worker_get_ip(Worker_context* wctx);
const char* worker_get_shared_dev_file(Worker_context* wctx);
// const char* worker_get_output_dir(Worker_context* wctx);
int worker_get_shared_file_size(Worker_context* wctx);
Shared_file* worker_get_shared_file(Worker_context* wctx);
void worker_release_shared_file(Worker_context* wctx, Shared_file* sf);
Mp_client* worker_get_master_conn(Worker_context* wctx);

#endif