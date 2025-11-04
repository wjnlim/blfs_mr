#ifndef MASTER_H
#define MASTER_H

#include "job.h"
#include "msg_pass/mp_client.h"
// #include "worker_conn_fwd.h"
#include "master_fwd.h"
// typedef struct Worker_conn Worker_conn;
// typedef struct Master_context Master_context;
typedef struct Worker_conn Worker_conn;
typedef TAILQ_HEAD(Worker_conn_list, Worker_conn) Worker_conn_list;

// void master_ctx_worker_incr_assigned_task(Master_context* mctx, Worker_conn* wc);
Worker_conn* master_ctx_get_min_assigned_task_worker(Master_context* mctx);
Worker_conn* master_ctx_get_worker_conn(Master_context* mctx, const char* name);
void master_ctx_print_assigned_tasks(Master_context* mctx);
Worker_conn_list* master_ctx_get_worker_list(Master_context* mctx);

Worker_conn* worker_conn_first(Worker_conn_list* wcl);
Worker_conn* worker_conn_next(Worker_conn* wc);

void worker_conn_assign_task(Worker_conn* wc, Task_info* ti);
Mp_client* worker_conn_get_client(Worker_conn* wc);
const char* worker_conn_get_name(Worker_conn* wc);

#endif
