#ifndef TASK_H
#define TASK_H

#include <sys/queue.h>
#include <linux/limits.h>

#include "utils_ds/state_machine.h"
#include "worker_context_fwd.h"
#include "shared_file_struct.h"
#include "intermediate_file_sturct.h"
#include "utils_ds/deque.h"
#include "msg_pass/mp_server.h"

typedef enum Task_state_t {
    TASK_ST_ASSIGNED,
    TASK_ST_RUNNING,
    TASK_ST_SUCCEEDED,
    TASK_ST_FAILED
} Task_state_t;

typedef enum Task_type {
    MAP_TASK,
    REDUCE_TASK
} Task_type;

typedef struct Task {
    State_machine state_machine;
    Task_type type;
    // int tid;
    char tid[NAME_MAX];
    // int job_id;
    // const char* job_id;
    char job_id[NAME_MAX];
    char job_name[NAME_MAX];
    Worker_context* wctx;
    pid_t pid;

    Mp_srv_request* launch_req;
    // pthread_mutex_t lock;
    TAILQ_ENTRY(Task) task_link;
} Task;

typedef TAILQ_HEAD(Task_list, Task) Task_list;

typedef struct Map_task {
    Task t;
    // Input_split* split;
    char inputsplit_path[PATH_MAX];
    int n_partitions;

    Shared_file* cur_allocated_shf;

    Shared_file_list mapoutputs;
} Map_task;

typedef struct Reduce_task {
    Task t;

    char outputfile_path[PATH_MAX];
    int partition_number;

    Deque* intermediate_file_Q;
    Mp_srv_request* intf_req;
    // bool fwd_intf_done;
    bool start_reduce_phase;
} Reduce_task;

////////////////////////// Task event ////////////////////////////////////

typedef enum Tev_type {
    // TEV_ASSIGN,
    TEV_LAUNCH,
    TEV_LAUNCHED,
    TEV_GET_SHF,
    TEV_MAPOUTPUT,
    TEV_GET_INTF,
    TEV_FWD_INTF,
    TEV_START_RD_PHASE,
    TEV_SUCCEEDED,
    TEV_FAILED,
    TEV_ABORT
    // TEV_CLEANUP
} Tev_type;

typedef struct Task_event {
    Tev_type type;
    Task* task;
} Task_event;

typedef struct Task_launch_event {
    Task_event tev;
    Mp_srv_request* request;

} Task_launch_event;

typedef struct Task_launched_event {
    Task_event tev;
} Task_launched_event;

typedef struct Task_get_shf_event {
    Task_event tev;
    Mp_srv_request* request;
} Task_get_shf_event;

typedef struct Task_mapoutput_event {
    Task_event tev;
    Shared_file* sf;
    int sf_has_data;
} Task_mapoutput_event;

typedef struct Task_get_intf_event {
    Task_event tev;
    Mp_srv_request* request;
} Task_get_intf_event;

typedef struct Task_fwd_intf_event {
    Task_event tev;
    // Mp_srv_request* request;
    Intermediate_file* intf;
} Task_fwd_intf_event;

typedef struct Task_start_reduce_phase_event {
    Task_event tev;
} Task_start_reduce_phase_event;

typedef struct Task_succeeded_event {
    Task_event tev;
} Task_succeeded_event;

typedef struct Task_failed_event {
    Task_event tev;
} Task_failed_event;

typedef struct Task_abort_event {
    Task_event tev;
} Task_abort_event;

typedef struct Task_cleanup_event {
    Task_event tev;
    Mp_srv_request* request;
} Task_cleanup_event;


////////////////////////// Task event handler ////////////////////////////////////

typedef struct Task_ev_handler Task_ev_handler;


Map_task* map_task_create(Worker_context* wctx, const char* tid, const char* job_id, 
                const char* job_name, const char* input_split_path, int n_partition);
int map_task_init(Map_task* mt, Worker_context* wctx, const char* tid, 
    const char* job_id, const char* job_name,const char* input_split_path, int n_partitions);
void map_task_destroy(Map_task* mt);
Reduce_task* reduce_task_create(Worker_context* wctx, const char* tid, 
    const char* job_id, const char* job_name, const char* outputfile_path, int partition_number);
int reduce_task_init(Reduce_task* rt, Worker_context* wctx, const char* tid, 
    const char* job_id, const char* job_name, const char* outputfile_path, int partition_number);
void reduce_task_destroy(Reduce_task* rt);

// Task_launch_event* task_launch_event_create(Task* t, const char* job_name, 
//     const char* job_id, const char* task_id, const char* inputsplit_path, 
//                                                     int num_partitions);
Task_launch_event* task_launch_event_create(Task* t, Mp_srv_request* request);  
Task_launched_event* task_launched_event_create(Task* t);
Task_get_shf_event* task_get_shf_event_create(Task* t, Mp_srv_request* request);
Task_mapoutput_event* task_mapoutput_event_create(Task* t, Shared_file* sf, int has_data);
Task_get_intf_event* task_get_intf_event_create(Task* t, Mp_srv_request* request);
Task_fwd_intf_event* task_fwd_intf_event_create(Task* t, Intermediate_file* intf);
// Task_fwd_intf_event* task_fwd_intf_done_event_create(Task* t);
Task_start_reduce_phase_event* task_start_reduce_phase_event_create(Task* t);
Task_succeeded_event* task_succeeded_event_create(Task* t);
Task_failed_event* task_failed_event_create(Task* t);
Task_abort_event* task_abort_event_create(Task* t);
// Task_cleanup_event* task_cleanup_event_create(Task* t, Mp_srv_request* request);
void task_event_destroy(Task_event* tev);
const char* task_event_to_string(Task_event* tev);
const char* task_state_to_string(Task_state_t t_state);

Task_ev_handler* task_ev_handler_create();
void task_ev_handler_destroy(Task_ev_handler* teh);
void task_ev_handler_handle(Task_ev_handler* teh, Task_event* tev);

#endif