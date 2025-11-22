#ifndef JOB_H
#define JOB_H

#include <linux/limits.h>
#include <sys/queue.h>


#include "master_fwd.h"
#include "utils/state_machine.h"
#include "utils_ds/hash_table.h"
#include "utils_ds/deque.h"
#include "msg_pass/mp_server.h"
#include "intermediate_file_sturct.h"

typedef struct Job Job;

typedef enum Job_state_t {
    // JOB_ST_IDLE,
    JOB_ST_INITED,
    JOB_ST_RUNNING,
    JOB_ST_DONE
    // JOB_ST_SUCCEEDED,
    // JOB_ST_FAILED
} Job_state_t;


typedef struct Input_split {
    char file_path[PATH_MAX];
    Worker_conn* location;

    TAILQ_ENTRY(Input_split) split_link;
} Input_split;
typedef TAILQ_HEAD(Input_split_list, Input_split) Input_split_list;

typedef enum Task_info_type {
    MAP_TASK_INFO,
    REDUCE_TASK_INFO
} Task_info_type;

typedef struct Task_info {
    Task_info_type type;
    char tid[NAME_MAX];
    Job* job;
    bool done;
    bool failed;
} Task_info;

typedef struct Map_task_info {
    Task_info ti;
    // Map_task* mt;
    Input_split* isplit;
    Deque* mapoutputs;
    // Interm_file_list intermediate_file_infos;

    TAILQ_ENTRY(Map_task_info) mti_link;
} Map_task_info;
typedef TAILQ_HEAD(Mt_info_list, Map_task_info) Mt_info_list;


typedef struct Reduce_task_info {
    Task_info ti;
    // Reduce_task* rt;
    char outputfile_name[NAME_MAX];
    char outputfile_path[PATH_MAX];
    int partition_number;
    Worker_conn* location;
    // Deque* intermediate_file_list;
    TAILQ_ENTRY(Reduce_task_info) rti_link;
} Reduce_task_info;
typedef TAILQ_HEAD(Rt_info_list, Reduce_task_info) Rt_info_list;

typedef struct Job_ev_handler Job_ev_handler;

/*
    A job is state machine that receive job events and do job event actions.
*/
struct Job {
    State_machine state_machine;
    char name[NAME_MAX];
    // int id;
    char id[NAME_MAX];
    char input_metadata_path[PATH_MAX];
    char output_metadata_path[PATH_MAX];

    char job_output_dir[PATH_MAX];

    int n_input_splits;
    Input_split_list input_splits;

    char next_tid[NAME_MAX];
    int next_tid_num;
    // int n_total_tasks;
    int n_map_tasks;
    int n_reduce_tasks;
    Mt_info_list map_task_infos;
    Deque* succeeded_map_task_infos;
    Deque* failed_map_task_infos;

    Rt_info_list reduce_task_infos;
    Deque* succeeded_reduce_task_infos;
    Deque* failed_reduce_task_infos;

    Hash_table* task_id_map;

    int n_workers;
    int n_job_inited_workers;

    int n_launched_map_task;
    int n_launched_reduce_task;
    int n_succeeded_map_task;
    int n_failed_map_task;
    int n_succeeded_reduce_task;
    int n_failed_reduce_task;
    int n_cleaned_up_task;
    
    bool aborted;

    Job_ev_handler* jev_handler;

    bool done;
    bool failed;
    Master_context* mctx;
    pthread_mutex_t lock;
    pthread_cond_t cv;
};

////////////////// Job event /////////////////////////////
typedef enum Jev_type {
    JEV_START,
    JEV_TASK_LNCHD,
    JEV_MAPOUTPUT,
    JEV_TASK_SUCCEEDED,
    JEV_TASK_FAILED,
    JEV_DONE,
    JEV_TASK_CLEANED_UP
} Jev_type;

typedef struct Job_event {
    Jev_type type;
    // int job_id;
    Job* job;
} Job_event;

typedef struct Job_start_event {
    Job_event jev;
} Job_start_event;

typedef struct Job_task_lnchd_event {
    Job_event jev;
    Task_info* ti;
} Job_task_lnchd_event;

typedef struct Job_mapoutput_event {
    Job_event jev;
    // char tid[NAME_MAX];
    Task_info* ti;
    Intermediate_file* intf;
} Job_mapoutput_event;

typedef struct Job_task_succeeded_event {
    Job_event jev;
    // char tid[NAME_MAX];
    Task_info* ti;
    // int t_failed;
} Job_task_succeeded_event;

typedef struct Job_task_failed_event {
    Job_event jev;
    // char tid[NAME_MAX];
    Task_info* ti;
    // int t_failed;
} Job_task_failed_event;

typedef struct Job_done_event {
    Job_event jev;
    // char tid[NAME_MAX];
    bool failed;
} Job_done_event;

typedef struct Job_task_cleaned_up_event {
    Job_event jev;
    // char tid[NAME_MAX];
    Task_info* ti;
} Job_task_cleaned_up_event;


///////////////////////////////////////////////////////////////////////////

void job_init(Job* job, const char* name, const char* input_metadata_file, 
    const char* output_metadata_file, Job_ev_handler* jev_handler, 
    int n_workers, Master_context* mctx);

Job_start_event* job_start_event_create(Job* job);
Job_task_lnchd_event* job_task_lnchd_event_create(Job* job, Task_info* ti);
Job_mapoutput_event* job_mapoutput_event_create(Job* job, Task_info* ti,
                                                    Intermediate_file* intf);
Job_task_succeeded_event* job_task_succeeded_event_create(Job* job, Task_info* ti);
Job_task_failed_event* job_task_failed_event_create(Job* job, Task_info* ti);
Job_done_event* job_done_event_create(Job* job, bool failed);
Job_task_cleaned_up_event* job_task_cleaned_up_event_create(Job* job, Task_info* ti);
void job_event_destroy(Job_event* jev);
const char* job_event_to_string(Job_event* jev);
const char* job_state_to_string(Job_state_t j_state);

Job_ev_handler* job_ev_handler_create();
void job_ev_handler_destroy(Job_ev_handler* jeh);
void job_ev_handler_handle(Job_ev_handler* jeh, Job_event* jev);

Task_info* job_get_task(Job* job, const char* tid);

#endif