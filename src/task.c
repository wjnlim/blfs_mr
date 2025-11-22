#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>

#include "task.h"
#include "utils/my_err.h"
#include "utils_ds/blocking_queue.h"
#include "worker.h"
#include "utils/state_machine.h"
#include "shared_file_struct.h"
#include "msg_pass/mp_buf_sizes.h"
#include "MR_msg_type.h"
#include "msg_pass/mp_client.h"

static sm_action_result_t map_task_launch_action(State_machine* sm, void* event_data);
static sm_action_result_t map_task_launched_action(State_machine* sm, void* event_data);
static sm_action_result_t get_shf_action(State_machine* sm, void* event_data);
static sm_action_result_t mapoutput_action(State_machine* sm, void* event_data);
static sm_action_result_t task_succeeded_action(State_machine* sm, void* event_data);
static sm_action_result_t task_failed_action(State_machine* sm, void* event_data);
static sm_action_result_t task_abort_action(State_machine* sm, void* event_data);


static sm_action_result_t reduce_task_launch_action(State_machine* sm, void* event_data);
static sm_action_result_t reduce_task_launched_action(State_machine* sm, void* event_data);
static sm_action_result_t get_intf_action(State_machine* sm, void* event_data);
static sm_action_result_t fwd_intf_action(State_machine* sm, void* event_data);
static sm_action_result_t start_reduce_phase_action(State_machine* sm, void* event_data);

// static sm_action_result_t event_ignore_action(State_machine* sm, void* event_data);
// static sm_action_result_t event_err_action(State_machine* sm, void* event_data);
/*
    Transition table for Map task
*/
// transition table at state ASSIGNED
DEFINE_TRANSITION_TABLE_PER_STATE(table_for_mt_ASSIGNED, 
    TABLE_ELEMENT(TASK_ST_ASSIGNED, map_task_launch_action), // for LAUNCH event
    TABLE_ELEMENT(TASK_ST_RUNNING, map_task_launched_action),         // for LAUNCHED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_SHF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for MAPOUTPUT event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for FWD_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for START_RD_PHASE event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for SUCCEEDED event
    TABLE_ELEMENT(TASK_ST_FAILED, task_failed_action),         // for FAILED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL)         // for ABORT event
    // TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL)         // for CLEANUP event
);
// transition table at state RUNNING
DEFINE_TRANSITION_TABLE_PER_STATE(table_for_mt_RUNNING, 
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL), // for LAUNCH event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for LAUNCHED event
    TABLE_ELEMENT(TASK_ST_RUNNING, get_shf_action),         // for GET_SHF event
    TABLE_ELEMENT(TASK_ST_RUNNING, mapoutput_action),         // for MAPOUTPUT event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for FWD_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for START_RD_PHASE event
    TABLE_ELEMENT(TASK_ST_SUCCEEDED, task_succeeded_action),         // for SUCCEEDED event
    TABLE_ELEMENT(TASK_ST_FAILED, task_failed_action),         // for FAILED event
    TABLE_ELEMENT(TASK_ST_FAILED, task_abort_action)         // for ABORT event
    // TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL)         // for CLEANUP event
);
// transition table at state SUCCEEDED
DEFINE_TRANSITION_TABLE_PER_STATE(table_for_mt_SUCCEEDED, 
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL), // for LAUNCH event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for LAUNCHED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_SHF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for MAPOUTPUT event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for FWD_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for START_RD_PHASE event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for SUCCEEDED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for FAILED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL)         // for ABORT event
    // TABLE_ELEMENT(TASK_ST_SUCCEEDED, task_cleanup_action)         // for CLEANUP event
);
// transition table at state FAILED
DEFINE_TRANSITION_TABLE_PER_STATE(table_for_mt_FAILED, 
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL), // for LAUNCH event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for LAUNCHED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_SHF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for MAPOUTPUT event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for FWD_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for START_RD_PHASE event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for SUCCEEDED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for FAILED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL)         // for ABORT event
    // TABLE_ELEMENT(TASK_ST_FAILED, task_cleanup_action)         // for CLEANUP event
);

DEFINE_COMBINED_TRANSITION_TABLE(transition_table_for_mt,
    table_for_mt_ASSIGNED, 
    table_for_mt_RUNNING, 
    table_for_mt_SUCCEEDED,
    table_for_mt_FAILED);

/*
    Transition table for Reduce task
*/
// transition table at state ASSIGNED
DEFINE_TRANSITION_TABLE_PER_STATE(table_for_rt_ASSIGNED, 
    TABLE_ELEMENT(TASK_ST_ASSIGNED, reduce_task_launch_action), // for LAUNCH event
    TABLE_ELEMENT(TASK_ST_RUNNING, reduce_task_launched_action),         // for LAUNCHED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_SHF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for MAPOUTPUT event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_INTF event
    TABLE_ELEMENT(TASK_ST_ASSIGNED, fwd_intf_action),         // for FWD_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for START_RD_PHASE event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for SUCCEEDED event
    TABLE_ELEMENT(TASK_ST_FAILED, task_failed_action),        // for FAILED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL)         // for ABORT event
    // TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL)         // for CLEANUP event
);
// transition table at state RUNNING
DEFINE_TRANSITION_TABLE_PER_STATE(table_for_rt_RUNNING, 
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL), // for LAUNCH event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for LAUNCHED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_SHF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for MAPOUTPUT event
    TABLE_ELEMENT(TASK_ST_RUNNING, get_intf_action),         // for GET_INTF event
    TABLE_ELEMENT(TASK_ST_RUNNING, fwd_intf_action),         // for FWD_INTF event
    TABLE_ELEMENT(TASK_ST_RUNNING, start_reduce_phase_action),         // for START_RD_PHASE event
    TABLE_ELEMENT(TASK_ST_SUCCEEDED, task_succeeded_action),         // for SUCCEEDED event
    TABLE_ELEMENT(TASK_ST_FAILED, task_failed_action),         // for FAILED event
    TABLE_ELEMENT(TASK_ST_FAILED, task_abort_action)         // for ABORT event
    // TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL)         // for CLEANUP event
);
// transition table at state SUCCEEDED
DEFINE_TRANSITION_TABLE_PER_STATE(table_for_rt_SUCCEEDED, 
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL), // for LAUNCH event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for LAUNCHED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_SHF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for MAPOUTPUT event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for FWD_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for START_RD_PHASE event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for SUCCEEDED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for FAILED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL)         // for ABORT event
    // TABLE_ELEMENT(TASK_ST_SUCCEEDED, task_cleanup_action)         // for CLEANUP event
);
// transition table at state FAILED
DEFINE_TRANSITION_TABLE_PER_STATE(table_for_rt_FAILED, 
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL), // for LAUNCH event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for LAUNCHED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_SHF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for MAPOUTPUT event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for GET_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for FWD_INTF event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for START_RD_PHASE event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for SUCCEEDED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for FAILED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL)         // for ABORT event
    // TABLE_ELEMENT(TASK_ST_FAILED, task_cleanup_action)         // for CLEANUP event
);
DEFINE_COMBINED_TRANSITION_TABLE(transition_table_for_rt,
    table_for_rt_ASSIGNED, 
    table_for_rt_RUNNING, 
    table_for_rt_SUCCEEDED,
    table_for_rt_FAILED);

static void task_init(Task* t, Task_type type, const char* tid, const char* job_id, 
    const char* job_name, const Table_element * const transition_table[], Worker_context* wctx) {
    sm_init(&t->state_machine, TASK_ST_ASSIGNED, transition_table);
    t->type = type;
    sprintf(t->tid, "%s", tid);
    sprintf(t->job_id, "%s", job_id);
    sprintf(t->job_name, "%s", job_name);
    t->wctx = wctx;
    t->pid = -1;
    t->launch_req = NULL;
}

int map_task_init(Map_task* mt, Worker_context* wctx, const char* tid, 
    const char* job_id, const char* job_name, const char* inputsplit_path, int n_partitions) {

    task_init(&mt->t, MAP_TASK, tid, job_id, job_name, transition_table_for_mt, wctx);

    sprintf(mt->inputsplit_path, "%s", inputsplit_path);
    mt->n_partitions = n_partitions;

    mt->cur_allocated_shf = NULL;

    TAILQ_INIT(&mt->mapoutputs);
    // return mt;

    return 0;
}

Map_task* map_task_create(Worker_context* wctx, const char* tid, const char* job_id, 
            const char* job_name, const char* inputsplit_path, int n_partitions) {

    Map_task* mt = (Map_task*)malloc(sizeof(*mt));
    if (mt == NULL) {
        err_msg_sys("[task.c: map_task_create()] malloc() error");
        return NULL;
    }

    map_task_init(mt, wctx, tid, job_id, job_name, inputsplit_path, n_partitions);
    return mt;
}

void map_task_destroy(Map_task* mt) {
    assert(mt != NULL);
    Shared_file* sf;
    while ((sf = TAILQ_FIRST(&mt->mapoutputs)) != NULL) {
        TAILQ_REMOVE(&mt->mapoutputs, sf, sf_link);
        worker_release_shared_file(mt->t.wctx, sf);
    }

    free(mt);
}

int reduce_task_init(Reduce_task* rt, Worker_context* wctx, const char* tid, 
    const char* job_id, const char* job_name, const char* outputfile_path, int partition_number) {

    task_init(&rt->t, REDUCE_TASK, tid, job_id, job_name, transition_table_for_rt, wctx);

    sprintf(rt->outputfile_path, "%s", outputfile_path);
    rt->partition_number = partition_number;

    rt->intermediate_file_Q = deque_create();
    if (rt->intermediate_file_Q == NULL) {
        err_msg("[task.c: reduce_task_init()] deque_create() error");
        return -1;
    }
    rt->intf_req = NULL;
    // rt->fwd_intf_done = false;
    rt->start_reduce_phase = false;

    return 0;
}

Reduce_task* reduce_task_create(Worker_context* wctx, const char* tid, 
    const char* job_id, const char* job_name, const char* outputfile_path, int partition_number) {
                                    
    Reduce_task* rt = (Reduce_task*)malloc(sizeof(*rt));
    if (rt == NULL) {
        err_msg_sys("[task.c: reduce_task_create()] malloc() error");
        return NULL;
    }

    if (reduce_task_init(rt, wctx, tid, job_id, job_name, outputfile_path, partition_number) < 0) {
        err_msg("[task.c: reduce_task_create()] reduce_task_init() error");
        free(rt);
        return NULL;
    }

    return rt;
}

void reduce_task_destroy(Reduce_task* rt) {
    assert(rt != NULL);

    Intermediate_file* intf; 
    while (!deque_empty(rt->intermediate_file_Q)) {
        intf = deque_front(rt->intermediate_file_Q)->elm;
        deque_pop_front(rt->intermediate_file_Q);

        free(intf);
    }

    deque_destroy(rt->intermediate_file_Q);
    free(rt);
}

struct Task_ev_handler {
    Blocking_Q* ev_queue;
    pthread_t worker_thread;

    Task_event shutdown_handler_ev;
};

// Task_launch_event* task_launch_event_create(Task* t, const char* job_name, 
//     const char* job_id, const char* task_id, const char* inputsplit_path, 
//     
Task_launch_event* task_launch_event_create(Task* t, Mp_srv_request* request) {
    Task_launch_event* tle = (Task_launch_event*)malloc(sizeof(*tle));
    if (tle == NULL) {
        err_msg_sys("[task.c: task_launch_event_create()] malloc() error");
        return NULL;
    }

    tle->tev.type = TEV_LAUNCH;
    tle->tev.task = t;

    tle->request = request;

    return tle;
}

Task_launched_event* task_launched_event_create(Task* t) {
    Task_launched_event* tlde = (Task_launched_event*)malloc(sizeof(*tlde));
    if (tlde == NULL) {
        err_msg_sys("[task.c: task_launched_event_create()] malloc() error");
        return NULL;
    }

    tlde->tev.type = TEV_LAUNCHED;
    tlde->tev.task = t;

    return tlde;
}

Task_get_shf_event* task_get_shf_event_create(Task* t, Mp_srv_request* request) {
    Task_get_shf_event* tgse = (Task_get_shf_event*)malloc(sizeof(*tgse));
    if (tgse == NULL) {
        err_msg_sys("[task.c: task_get_shf_event_create()] malloc() error");
        return NULL;
    }

    tgse->tev.type = TEV_GET_SHF;
    tgse->tev.task = t;

    tgse->request = request;

    return tgse;
}

Task_mapoutput_event* task_mapoutput_event_create(Task* t, Shared_file* sf, int has_data) {
    Task_mapoutput_event* tme = (Task_mapoutput_event*)malloc(sizeof(*tme));
    if (tme == NULL) {
        err_msg_sys("[task.c: task_mapoutput_event_create()] malloc() error");
        return NULL;
    }

    tme->tev.type = TEV_MAPOUTPUT;
    tme->tev.task = t;

    tme->sf = sf;
    tme->sf_has_data = has_data;

    return tme;
}

Task_get_intf_event* task_get_intf_event_create(Task* t, Mp_srv_request* request) {
    Task_get_intf_event* tgie = (Task_get_intf_event*)malloc(sizeof(*tgie));
    if (tgie == NULL) {
        err_msg_sys("[task.c: task_get_intf_event_create()] malloc() error");
        return NULL;
    }

    tgie->tev.type = TEV_GET_INTF;
    tgie->tev.task = t;

    tgie->request = request;

    return tgie;
}

Task_fwd_intf_event* task_fwd_intf_event_create(Task* t, Intermediate_file* intf) {
    Task_fwd_intf_event* tfie = (Task_fwd_intf_event*)malloc(sizeof(*tfie));
    if (tfie == NULL) {
        err_msg_sys("[task.c: task_fwd_intf_event_create()] malloc() error");
        return NULL;
    }

    tfie->tev.type = TEV_FWD_INTF;
    tfie->tev.task = t;

    tfie->intf = intf;

    return tfie;
}

// Task_fwd_intf_event* task_fwd_intf_done_event_create(Task* t);
Task_start_reduce_phase_event* task_start_reduce_phase_event_create(Task* t) {
    Task_start_reduce_phase_event* tsrpe = (Task_start_reduce_phase_event*)malloc(sizeof(*tsrpe));
    if (tsrpe == NULL) {
        err_msg_sys("[task.c: task_start_reduce_phase_event_create()] malloc() error");
        return NULL;
    }

    tsrpe->tev.type = TEV_START_RD_PHASE;
    tsrpe->tev.task = t;

    return tsrpe;
}

Task_succeeded_event* task_succeeded_event_create(Task* t) {
    Task_succeeded_event* tse = (Task_succeeded_event*)malloc(sizeof(*tse));
    if (tse == NULL) {
        err_msg_sys("[task.c: task_succeeded_event_create()] malloc() error");
        return NULL;
    }

    tse->tev.type = TEV_SUCCEEDED;
    tse->tev.task = t;

    return tse;
}

Task_failed_event* task_failed_event_create(Task* t) {
    Task_failed_event* tfe = (Task_failed_event*)malloc(sizeof(*tfe));
    if (tfe == NULL) {
        err_msg_sys("[task.c: task_failed_event_create()] malloc() error");
        return NULL;
    }

    tfe->tev.type = TEV_FAILED;
    tfe->tev.task = t;

    return tfe;
}

Task_abort_event* task_abort_event_create(Task* t) {
    Task_abort_event* tae = (Task_abort_event*)malloc(sizeof(*tae));
    if (tae == NULL) {
        err_msg_sys("[task.c: task_abort_event_create()] malloc() error");
        return NULL;
    }

    tae->tev.type = TEV_ABORT;
    tae->tev.task = t;

    return tae;
}


void task_event_destroy(Task_event* tev) {
    assert(tev != NULL);
    free(tev);
}

const char* task_event_to_string(Task_event* tev) {
    switch (tev->type) {
    case TEV_LAUNCH: {
        return "TEV_LAUNCH";
    }
    case TEV_LAUNCHED: {
        return "TEV_LAUNCHED";
    }
    case TEV_GET_SHF: {
        return "TEV_GET_SHF";
    }
    case TEV_MAPOUTPUT: {
        return "TEV_MAPOUTPUT";
    }
    case TEV_GET_INTF: {
        return "TEV_GET_INTF";
    }
    case TEV_FWD_INTF: {
        return "TEV_FWD_INTF";
    }
    case TEV_START_RD_PHASE: {
        return "TEV_START_RD_PHASE";
    }
    case TEV_SUCCEEDED: {
        return "TEV_SUCCEEDED";
    }
    case TEV_FAILED: {
        return "TEV_FAILED";
    }
    case TEV_ABORT: {
        return "TEV_ABORT";
    }
    // case TEV_CLEANUP: {
    //     return "TEV_CLEANUP";
    // }
    default: {
        return "Unknown task event type";
    }
    }
}

const char* task_state_to_string(Task_state_t t_state) {
    switch (t_state) {
    case TASK_ST_ASSIGNED: {
        return "TASK_ST_ASSIGNED";
    }
    case TASK_ST_RUNNING: {
        return "TASK_ST_RUNNING";
    }
    case TASK_ST_SUCCEEDED: {
        return "TASK_ST_SUCCEEDED";
    }
    case TASK_ST_FAILED: {
        return "TASK_ST_FAILED";
    }
    default: {
        return "Unknown task state";
    }
    }
}

static void handle_task_event(Task_event* tev) {
    Tev_type tet = tev->type;
    void* event_data = tev;

    State_machine* sm = &tev->task->state_machine;
    // handle event
    sm_result_t res = sm_handle_event(sm, tet, event_data);
    if (res == SM_EVENT_IGNORED) {
        printf("[task.c: handle_task_event()] Can't take the event(%s) at current state(%s).\n",
                    task_event_to_string(tev), task_state_to_string(sm->current_state));
    } else if (res == SM_ERR) {
        err_exit("[task.c: handle_task_event()] sm_handle_event error()");
    }
    task_event_destroy(tev);
}

static void* run_handler_thread(void* arg) {
    Task_ev_handler* teh = (Task_ev_handler*)arg;
    Blocking_Q* ev_q = teh->ev_queue;
    Task_event* tev;
    while(1) {
        tev = bq_pop(ev_q);

        if (tev == &teh->shutdown_handler_ev) {
            break;
        }
        handle_task_event(tev);
    }

    // flush ev queue
    while ((tev = bq_pop(ev_q)) != NULL) {
        task_event_destroy(tev);
    }

    return NULL;
}

Task_ev_handler* task_ev_handler_create() {
    Task_ev_handler* teh = malloc(sizeof(*teh));
    if (teh == NULL) {
        err_msg_sys("[task.c: task_ev_handler_create()] malloc() error");
        return NULL;
    }

    Blocking_Q* bq = bq_create();
    if (bq == NULL) {
        err_msg_sys("[task.c: task_ev_handler_create()] bq_create() error");
        free(teh);
        return NULL;
    }
    teh->ev_queue = bq;

    // start handler thread
    int err = pthread_create(&teh->worker_thread, NULL, run_handler_thread, (void*)teh);
    if (err) {
        err_exit_errn(err, "[task.c: task_ev_handler_create()] pthread_create() error");
    }

    return teh;
}

void task_ev_handler_destroy(Task_ev_handler* teh) {
    assert(teh != NULL);
    bq_push(teh->ev_queue, &teh->shutdown_handler_ev);

    pthread_join(teh->worker_thread, NULL);

    bq_destroy(teh->ev_queue);
    free(teh);
}

void task_ev_handler_handle(Task_ev_handler* teh, Task_event* tev) {
    assert(teh != NULL && tev != NULL);
    bq_push(teh->ev_queue, tev);
}

/////////////////// event actions //////////////////////////////////

static sm_action_result_t map_task_launch_action(State_machine* sm, void* event_data) {
    Task_launch_event* tle = event_data;
    Task* t = tle->tev.task;
    assert(t->type == MAP_TASK);

    printf("[worker[task(%s)]] Running map task launch action...\n", t->tid);

    t->launch_req = tle->request;
    // worker_add_task(t->wctx, t);
    // launch task
    /* 
        A socketpair which is used to communicate 
        between the parent(worker) and its child process
    */
    int sockfd[2];
    if (socketpair(AF_LOCAL, SOCK_STREAM, 0, sockfd) < 0) {
        err_exit_sys("[task.c: map_task_launch_action()] socketpair() error");
    }


    // Task* t = tle->tev.task;
    // printf("[worker[task(%s)]] task forked(%d)\n", t->tid, pid);
    
    // worker_add_launched_task(t->wctx, pid, t);
    // worker_add_task_conn(t->wctx, sockfd[0]);
    Map_task* mt = (Map_task*)t;

    pid_t pid;
    // if (pid < 0) {
    //     err_exit_sys("[task.c: map_task_launch_action()] fork() error");
    // }
    switch(pid = fork()) {
    case 0: { // child
        close(sockfd[0]);

        char c;
        read(sockfd[1], &c, 1);
        char mr_prog_path[PATH_MAX], parent_sockfd_str[11], num_partitions_str[11];
        sprintf(mr_prog_path, "%s/%s", worker_get_mr_exe_dir(t->wctx), t->job_name);
        sprintf(parent_sockfd_str, "%d", sockfd[1]);
        sprintf(num_partitions_str, "%d", mt->n_partitions);
        // arguments <parent sockfd> <task type> <job_id> <task id> 
        //                  <inputsplit path> <num_partition_str> <shared_dev_file>
        execlp(mr_prog_path, mr_prog_path, parent_sockfd_str, "m", t->job_id, 
            t->tid, mt->inputsplit_path, num_partitions_str, 
                worker_get_shared_dev_file(t->wctx), (char*)NULL);
        break;
    }
    case -1: {
        err_exit_sys("[task.c: map_task_launch_action()] fork() error");
        break;
    }
    default: { // parent
        close(sockfd[1]);

        t->pid = pid;
        worker_add_launched_task(t->wctx, pid, t);
        worker_add_task_conn(t->wctx, sockfd[0]);

        write(sockfd[0], "x", 1);
    }
    }

    return SM_ACTION_ERR_NONE;
}

static sm_action_result_t map_task_launched_action(State_machine* sm, void* event_data) {
    Task_launched_event* tlde = (Task_launched_event*)event_data;
    Task* t = tlde->tev.task;
    assert(t->type == MAP_TASK);

    printf("[worker[task(%s)]] Running map task launched action...\n", t->tid);

    char reply[MP_MAXMSGLEN];
    sprintf(reply, "%s %s %s\n", J_MT_LNCHD, t->job_id, t->tid);
    mp_server_request_done(t->launch_req, reply);

    return SM_ACTION_ERR_NONE;
}

static sm_action_result_t reduce_task_launch_action(State_machine* sm, void* event_data) {
    Task_launch_event* tle = event_data;
    Task* t = tle->tev.task;
    assert(t->type == REDUCE_TASK);

    printf("[worker[task(%s)]] Running reduce task launch action...\n", t->tid);

    t->launch_req = tle->request;
    // launch task
    /* 
        A socketpair which is used to communicate 
        between the parent(worker) and its child process
    */
    int sockfd[2];
    if (socketpair(AF_LOCAL, SOCK_STREAM, 0, sockfd) < 0) {
        err_exit_sys("[task.c: reduce_task_launch_action()] socketpair() error");
    }

    Reduce_task* rt = (Reduce_task*)t;
    pid_t pid;
    switch(pid = fork()) {
    case 0: { // child
        close(sockfd[0]);

        char c;
        read(sockfd[1], &c, 1);
        char mr_prog_path[PATH_MAX], parent_sockfd_str[11], 
            partition_number_str[11], shared_file_size_str[11];
        sprintf(mr_prog_path, "%s/%s", worker_get_mr_exe_dir(t->wctx), t->job_name);
        sprintf(parent_sockfd_str, "%d", sockfd[1]);
        sprintf(partition_number_str, "%d", rt->partition_number);
        sprintf(shared_file_size_str, "%d", worker_get_shared_file_size(t->wctx));
        // sprintf(output_path, "%s/%s");
        // arguments <parent sockfd> <task type> <job id> <task id>
        //           <partition number str> <output path> <shared_file_size_str>
        execlp(mr_prog_path, mr_prog_path, parent_sockfd_str, "r", t->job_id, 
            t->tid, partition_number_str, rt->outputfile_path, 
                                shared_file_size_str, (char*)NULL);
        break;
    }
    case -1: {
        err_exit_sys("[task.c: reduce_task_launch_action()] fork() error");
        break;
    }
    default: { // parent
        close(sockfd[1]);

        t->pid = pid;
        worker_add_launched_task(t->wctx, pid, t);
        worker_add_task_conn(t->wctx, sockfd[0]);

        write(sockfd[0], "x", 1);
    }
    }

    return SM_ACTION_ERR_NONE;
}

static sm_action_result_t reduce_task_launched_action(State_machine* sm, void* event_data) {
    Task_launched_event* tlde = (Task_launched_event*)event_data;
    Task* t = tlde->tev.task;
    assert(t->type == REDUCE_TASK);

    printf("[worker[task(%s)]] Running reduce task launched action...\n", t->tid);

    char reply[MP_MAXMSGLEN];
    sprintf(reply, "%s %s %s\n", J_RT_LNCHD, t->job_id, t->tid);
    mp_server_request_done(t->launch_req, reply);

    return SM_ACTION_ERR_NONE;
}

static sm_action_result_t get_shf_action(State_machine* sm, void* event_data) {
    Task_get_shf_event* tgse = (Task_get_shf_event*)event_data;
    Task* t = tgse->tev.task;
    assert(t->type == MAP_TASK);

    printf("[worker[task(%s)]] Running get shared file action...\n", t->tid);

    const char* shared_dir = worker_get_shared_dir(t->wctx);
    // const char* host_ip = worker_get_host_ip(t->wctx);
    const char* worker_name = worker_get_name(t->wctx);
    Shared_file* sf = worker_get_shared_file(t->wctx);
    ((Map_task*)t)->cur_allocated_shf = sf;

    char reply[MP_MAXMSGLEN];
    sprintf(reply, "%s %s/%s %s %d\n", T_SHF, shared_dir, worker_name, sf->name, sf->capacity);
    mp_server_request_done(tgse->request, reply);

    return SM_ACTION_ERR_NONE;
}

static sm_action_result_t mapoutput_action(State_machine* sm, void* event_data) {
    Task_mapoutput_event* tme = (Task_mapoutput_event*)event_data;
    Task* t = tme->tev.task;
    Map_task* mt = (Map_task*)t;

    printf("[worker[task(%s)]] Running mapoutput action...\n", t->tid);

    if (tme->sf_has_data) {
        TAILQ_INSERT_TAIL(&mt->mapoutputs, tme->sf, sf_link);
    } else {
        worker_release_shared_file(t->wctx, tme->sf);
    }

    mt->cur_allocated_shf = NULL;

    return SM_ACTION_ERR_NONE;
}

static void notity_mapoutputs(Map_task* mt, Mp_client* master_conn) {
    // Mp_client* master_conn = worker_get_master_conn(t->wctx);
    char msg[MP_MAXMSGLEN];
    // send all mapoutputs to master
    Shared_file* sf;
    const char* job_id = mt->t.job_id;
    const char* task_id = mt->t.tid;
    TAILQ_FOREACH(sf, &mt->mapoutputs, sf_link) {
        // J_MAPOUTPUT <jid> <tid> <sharedfile name> <metadata size> <location>
        sprintf(msg, "%s %s %s %s %d %s\n", J_MAPOUTPUT, job_id, task_id, sf->name,
                                    sf->metadata_size, worker_get_name(mt->t.wctx));
        mp_client_send_request(master_conn, msg, NULL, NULL);
    }
}

static sm_action_result_t task_succeeded_action(State_machine* sm, void* event_data) {
    Task_succeeded_event* tse = (Task_succeeded_event*)event_data;
    Task* t = tse->tev.task;
    // Reduce_task* rt = (Reduce_task*)t;
    // const char* msg_type;
    printf("[worker[task(%s)]] Running task succeeded action...\n", t->tid);

    Mp_client* master_conn = worker_get_master_conn(t->wctx);

    if (t->type == MAP_TASK) {
        notity_mapoutputs((Map_task*)t, master_conn);
    }

    char msg[MP_MAXMSGLEN];
    if (t->type == MAP_TASK) {
        // J_TASK_DONE <job_id> <task id> <t_failed>
        sprintf(msg, "%s %s %s %d\n", J_TASK_DONE, t->job_id, t->tid, 0);
    } else {
        char absolute_path[PATH_MAX];
        if (realpath(((Reduce_task*)t)->outputfile_path, absolute_path) == NULL) {
            err_exit_sys("[task.c: task_succeeded_action()] realpath() error");
        }
        // J_TASK_DONE <job_id> <task id> <t_failed> <outputfile path>
        sprintf(msg, "%s %s %s %d %s\n", J_TASK_DONE, t->job_id, t->tid, 0,
            absolute_path);
    }
    mp_client_send_request(master_conn, msg, NULL, NULL);

    return SM_ACTION_ERR_NONE;
}

static void send_task_failed(Task* t) {
    Mp_client* master_conn = worker_get_master_conn(t->wctx);

    char msg[MP_MAXMSGLEN];
    if (t->type == MAP_TASK) {
        // J_TASK_DONE <job_id> <task id> <t_failed>
        sprintf(msg, "%s %s %s %d\n", J_TASK_DONE, t->job_id, t->tid, 1);
    } else {
        // J_TASK_DONE <job_id> <task id> <t_failed> < >
        sprintf(msg, "%s %s %s %d \n", J_TASK_DONE, t->job_id, t->tid, 1);
    }
    mp_client_send_request(master_conn, msg, NULL, NULL);
}

static sm_action_result_t task_failed_action(State_machine* sm, void* event_data) {
    Task_failed_event* tfe = (Task_failed_event*)event_data;
    Task* t = tfe->tev.task;

    printf("[worker[task(%s)]] Running task failed action...\n", t->tid);

    send_task_failed(t);

    return SM_ACTION_ERR_NONE;
}

static void forward_intermediate_file(Reduce_task* rt, Mp_srv_request* req) {
    Intermediate_file* intf = deque_front(rt->intermediate_file_Q)->elm;
    deque_pop_front(rt->intermediate_file_Q);

    char reply[MP_MAXMSGLEN];
    // T_INTF <intermediate file path> <intermediate metadata size>
    sprintf(reply, "%s %s/%s/%s %d\n", T_INTF, worker_get_shared_dir(rt->t.wctx), 
                                    intf->worker_name, intf->file_name, intf->metadata_size);
    printf("[worker[task(%s)]] Forwarding intermediate file: %s", rt->t.tid, reply);
    mp_server_request_done(req, reply);
    free(intf);
    // return SM_ACTION_ERR_NONE;
}

static sm_action_result_t fwd_intf_action(State_machine* sm, void* event_data) {
    Task_fwd_intf_event* tfie = (Task_fwd_intf_event*)event_data;
    Task* t = tfie->tev.task;
    Reduce_task* rt = (Reduce_task*)t;

    printf("[worker[task(%s)]] Running forward intermediate file action...\n", t->tid);

    deque_push_back(rt->intermediate_file_Q, tfie->intf);
    if (rt->intf_req != NULL) {
        forward_intermediate_file(rt, rt->intf_req);
        rt->intf_req = NULL;
    }

    return SM_ACTION_ERR_NONE;
}

static void start_reduce_phase(Reduce_task* rt, Mp_srv_request* req) {
    char reply[MP_MAXMSGLEN];
    sprintf(reply, "%s\n", T_START_REDUCE_PHASE);
    mp_server_request_done(req, reply);
}

static sm_action_result_t get_intf_action(State_machine* sm, void* event_data) {
    Task_get_intf_event* tgie = (Task_get_intf_event*)event_data;
    Task* t = tgie->tev.task;
    Reduce_task* rt = (Reduce_task*)t;

    printf("[worker[task(%s)]] Running get intermediate file action...\n", t->tid);

    if (!deque_empty(rt->intermediate_file_Q)) {
        forward_intermediate_file(rt, tgie->request);
        return SM_ACTION_ERR_NONE;
    }

    // if (rt->fwd_intf_done) {
    if (rt->start_reduce_phase) {
        // forward_intermediate_file_done(rt, tgie->request);
        start_reduce_phase(rt, tgie->request);
        return SM_ACTION_ERR_NONE;
    }

    rt->intf_req = tgie->request;

    return SM_ACTION_ERR_NONE;
}

static sm_action_result_t start_reduce_phase_action(State_machine* sm, void* event_data) {
    Task_start_reduce_phase_event* tsrpe = (Task_start_reduce_phase_event*)event_data;
    Task* t = tsrpe->tev.task;
    Reduce_task* rt = (Reduce_task*)t;

    printf("[worker[task(%s)]] Running start reduce phase action...\n", t->tid);

    // rt->fwd_intf_done = true;
    rt->start_reduce_phase = true;
    if (deque_empty(rt->intermediate_file_Q) && rt->intf_req != NULL) {
        // forward_intermediate_file_done(rt, rt->intf_req);
        start_reduce_phase(rt, rt->intf_req);
    }

    return SM_ACTION_ERR_NONE;
}

static sm_action_result_t task_abort_action(State_machine* sm, void* event_data) {
    Task_abort_event* tae = (Task_abort_event*)event_data;
    Task* t = tae->tev.task;

    printf("[worker[task(%s)]] Running task abort action...\n", t->tid);

    kill(t->pid, SIGTERM);
    if (t->type == MAP_TASK) {
        Map_task* mt = (Map_task*)t;
        if (mt->cur_allocated_shf != NULL) {
            // put the shared file in the mapoutputs for the later cleanup
            TAILQ_INSERT_TAIL(&mt->mapoutputs, mt->cur_allocated_shf, sf_link);
            mt->cur_allocated_shf = NULL;
        }
    }

    send_task_failed(t);

    return SM_ACTION_ERR_NONE;
}
