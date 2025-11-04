#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>
#include <linux/limits.h>
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include "utils/my_err.h"
#include "utils_ds/state_machine.h"
#include "job.h"
#include "utils_ds/blocking_queue.h"
#include "msg_pass/mp_client.h"
#include "msg_pass/mp_server.h"
#include "MR_msg_type.h"
#include "msg_pass/mp_buf_sizes.h"
#include "master.h"

#define LINE_MAX 4096

static int next_job_id_num = 0;
/*
    For now, we just use a single job for the project.
    A hash table for id - job mapping will be used later
*/

static sm_action_result_t job_start_action(State_machine* sm, void* event_data);
static sm_action_result_t job_task_launched_action(State_machine* sm, void* event_data);
static sm_action_result_t job_mapoutput_action(State_machine* sm, void* event_data);
static sm_action_result_t job_task_succeeded_action(State_machine* sm, void* event_data);
static sm_action_result_t job_task_failed_action(State_machine* sm, void* event_data);
static sm_action_result_t job_done_action(State_machine* sm, void* event_data);
static sm_action_result_t job_task_cleaned_up_action(State_machine* sm, void* event_data);

// transition table at state INITED
DEFINE_TRANSITION_TABLE_PER_STATE(table_for_INITED, 
    TABLE_ELEMENT(JOB_ST_RUNNING, job_start_action), // for START event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for TASK_LNCHD event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for MAPOUTPUT event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for TASK_SUCCEEDED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for TASK_FAILED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),        // for DONE event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL)         // for CLEANED UP event
);
// transition table at state RUNNING
DEFINE_TRANSITION_TABLE_PER_STATE(table_for_RUNNING, 
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),   // for START event
    TABLE_ELEMENT(JOB_ST_RUNNING, job_task_launched_action),         // for TASK_LNCHD event
    TABLE_ELEMENT(JOB_ST_RUNNING, job_mapoutput_action),         // for MAPOUTPUT event
    TABLE_ELEMENT(JOB_ST_RUNNING, job_task_succeeded_action),         // for TASK_SUCCEEDED event
    TABLE_ELEMENT(JOB_ST_RUNNING, job_task_failed_action),         // for TASK_FAILED event
    TABLE_ELEMENT(JOB_ST_DONE, job_done_action),         // for DONE event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL)         // for CLEANED UP event
);
// transition table at state DONE
DEFINE_TRANSITION_TABLE_PER_STATE(table_for_DONE, 
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),   // for START event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for TASK_LNCHD event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for MAPOUTPUT event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for TASK_SUCCEEDED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for TASK_FAILED event
    TABLE_ELEMENT(STATE_EVENT_IGNORED, NULL),         // for DONE event
    TABLE_ELEMENT(JOB_ST_DONE, job_task_cleaned_up_action)         // for CLEANED UP event
);

DEFINE_COMBINED_TRANSITION_TABLE(transition_table_for_job,
    table_for_INITED, 
    table_for_RUNNING,
    table_for_DONE);


struct Job_ev_handler {
    Blocking_Q* ev_queue;
    pthread_t worker_thread;

    Job_event shutdown_handler_ev;
};


static void read_inputsplit_info(Job* job) {
    printf("[[job]] Reading inputsplit_infos...\n");
    FILE* isf = fopen(job->input_metadata_path, "r");
    if (isf == NULL) {
        err_exit_sys("[job.c: read_inputsplit_info()] fopen() error");
    }

    char line[LINE_MAX];
    while (fgets(line, sizeof(line), isf) != NULL) {
        Input_split* is = (Input_split*)malloc(sizeof(*is));
        if (is == NULL) {
            err_exit_sys("[job.c: read_inputsplit_info()] malloc() error");
        }

        char worker_name[NAME_MAX];
        sscanf(line, "%[^:]:%s\n", is->file_path, worker_name);
        printf("[[job]] isplit path: %s, location: %s\n", is->file_path, worker_name);

        Worker_conn* wc = master_ctx_get_worker_conn(job->mctx, worker_name);

        is->location = wc;

        TAILQ_INSERT_TAIL(&job->input_splits, is, split_link);
        job->n_input_splits++;
    }
    if (ferror(isf)) {
        err_exit_sys("[job.c: read_inputsplit_info()] fgets() error");
    }
    fclose(isf);
}


static const char* job_get_next_tid(Job* job) {
    sprintf(job->next_tid, "task_%d", job->next_tid_num++);

    return job->next_tid;
}

static void create_map_task_infos(Job* job) {
    Input_split* is;
    Worker_conn* wc;
    TAILQ_FOREACH(is, &job->input_splits, split_link) {
        Map_task_info* mti = (Map_task_info*)malloc(sizeof(*mti));
        if (mti == NULL) {
            err_exit("[job.c: create_map_task_infos()] malloc() error");
        }
        mti->ti.type = MAP_TASK_INFO;
        sprintf(mti->ti.tid, "%s", job_get_next_tid(job));
        mti->ti.job = job;
        mti->ti.done = false;
        mti->ti.failed = false;

        mti->isplit = is;
        mti->mapoutputs = deque_create();
        if (mti->mapoutputs == NULL) {
            err_exit("[job.c: create_map_task_infos()] deque_create() error");
        }

        TAILQ_INSERT_TAIL(&job->map_task_infos, mti, mti_link);
        hash_table_put(job->task_id_map, mti->ti.tid, mti);
        wc = is->location;
        // worker_conn_incr_n_assigned_task(wc);
        // master_ctx_worker_incr_assigned_task(job->mctx, wc);
        worker_conn_assign_task(wc, (Task_info*)mti);
        // job->n_map_tasks++;
        // job->n_total_tasks++;
    }
}

static void create_reduce_task_infos(Job* job) {
    // Worker_conn* wc;
    // RBT_iterator* it;
    for (int i = 0; i < job->n_reduce_tasks; i++) {
        Reduce_task_info* rti = (Reduce_task_info*)malloc(sizeof(*rti));
        if (rti == NULL) {
            err_exit("[job.c: create_reduce_task_infos()] malloc() error");
        }
        rti->ti.type = REDUCE_TASK_INFO;
        sprintf(rti->ti.tid, "%s", job_get_next_tid(job));
        rti->ti.job = job;
        rti->ti.done = false;
        rti->ti.failed = false;

        sprintf(rti->outputfile_name, "%s_out_%d", job->name, i);
        rti->partition_number = i;
        // get the first worker which has lowest number of assigned tasks
        Worker_conn* wc = master_ctx_get_min_assigned_task_worker(job->mctx);
        rti->location = wc;
        TAILQ_INSERT_TAIL(&job->reduce_task_infos, rti, rti_link);
        hash_table_put(job->task_id_map, rti->ti.tid, rti);
        // worker_conn_incr_n_assigned_task(wc);
        // master_ctx_worker_incr_assigned_task(job->mctx, rti->location);
        worker_conn_assign_task(wc, (Task_info*)rti);
        // job->n_reduce_tasks++;
        // job->n_total_tasks++;
    }
}

static void create_task_infos(Job* job) {
    create_map_task_infos(job);
    create_reduce_task_infos(job);
    master_ctx_print_assigned_tasks(job->mctx);
}

static void job_inited(Req_completion* rc, void* cb_arg) {
    if (rc->status != REQ_ERR_NONE) {
        err_exit("[job.c: job_inited()] req error, req status: %s\n"
        "req: %s, resp: %s", req_status_str(rc->status), rc->request_msg, rc->respose_msg);
    }

    const char* msg = rc->respose_msg;
    Job* job = (Job*)cb_arg;

    if (strncmp(msg, J_INITED, strlen(J_INITED)) == 0) {
        pthread_mutex_lock(&job->lock);
        job->n_job_inited_workers++;
        pthread_cond_signal(&job->cv);
        pthread_mutex_unlock(&job->lock);
    } else {
        err_exit("[job.c: job_inited()] Unknown resp msg: %s", msg);
    }
}

static void send_job_init_msg(Job* job) {
    Worker_conn_list* wcl = master_ctx_get_worker_list(job->mctx);
    Worker_conn* wc = worker_conn_first(wcl);

    char msg[MP_MAXMSGLEN];
    // msg: J_INIT <job_name> <job_id> <job_output_dir>
    sprintf(msg, "%s %s %s %s\n", J_INIT, job->name, job->id, job->job_output_dir);
    while (wc != NULL) {
        mp_client_send_request(worker_conn_get_client(wc), msg, job_inited, job);
        wc = worker_conn_next(wc);
    }
}

static void wait_job_inited(Job* job) {
    pthread_mutex_lock(&job->lock);
    while (job->n_job_inited_workers < job->n_workers) {
        pthread_cond_wait(&job->cv, &job->lock);
    }
    pthread_mutex_unlock(&job->lock);
}

void job_init(Job* job, const char* name, const char* input_metadata_file, 
    const char* output_metadata_file, Job_ev_handler* jev_handler, 
    int n_workers, Master_context* mctx) {
    sm_init(&job->state_machine, JOB_ST_INITED, transition_table_for_job);
    sprintf(job->name, "%s", name);
    sprintf(job->id, "job_%d", next_job_id_num++);
    // job->id = next_job_id++;
    sprintf(job->input_metadata_path, "%s", input_metadata_file);
    sprintf(job->output_metadata_path, "%s", output_metadata_file);

    // create output metadtat file
    int fd;
    if ((fd = open(job->output_metadata_path, O_WRONLY | O_CREAT, 0664)) < 0) {
        err_exit_sys("[job.c: job_init()] create file (%s) error", job->output_metadata_path);
    }
    close(fd);

    // build job output dir
    // int n = sprintf(job->job_output_dir, "%s/", job->name);
    char* p = strrchr(output_metadata_file, '/');
    const char* fname = p? p + 1: output_metadata_file;
        
    snprintf(job->job_output_dir, 
        strlen(fname) - strlen(".meta") + 1,"%s", fname);

    job->n_input_splits = 0;
    TAILQ_INIT(&job->input_splits);

    // read input_metadata_file
    job->next_tid_num = 0;
    // job->n_total_tasks = 0;
    job->n_map_tasks = 0;
    job->n_reduce_tasks = 0;
    TAILQ_INIT(&job->map_task_infos);
    job->succeeded_map_task_infos = deque_create();
    if (job->succeeded_map_task_infos == NULL) {
        err_exit("[job.c: job_init()] deque_create(succeeded_map_task_infos) error");
    }
    job->failed_map_task_infos = deque_create();
    if (job->failed_map_task_infos == NULL) {
        err_exit("[job.c: job_init()] deque_create(failed_map_task_infos) error");
    }

    TAILQ_INIT(&job->reduce_task_infos);
    job->succeeded_reduce_task_infos = deque_create();
    if (job->succeeded_reduce_task_infos == NULL) {
        err_exit("[job.c: job_init()] deque_create(succeeded_reduce_task_infos) error");
    }
    job->failed_reduce_task_infos = deque_create();
    if (job->failed_reduce_task_infos == NULL) {
        err_exit("[job.c: job_init()] deque_create(failed_reduce_task_infos) error");
    }
    
    job->task_id_map = hash_table_create();
    if (job->task_id_map == NULL) {
        err_exit("[job.c: job_init()] hash_table_create() error");
    }

    job->n_workers = n_workers;
    job->n_job_inited_workers = 0;

    job->n_launched_map_task = 0;
    job->n_launched_reduce_task = 0;
    job->n_succeeded_map_task = 0;
    job->n_failed_map_task = 0;
    job->n_succeeded_reduce_task = 0;
    job->n_failed_reduce_task = 0;
    job->n_cleaned_up_task = 0;

    job->aborted = false;

    job->jev_handler = jev_handler;

    job->done = false;
    job->failed = false;

    job->mctx = mctx;
    pthread_mutex_init(&job->lock, NULL);
    pthread_cond_init(&job->cv, NULL);

    // job_ptr = job;

    read_inputsplit_info(job);
    job->n_reduce_tasks = job->n_map_tasks = job->n_input_splits;
    create_task_infos(job);

    // send job init msg
    send_job_init_msg(job);
    // wait for inited
    wait_job_inited(job);
}

/////////////////////// job events ////////////////////////////////////

Job_start_event* job_start_event_create(Job* job) {
    Job_start_event* jse = (Job_start_event*)malloc(sizeof(*jse));
    if (jse == NULL) {
        err_msg_sys("[job.c: job_start_event_create()] malloc() error");
        return NULL;
    }

    jse->jev.type = JEV_START;
    jse->jev.job = job;

    return jse;
}

Job_task_lnchd_event* job_task_lnchd_event_create(Job* job, Task_info* ti) {
    Job_task_lnchd_event* jtle = (Job_task_lnchd_event*)malloc(sizeof(*jtle));
    if (jtle == NULL) {
        err_msg_sys("[job.c: job_task_lnchd_event_create()] malloc() error");
        return NULL;
    }

    jtle->jev.type = JEV_TASK_LNCHD;
    jtle->jev.job = job;

    jtle->ti = ti;

    return jtle;
}

Job_mapoutput_event* job_mapoutput_event_create(Job* job, Task_info* ti,
                                                    Intermediate_file* intf) {
    Job_mapoutput_event* jme = (Job_mapoutput_event*)malloc(sizeof(*jme));
    if (jme == NULL) {
        err_msg_sys("[job.c: job_mapoutput_event_create()] malloc() error");
        return NULL;
    }

    jme->jev.type = JEV_MAPOUTPUT;
    jme->jev.job = job;

    // sprintf(jme->tid, "%s", tid);
    jme->ti = ti;
    jme->intf = intf;
    
    return jme;
}

Job_task_succeeded_event* job_task_succeeded_event_create(Job* job, Task_info* ti) {
    Job_task_succeeded_event* jtse = (Job_task_succeeded_event*)malloc(sizeof(*jtse));
    if (jtse == NULL) {
        err_msg_sys("[job.c: job_task_succeeded_event_create()] malloc() error");
        return NULL;
    }

    jtse->jev.type = JEV_TASK_SUCCEEDED;
    jtse->jev.job = job;

    // sprintf(jtse->tid, "%s", tid);
    jtse->ti = ti;
    
    return jtse;
}

Job_task_failed_event* job_task_failed_event_create(Job* job, Task_info* ti) {
    Job_task_failed_event* jtfe = (Job_task_failed_event*)malloc(sizeof(*jtfe));
    if (jtfe == NULL) {
        err_msg_sys("[job.c: job_task_failed_event_create()] malloc() error");
        return NULL;
    }

    jtfe->jev.type = JEV_TASK_FAILED;
    jtfe->jev.job = job;

    // sprintf(jtfe->tid, "%s", tid);
    jtfe->ti = ti;
    
    return jtfe;
}

Job_done_event* job_done_event_create(Job* job, bool failed) {
    Job_done_event* jde = (Job_done_event*)malloc(sizeof(*jde));
    if (jde == NULL) {
        err_msg_sys("[job.c: job_done_event_create()] malloc() error");
        return NULL;
    }

    jde->jev.type = JEV_DONE;
    jde->jev.job = job;

    jde->failed = failed;
    
    return jde;
}

Job_task_cleaned_up_event* job_task_cleaned_up_event_create(Job* job, Task_info* ti) {
    Job_task_cleaned_up_event* jtcue = (Job_task_cleaned_up_event*)malloc(sizeof(*jtcue));
    if (jtcue == NULL) {
        err_msg_sys("[job.c: job_task_cleaned_up_event_create()] malloc() error");
        return NULL;
    }

    jtcue->jev.type = JEV_TASK_CLEANED_UP;
    jtcue->jev.job = job;

    // sprintf(jtcue->tid, "%s", tid);
    jtcue->ti = ti;
    
    return jtcue;
}

void job_event_destroy(Job_event* jev) {
    assert(jev != NULL);
    free(jev);
}

const char* job_event_to_string(Job_event* jev) {
    switch (jev->type) {
    case JEV_START: {
        return "JEV_START";
        // break;
    }
    case JEV_TASK_LNCHD: {
        return "JEV_TASK_LNCHD";
        // break;
    }
    case JEV_MAPOUTPUT: {
        return "JEV_MAPOUTPUT";
        // break;
    }
    case JEV_TASK_SUCCEEDED: {
        return "JEV_TASK_SUCCEEDED";
        // break;
    }
    case JEV_TASK_FAILED: {
        return "JEV_TASK_FAILED";
        // break;
    }
    case JEV_DONE: {
        return "JEV_DONE";
        // break;
    }
    default: {
        return "Unknown job event type";
    }
    }
}

const char* job_state_to_string(Job_state_t j_state) {
    switch (j_state) {
    case JOB_ST_INITED: {
        return "JOB_ST_INITED";
    }
    case JOB_ST_RUNNING: {
        return "JOB_ST_RUNNING";
    }
    case JOB_ST_DONE: {
        return "JOB_ST_DONE";
    }
    default: {
        return "Unknown job state";
    }
    }
}

static void handle_job_event(/* Job* job,  */Job_event* jev) {
    Jev_type jet = jev->type;
    void* event_data = jev;
    // State_machine* smp = &job->state_machine;
    // State_machine* sm = job_get_state_machine(job);
    State_machine* sm = &jev->job->state_machine;
    // handle event
    sm_result_t res = sm_handle_event(sm, jet, event_data);
    if (res == SM_EVENT_IGNORED) {
        printf("[job.c: handle_job_event()] Can't take the event(%s) at current state(%s).\n",
                    job_event_to_string(jev), job_state_to_string(sm->current_state));
    } else if (res == SM_ERR) {
        err_exit("[job.c: handle_job_event()] sm_handle_event error()");
    }
    job_event_destroy(jev);
}

static void* run_handler_thread(void* arg) {
    Job_ev_handler* jeh = (Job_ev_handler*)arg;
    Blocking_Q* ev_q = jeh->ev_queue;
    Job_event* jev;
    while(1) {
        jev = bq_pop(ev_q);

        if (jev == &jeh->shutdown_handler_ev) {
            break;
        }
        handle_job_event(/* jev->job,  */jev);
    }

    // flush ev queue
    while ((jev = bq_pop(ev_q)) != NULL) {
        job_event_destroy(jev);
    }

    return NULL;
}

Job_ev_handler* job_ev_handler_create() {
    Job_ev_handler* jeh = malloc(sizeof(*jeh));
    if (jeh == NULL) {
        err_msg_sys("[job.c: job_ev_handler_create()] malloc() error");
        return NULL;
    }

    Blocking_Q* bq = bq_create();
    if (bq == NULL) {
        err_msg_sys("[job.c: job_ev_handler_create()] bq_create() error");
        free(jeh);
        return NULL;
    }
    jeh->ev_queue = bq;

    // start handler thread
    int err = pthread_create(&jeh->worker_thread, NULL, run_handler_thread, (void*)jeh);
    if (err) {
        err_exit_errn(err, "[job.c: job_ev_handler_create()] pthread_create() error");
    }

    return jeh;
}

void job_ev_handler_destroy(Job_ev_handler* jeh) {
    assert(jeh != NULL);
    bq_push(jeh->ev_queue, &jeh->shutdown_handler_ev);

    pthread_join(jeh->worker_thread, NULL);

    bq_destroy(jeh->ev_queue);
    free(jeh);
}

void job_ev_handler_handle(Job_ev_handler* handler, Job_event* jev) {
    assert(handler != NULL && jev != NULL);
    bq_push(handler->ev_queue, jev);
}

Task_info* job_get_task(Job* job, const char* tid) {
    assert(job != NULL); assert(tid != NULL);

    return hash_table_get(job->task_id_map, tid);
}


/////////////////// event actions //////////////////////////////////


static void handle_task_launched(Req_completion* rc, void* cb_arg) {
    if (rc->status != REQ_ERR_NONE) {
        err_exit("[job.c: handle_task_launched()] req error, req status: %s\n"
        "req: %s, resp: %s", req_status_str(rc->status), rc->request_msg, rc->respose_msg);
    }

    // Map_task_info* mti = (Map_task_info*)cb_arg;
    Job* job = (Job*)cb_arg;
    const char* msg = rc->respose_msg;
    if (strncmp(msg, J_MT_LNCHD, strlen(J_MT_LNCHD)) == 0) {
        msg += strlen(J_MT_LNCHD);
        
    } else if (strncmp(msg, J_RT_LNCHD, strlen(J_RT_LNCHD)) == 0) {
        msg += strlen(J_RT_LNCHD);
    }
    else {
        err_exit("[job.c: handle_task_launched()] Unknown resp msg: %s", msg);
    }

    char jid[NAME_MAX], tid[NAME_MAX];
    sscanf(msg, " %s %s\n", jid, tid);
    Task_info* ti = hash_table_get(job->task_id_map, tid);
    assert(ti != NULL);
    
    Job_task_lnchd_event* jtle = job_task_lnchd_event_create(job, ti);
    job_ev_handler_handle(job->jev_handler, (Job_event*)jtle);
}

static void launch_tasks(Job* job) {
    Map_task_info* mti;
    // launch map tasks
    TAILQ_FOREACH(mti, &job->map_task_infos, mti_link) {
        // Map_task* mt = mti->mt;
        Worker_conn* wc = mti->isplit->location;
        char msg[MP_MAXMSGLEN];
        // msg: J_LNCH_MT <job_id> <task_id> <inputsplit path> <num_partitons>
        snprintf(msg, sizeof(msg), "%s %s %s %s %d\n",J_LNCH_MT, job->id, mti->ti.tid, 
            mti->isplit->file_path, job->n_reduce_tasks);

        printf("[sending launch map task] %s", msg);
        mp_client_send_request(worker_conn_get_client(wc), msg, handle_task_launched, job);
    }

    // launch reduce tasks
    Reduce_task_info* rti;
    TAILQ_FOREACH(rti, &job->reduce_task_infos, rti_link) {
        Worker_conn* wc = rti->location;
        char msg[MP_MAXMSGLEN];
        // J_LNCH_RT <job id> <task id> <outputfile name> <partition number>
        sprintf(msg, "%s %s %s %s %d\n",J_LNCH_RT, job->id, rti->ti.tid, 
            rti->outputfile_name, rti->partition_number);

        printf("[sending launch reduce task] %s", msg);
        mp_client_send_request(worker_conn_get_client(wc), msg, handle_task_launched, job);
    }
}

static sm_action_result_t job_start_action(State_machine* sm, void* event_data) {
    Job_start_event* jse = (Job_start_event*)event_data;
    Job* job = jse->jev.job;

    printf("[master[job(%s)]] Running job start action...\n", job->name);

    launch_tasks(job);

    return SM_ACTION_ERR_NONE;
}

static sm_action_result_t job_task_launched_action(State_machine* sm, void* event_data) {
    Job_task_lnchd_event* jtle = (Job_task_lnchd_event*)event_data;
    Job* job = jtle->jev.job;

    printf("[master[job(%s)]] (%s) Running job launched action...\n", job->name, jtle->ti->tid);

    if (jtle->ti->type == MAP_TASK_INFO) {
        job->n_launched_map_task++;
    } else {
        job->n_launched_reduce_task++;
    }

    return SM_ACTION_ERR_NONE;
}

static sm_action_result_t job_mapoutput_action(State_machine* sm, void* event_data) {
    Job_mapoutput_event* jme = (Job_mapoutput_event*)event_data;
    Job* job = jme->jev.job;
    // Map_task_info* mti = hash_table_get(job->task_id_map, jme->tid);
    Map_task_info* mti = (Map_task_info*)jme->ti;
    assert(mti != NULL);
    assert(mti->ti.type == MAP_TASK_INFO);

    printf("[master[job(%s)]] (%s) Running job mapoutput action...\n", job->name, mti->ti.tid);

    deque_push_back(mti->mapoutputs, jme->intf);

    return SM_ACTION_ERR_NONE;
}

static void forward_intermediate_files(Job* job, Map_task_info* mti) {
    // for each reduce task
    // send all intermediate files in the mti
    Reduce_task_info* rti;
    char msg[MP_MAXMSGLEN];
    TAILQ_FOREACH(rti, &job->reduce_task_infos, rti_link) {
        Deque_iterator* dit = deque_front(mti->mapoutputs);
        while (dit != NULL) {
            Intermediate_file* intf = dit->elm;
            // msg: J_FWD_INTF_LOC <job_id> <task_id> 
            // <intermediate file name> <intermediate metadata size> <intermediate file location>
            sprintf(msg, "%s %s %s %s %d %s\n", J_FWD_INTF_LOC, job->id, rti->ti.tid, 
                intf->file_name, intf->metadata_size, intf->worker_name);
            mp_client_send_request(worker_conn_get_client(rti->location), msg, NULL, NULL);
            dit = deque_next(mti->mapoutputs, dit);
        }
    }

    // free all intermediate file objs
    while (!deque_empty(mti->mapoutputs)) {
        Intermediate_file* intf = deque_front(mti->mapoutputs)->elm;
        deque_pop_front(mti->mapoutputs);
        free(intf);
    }
}

static void start_reduce_phase(Job* job) {
    // for each reduce task
    // start reduce phase
    Reduce_task_info* rti;
    char msg[MP_MAXMSGLEN];

    TAILQ_FOREACH(rti, &job->reduce_task_infos, rti_link) {
        // J_START_REDUCE_PHASE <job_id> <task_id>
        sprintf(msg, "%s %s %s\n", J_START_REDUCE_PHASE, job->id, rti->ti.tid);
        mp_client_send_request(worker_conn_get_client(rti->location), msg, NULL, NULL);
    }
}

static sm_action_result_t job_task_succeeded_action(State_machine* sm, void* event_data) {
    Job_task_succeeded_event* jtse = (Job_task_succeeded_event*)event_data;
    Job* job = jtse->jev.job;
    // Task_info* ti = hash_table_get(job->task_id_map, jtse->tid);
    Task_info* ti = jtse->ti;
    assert(ti != NULL);

    printf("[master[job(%s)]] (%s) Running job task succeeded action...\n", job->name, ti->tid);

    ti->done = true;
    if (ti->type == MAP_TASK_INFO) {
        deque_push_back(job->succeeded_map_task_infos, ti);
        job->n_succeeded_map_task++;
        forward_intermediate_files(job, (Map_task_info*)ti);
        if (job->n_succeeded_map_task == job->n_map_tasks) {
            start_reduce_phase(job);
        }
    } else {
        // write_output_metadata(job, (Reduce_task_info*)ti);
        deque_push_back(job->succeeded_reduce_task_infos, ti);
        job->n_succeeded_reduce_task++;
        if (job->n_succeeded_reduce_task == job->n_reduce_tasks) {
            Job_done_event* jde = job_done_event_create(job, false);
            if (jde == NULL) {
                err_exit("[job.c: job_task_succeeded_action()] job_done_event_create() error");
            }

            job_ev_handler_handle(job->jev_handler, (Job_event*)jde);
        }

    }

    return SM_ACTION_ERR_NONE;
}

static void abort_job(Job* job) {
    job->aborted = true;

    char msg[MP_MAXMSGLEN];
    Map_task_info* mti;
    TAILQ_FOREACH(mti, &job->map_task_infos, mti_link) {
        if (!mti->ti.done) {
            // J_ABORT <job_id> <task_id>
            sprintf(msg, "%s %s %s\n", J_ABORT, job->id, mti->ti.tid);
            mp_client_send_request(worker_conn_get_client(mti->isplit->location), msg, NULL, NULL);
        }
    }
    Reduce_task_info* rti;
    TAILQ_FOREACH(rti, &job->reduce_task_infos, rti_link) {
        if (!rti->ti.done) {
            // J_ABORT <job_id> <task_id>
            sprintf(msg, "%s %s %s\n", J_ABORT, job->id, rti->ti.tid);
            mp_client_send_request(worker_conn_get_client(rti->location), msg, NULL, NULL);
        }
    }
}

static sm_action_result_t job_task_failed_action(State_machine* sm, void* event_data) {
    Job_task_failed_event* jtfe = (Job_task_failed_event*)event_data;
    Job* job = jtfe->jev.job;
    // Task_info* ti = hash_table_get(job->task_id_map, jtfe->tid);
    Task_info* ti = jtfe->ti;
    assert(ti != NULL);

    printf("[master[job(%s)]] (%s) Running job task failed action...\n", job->name, ti->tid);

    ti->done = true;
    ti->failed = true;

    if (ti->type == MAP_TASK_INFO) {
        deque_push_back(job->failed_map_task_infos, ti);
        job->n_failed_map_task++;
        if (!job->aborted) {
            abort_job(job);
        }
    } else {
        deque_push_back(job->failed_reduce_task_infos, ti);
        job->n_failed_reduce_task++;
        if (job->n_reduce_tasks == 
                job->n_succeeded_reduce_task + job->n_failed_reduce_task) {
            Job_done_event* jde = job_done_event_create(job, true);
            if (jde == NULL) {
                err_exit("[job.c: job_task_failed_action()] job_done_event_create() error");
            }

            job_ev_handler_handle(job->jev_handler, (Job_event*)jde);
        } else if (!job->aborted) {
            abort_job(job);
        }
    }

    return SM_ACTION_ERR_NONE;
}

static void task_cleaned_up(Req_completion* rc, void* cb_arg) {
    if (rc->status != REQ_ERR_NONE) {
        err_exit("[job.c: task_cleaned_up()] req error, req status: %s\n"
        "req: %s, resp: %s", req_status_str(rc->status), rc->request_msg, rc->respose_msg);
    }

    // Map_task_info* mti = (Map_task_info*)cb_arg;
    Job* job = (Job*)cb_arg;
    const char* msg = rc->respose_msg;
    if (strncmp(msg, J_TASK_CLEANED_UP, strlen(J_TASK_CLEANED_UP)) == 0) {
        msg += strlen(J_TASK_CLEANED_UP);
        char jid[NAME_MAX], tid[NAME_MAX];
        sscanf(msg, " %s %s\n", jid, tid);

        Task_info* ti = hash_table_get(job->task_id_map, tid);
        assert(ti != NULL);

        Job_task_cleaned_up_event* jtcue = job_task_cleaned_up_event_create(job, ti);
        job_ev_handler_handle(job->jev_handler, (Job_event*)jtcue);
    } else {
        err_exit("[job.c: task_cleaned_up()] Unknown resp msg: %s", msg);
    }
}

static sm_action_result_t job_done_action(State_machine* sm, void* event_data) { 
    Job_done_event* jde = (Job_done_event*)event_data;
    Job* job = jde->jev.job;

    printf("[master[job(%s)]] Running job done action...\n", job->name);

    char msg[MP_MAXMSGLEN];
    Map_task_info* mti;
    TAILQ_FOREACH(mti, &job->map_task_infos, mti_link) {
        // J_TASK_CLEANUP <job_id> <task_id>
        sprintf(msg, "%s %s %s\n", J_TASK_CLEANUP, job->id, mti->ti.tid);
        mp_client_send_request(worker_conn_get_client(mti->isplit->location), msg, task_cleaned_up, job);

    }
    Reduce_task_info* rti;
    FILE* of = fopen(job->output_metadata_path, "a");
    if (of == NULL) {
        err_exit_sys("[job.c: job_done_action()] fopen() error");
    }
    TAILQ_FOREACH(rti, &job->reduce_task_infos, rti_link) {
        fprintf(of, "%s:%s\n", rti->outputfile_path, worker_conn_get_name(rti->location));
        // write_output_metadata(job, rti);
        // J_TASK_CLEANUP <job_id> <task_id>
        sprintf(msg, "%s %s %s\n", J_TASK_CLEANUP, job->id, rti->ti.tid);
        mp_client_send_request(worker_conn_get_client(rti->location), msg, task_cleaned_up, job);
    }
    fclose(of);

    // pthread_mutex_lock(&job->lock);
    // job->done = true;
    job->failed = jde->failed;
    // pthread_cond_signal(&job->cv);
    // pthread_mutex_unlock(&job->lock);

    return SM_ACTION_ERR_NONE;
}

static sm_action_result_t job_task_cleaned_up_action(State_machine* sm, void* event_data) { 
    Job_task_cleaned_up_event* jtcue = (Job_task_cleaned_up_event*)event_data;
    Job* job = jtcue->jev.job;
    printf("[master[job(%s)]] (%s) Running job cleaned up action...\n", job->name, jtcue->ti->tid);

    job->n_cleaned_up_task++;
    if (job->n_cleaned_up_task == job->n_map_tasks + job->n_reduce_tasks) {
        pthread_mutex_lock(&job->lock);
        job->done = true;
        // job->failed = jde->failed;
        pthread_cond_signal(&job->cv);
        pthread_mutex_unlock(&job->lock);
    }

    return SM_ACTION_ERR_NONE;
}
