#include <stdio.h>
#include <netinet/in.h>
#include <string.h>
#include <stdlib.h>
// #include <sys/queue.h>
#include <unistd.h>
#include <assert.h>

#include "master.h"
#include "msg_pass/mp_client.h"
#include "msg_pass/mp_server.h"
#include "utils/my_err.h"
#include "ep_engine/epoll_event_engine.h"
// #include "event_engine.h"
// #include "worker_node.h"
// #include "job_event_handler.h"
#include "job.h"
// #include "job_event.h"
// #include "job_msg.h"
#include "utils_ds/hash_table.h"
// #include "worker_conn_struct.h"
// #include "priority_queue.h"
#include "utils_ds/rb_tree.h"
#include "MR_msg_type.h"
#include "intermediate_file_sturct.h"
#include "utils_ds/deque.h"
// #include "event_engine.h"
// #include "epoll_engine.h"

// #define WORKERNODES_FILE "workers"
// #define MAX_N_WORKERNODES 4
#define N_ENGINE_THREADS 4
#define LINE_MAX 512

#define MASTER_PORT 30030
#define WORKER_PORT 30031

struct Worker_conn {
    Mp_client* client;
    char name[NAME_MAX];
    char ip[INET_ADDRSTRLEN];

    int n_assigned_tasks;
    /*
        These objects should be created per job.
        But for now, we use a single job
    */
    Deque* assigned_map_tasks;
    Deque* assigned_reduce_tasks;

    void* data;
    TAILQ_ENTRY(Worker_conn) wc_link;
};

const char* WORKER_NODES_FILE;

struct Master_context {
    RB_tree* worker_conns;
    Worker_conn_list worker_list;

    Hash_table* name_worker_map;
    Hash_table* ip_worker_map;
    int n_worker_conns;
    int n_inited_conns;

    Job* job;
    // /*
    //     These objects should be created per job.
    //     But for now, we use a single job
    // */
    // Deque* assigned_map_tasks;
    // Deque* assigned_reduce_tasks;

    pthread_mutex_t lock;
    pthread_cond_t cv;
};

Master_context g_master_ctx;

Worker_conn* master_ctx_get_min_assigned_task_worker(Master_context* mctx) {
    RBT_iterator* it = rbt_first(mctx->worker_conns);
    assert(it != NULL);

    return (Worker_conn*)it->key;
}

Worker_conn* master_ctx_get_worker_conn(Master_context* mctx, const char* name) {
    Worker_conn* wc = (Worker_conn*)hash_table_get(g_master_ctx.name_worker_map, name);
    assert(wc != NULL);

    return wc;
}

void master_ctx_print_assigned_tasks(Master_context* mctx) {
    Worker_conn* wc;
    printf("assigned tasks...\n");
    TAILQ_FOREACH(wc, &g_master_ctx.worker_list, wc_link) {
        printf("[[%s]] ", wc->name);
        if (!deque_empty(wc->assigned_map_tasks)) {
            Deque_iterator* dit = deque_front(wc->assigned_map_tasks);
            Map_task_info* mti;
            while (dit != NULL) {
                mti = dit->elm;
                printf("MAPTASK(%s) ", mti->ti.tid);
                dit = deque_next(wc->assigned_map_tasks, dit);
            }
        }

        if (!deque_empty(wc->assigned_reduce_tasks)) {
            Deque_iterator* dit = deque_front(wc->assigned_reduce_tasks);
            Reduce_task_info* rti;
            while (dit != NULL) {
                rti = dit->elm;
                printf("REDUCETASK(%s) ", rti->ti.tid);
                dit = deque_next(wc->assigned_reduce_tasks, dit);
            }
        }
        printf("\n");
    }
    printf("\n");
}

Worker_conn_list* master_ctx_get_worker_list(Master_context* mctx) {
    return &mctx->worker_list;
}

Worker_conn* worker_conn_first(Worker_conn_list* wcl) {
    return TAILQ_FIRST(wcl);
}

Worker_conn* worker_conn_next(Worker_conn* wc) {
    return TAILQ_NEXT(wc, wc_link);
}

void worker_conn_assign_task(Worker_conn* wc, Task_info* ti) {
    RBT_iterator* it = (RBT_iterator*)wc->data;
    rbt_delete_iterator(g_master_ctx.worker_conns, it);
    wc->n_assigned_tasks++;
    if (ti->type == MAP_TASK_INFO)
        deque_push_back(wc->assigned_map_tasks, ti);
    else
        deque_push_back(wc->assigned_reduce_tasks, ti);

    if ((it = rbt_insert(g_master_ctx.worker_conns, (void*)wc)) == NULL) {
        err_exit("[master.c: master_ctx_worker_incr_assigned_task()] rbt_insert(wc) error");
    }
    wc->data = (void*)it;
}

Mp_client* worker_conn_get_client(Worker_conn* wc) {
    return wc->client;
}

const char* worker_conn_get_name(Worker_conn* wc) {
    return wc->name;
}

static bool is_metadata_file(const char* filename) {
    const char* suffix = ".meta";

    if (filename == NULL) {
        return false;
    }

    size_t filename_len = strlen(filename);
    size_t suffix_len = strlen(suffix);
    if (suffix_len > filename_len) {
        return false;
    }

    return strncmp(filename + filename_len - suffix_len, suffix, suffix_len) == 0;
}

static void conn_inited(Req_completion* rc, void* cb_arg) {
    if (rc->status != REQ_ERR_NONE) {
        err_exit("[master.c: conn_inited()] req error, req status: %s\n"
        "req: %s, resp: %s", req_status_str(rc->status), rc->request_msg, rc->respose_msg);
    }

    const char* msg = rc->respose_msg;
    if (strncmp(msg, CONN_INITED, strlen(CONN_INITED)) == 0) {
        pthread_mutex_lock(&g_master_ctx.lock);
        g_master_ctx.n_inited_conns++;
        pthread_cond_signal(&g_master_ctx.cv);
        pthread_mutex_unlock(&g_master_ctx.lock);
    } else {
        err_exit("[master.c: conn_inited()] Unknown resp msg: %s", msg);
    }
}

static void send_init_conn_msgs() {
    RBT_iterator* it = rbt_first(g_master_ctx.worker_conns);
    Worker_conn* wc;
    while (it != NULL) {
        wc = it->key;
        char msg[MP_MAXMSGLEN];
        sprintf(msg, "%s %d\n", INIT_CONN, MASTER_PORT);
        mp_client_send_request(wc->client, msg, conn_inited, NULL);

        it = rbt_iterator_next(g_master_ctx.worker_conns, it);
    }
}

static void wait_conns_inited() {
    pthread_mutex_lock(&g_master_ctx.lock);
    while (g_master_ctx.n_inited_conns < g_master_ctx.n_worker_conns) {
        pthread_cond_wait(&g_master_ctx.cv, &g_master_ctx.lock);
    }
    pthread_mutex_unlock(&g_master_ctx.lock);
}
/*
    connect to all workers
*/
static void connect_workers(EP_engine* engine) {
    // open worker node list file
    // FILE* wfp = fopen(WORKERNODES_FILE, "r");
    FILE* wfp = fopen(WORKER_NODES_FILE, "r");
    if (wfp == NULL) {
        err_exit_sys("[master.c: connect_workers()] fopen error");
    }

    char line[LINE_MAX];
    while (fgets(line, sizeof(line), wfp) != NULL) {
        Worker_conn* wc = (Worker_conn*)malloc(sizeof(*wc));
        if (wc == NULL) {
            err_exit_sys("[master.c: connect_workers()] malloc error");
        }

        sscanf(line, "%[^:]:%s\n", wc->name, wc->ip);
        Mp_client* client = mp_client_create(wc->ip, WORKER_PORT, engine);
        if (client == NULL) {
            err_exit("[master.c: connect_workers()] mp_client_create() error");
        }
        wc->client = client;
        wc->n_assigned_tasks = 0;

        // TAILQ_INSERT_TAIL(&g_wc_list, wc, wc_link);
        TAILQ_INSERT_TAIL(&g_master_ctx.worker_list, wc, wc_link);
        printf("[[master]] Putting connected worker(%s) in name-worker map\n", wc->name);
        if (!hash_table_put(g_master_ctx.name_worker_map, wc->name, wc)) {
            err_exit("[master.c: connect_workers()] hash_table_put() error");
        }
        RBT_iterator* it = rbt_insert(g_master_ctx.worker_conns, (void*)wc);
        if (it == NULL) {
            err_exit("[master.c: connect_workers()] rbt_insert() error");
        }

        wc->assigned_map_tasks = deque_create();
        if (wc->assigned_map_tasks == NULL) {
            err_exit("[master.c: connect_workers()] deque_create(assigned_map_tasks) error");
        }
        wc->assigned_reduce_tasks = deque_create();
        if (wc->assigned_reduce_tasks == NULL) {
            err_exit("[master.c: connect_workers()] deque_create(assigned_reduce_tasks) error");
        }

        wc->data = (void*)it;

        g_master_ctx.n_worker_conns++;
    }
    if (ferror(wfp)) {
        err_exit_sys("[master.c: connect_workers()] fgets() error");
    }
    fclose(wfp);

    send_init_conn_msgs();
    wait_conns_inited();
}


static int cmp_n_tasks(const void* lwc, const void* rwc) {
    Worker_conn* l = (Worker_conn*)lwc;
    Worker_conn* r = (Worker_conn*)rwc;

    if (l->n_assigned_tasks == r->n_assigned_tasks) {
        return 0;
    } else if (l->n_assigned_tasks < r->n_assigned_tasks) {
        return -1;
    } else {
        return 1;
    }
}

void master_context_init() {
    g_master_ctx.worker_conns = rbt_create(cmp_n_tasks);
    if (g_master_ctx.worker_conns == NULL) {
        err_exit("[master.c: master_context_init()] rbt_create() error");
    }
    TAILQ_INIT(&g_master_ctx.worker_list);

    g_master_ctx.name_worker_map = hash_table_create();
    if (g_master_ctx.name_worker_map == NULL) {
        err_exit("[master.c: master_context_init()] hash_table_create() error");
    }

    g_master_ctx.n_worker_conns = 0;
    g_master_ctx.n_inited_conns = 0;

    g_master_ctx.job = NULL;

    pthread_mutex_init(&g_master_ctx.lock, NULL);
    pthread_cond_init(&g_master_ctx.cv, NULL);
}

static void* ep_engine_event_loop_thread(void* arg) {
    EP_engine* engine = (EP_engine*)arg;
    int err = ep_engine_start_event_loop(engine);
    if (err < 0) {
        err_exit("[master.c ep_engine_event_loop_thread()] ep_engine_start_event_loop() error");
    }

    return NULL;
}

static void handle_mapoutput(const char* msg_ptr, Mp_srv_request* req) {
    // J_MAPOUTPUT <jid> <tid> <outputfile name> <metadata size> <location>
    char job_id[NAME_MAX], tid[NAME_MAX];
    Intermediate_file* intf = (Intermediate_file*)malloc(sizeof(*intf));
    if (intf == NULL) {
        err_exit("[master.c: handle_mapoutput()] malloc() error");
    }
    sscanf(msg_ptr, " %s %s %s %d %s\n", job_id, tid, intf->file_name, 
                                &intf->metadata_size, intf->worker_name);

    Task_info* ti = job_get_task(g_master_ctx.job, tid);
    assert(ti != NULL);

    Job_mapoutput_event* jme = job_mapoutput_event_create(g_master_ctx.job, ti, intf);
    if (jme == NULL) {
        err_exit("[master.c: handle_mapoutput()] job_mapoutput_event_create() error");
    }

    job_ev_handler_handle(g_master_ctx.job->jev_handler, (Job_event*)jme);

    mp_server_request_done(req, NULL);
}

static void handle_task_done(const char* msg_ptr, Mp_srv_request* req) {
    // J_TASK_DONE <job_id> <task id> <t failed> [outputfile_path]
    char job_id[NAME_MAX], tid[NAME_MAX];
    int t_failed;
    // sscanf(msg_ptr, " %s %s %d\n", job_id, tid, &t_failed);
    int n_read;
    sscanf(msg_ptr, " %s %s%n", job_id, tid, &n_read);
    msg_ptr += n_read;

    Task_info* ti = job_get_task(g_master_ctx.job, tid);
    assert(ti != NULL);
    if (ti->type == MAP_TASK_INFO) {
        sscanf(msg_ptr, " %d\n", &t_failed);
    } else {
        sscanf(msg_ptr, " %d %s\n", &t_failed, ((Reduce_task_info*)ti)->outputfile_path);
    }

    if (!t_failed) {
        Job_task_succeeded_event* jtse = job_task_succeeded_event_create(g_master_ctx.job, ti);
        if (jtse == NULL) {
            err_exit("[master.c: handle_task_done()] job_task_succeeded_event_create() error");
        }
        job_ev_handler_handle(g_master_ctx.job->jev_handler, (Job_event*)jtse); 
    } else {
        Job_task_failed_event* jtfe = job_task_failed_event_create(g_master_ctx.job, ti);
        if (jtfe == NULL) {
            err_exit("[master.c: handle_task_done()] job_task_failed_event_create() error");
        }
        job_ev_handler_handle(g_master_ctx.job->jev_handler, (Job_event*)jtfe); 
    }

    mp_server_request_done(req, NULL);
}

static void handle_job_msg(Mp_srv_request* request, void* cb_arg) {
    const char* msg = request->msg;
    if (strncmp(msg, J_MAPOUTPUT, strlen(J_MAPOUTPUT)) == 0) {
        printf("[[master]] handling mapoutput msg: %s", msg);
        handle_mapoutput(msg+strlen(J_MAPOUTPUT), request);

    } else if (strncmp(msg, J_TASK_DONE, strlen(J_TASK_DONE)) == 0) {
        printf("[[master]] handling task done msg: %s", msg);
        handle_task_done(msg+strlen(J_TASK_DONE), request);

    } else {
        err_exit("[master.c: handle_job_msg()] Unknown task msg type (%s).", msg);
    }
}

static void start_job(Job* job) {
    printf("[[master]] starting job(%s)\n", job->name);

    Job_start_event* jse = job_start_event_create(job);
    job_ev_handler_handle(job->jev_handler, (Job_event*)jse);
}

static void wait_for_job_completion(Job* job) {
    pthread_mutex_lock(&job->lock);
    while (!job->done) {
        pthread_cond_wait(&job->cv, &job->lock);
    }
    pthread_mutex_unlock(&job->lock);
}

static void print_job_result(Job* job) {
    printf("\n==================================================\n");
    printf("[[master]]: Job <%s>'s result: \n"
            "Job status: %s\n"
            "Input metadata directory: %s\n"
            "Output metadata directory: %s\n"
            "Total # of inputsplits: %d\n"
            "# of Launched map tasks: %d\n"
            "# of Launched reduce tasks: %d\n"
            "# of Succeeded map tasks: %d\n"
            "# of Succeeded reduce tasks: %d\n"
            "# of Failed map tasks: %d\n"
            "# of Failed reduce tasks: %d\n",
            job->name, job->failed ? "FAILED" : "SUCCEEDED", job->input_metadata_path,
            job->output_metadata_path, job->n_input_splits, job->n_launched_map_task,
            job->n_launched_reduce_task, job->n_succeeded_map_task, job->n_succeeded_reduce_task,
            job->n_failed_map_task, job->n_failed_reduce_task);
    printf("Succeeded map tasks: ");
    if (!deque_empty(job->succeeded_map_task_infos)) {
        Deque_iterator* dit = deque_front(job->succeeded_map_task_infos);
        Task_info* ti;
        while (dit != NULL) {
            ti = dit->elm;
            assert(ti->type == MAP_TASK_INFO);
            printf("(%s) ", ti->tid);
            dit = deque_next(job->succeeded_map_task_infos, dit);
        }
    }
    printf("\n");
    printf("Failed map tasks: ");
    if (!deque_empty(job->failed_map_task_infos)) {
        Deque_iterator* dit = deque_front(job->failed_map_task_infos);
        Task_info* ti;
        while (dit != NULL) {
            ti = dit->elm;
            assert(ti->type == MAP_TASK_INFO);
            printf("(%s) ", ti->tid);
            dit = deque_next(job->failed_map_task_infos, dit);
        }
    }
    printf("\n");
    printf("Succeeded reduce tasks: ");
    if (!deque_empty(job->succeeded_reduce_task_infos)) {
        Deque_iterator* dit = deque_front(job->succeeded_reduce_task_infos);
        Task_info* ti;
        while (dit != NULL) {
            ti = dit->elm;
            assert(ti->type == REDUCE_TASK_INFO);
            printf("(%s) ", ti->tid);
            dit = deque_next(job->succeeded_reduce_task_infos, dit);
        }
    }
    printf("\n");
    printf("Failed reduce tasks: ");
    if (!deque_empty(job->failed_reduce_task_infos)) {
        Deque_iterator* dit = deque_front(job->failed_reduce_task_infos);
        Task_info* ti;
        while (dit != NULL) {
            ti = dit->elm;
            assert(ti->type == REDUCE_TASK_INFO);
            printf("(%s) ", ti->tid);
            dit = deque_next(job->failed_reduce_task_infos, dit);
        }
    }
    printf("\n");
    printf("==================================================\n\n");
}

int main(int argc, char** argv) {
    // Usage: prog <worker list> <job name> <input metadata file> <output metadata file>
    if (argc != 5) {
        err_exit("[master.c: main()] "
                 "usage: %s <worker list file> <job name> <input metadata file> <output metadata file> \n", argv[0]);
    }

    WORKER_NODES_FILE = argv[1];
    const char* job_name = argv[2];
    const char* input_metadata_file = argv[3];
    const char* output_metadata_file = argv[4];
    // check arguments
    if (!is_metadata_file(input_metadata_file)) {
        err_exit("[master.c: main()] %s is not metadata file", input_metadata_file);
    }
    if (!is_metadata_file(output_metadata_file)) {
        err_exit("[master.c: main()] %s is not metadata file", output_metadata_file);
    }
    if (access(input_metadata_file, F_OK) < 0) {
        err_exit("[master.c: main()] input metadata file: %s does not exist.", input_metadata_file);
    }
    if (access(output_metadata_file, F_OK) == 0) {
        err_exit("[master.c: main()] output metadata file: %s already exist.", output_metadata_file);
    }
    // input metadata format: <splitfile path>:<workername>


    EP_engine *engine = ep_engine_create(false, 0);
    if (engine == NULL) {
        err_exit("[master.c: main()] ep_engine_create() error");
    }

    pthread_t ev_loop_thr;
    int err = pthread_create(&ev_loop_thr, NULL, ep_engine_event_loop_thread, engine);
    if (err) {
        err_exit_errn(err, "[master.c: main()] pthread_create(ep_engine_event_loop_thread) error");
    }

    Mp_server* job_msg_listener = mp_server_create(MASTER_PORT, engine, handle_job_msg, NULL);

    master_context_init();

    connect_workers(engine);

    Job_ev_handler* handler = job_ev_handler_create();
    if (handler == NULL) {
        err_exit("[master.c: main()] job_ev_handler_create() error");
    }
    Job job;
    job_init(&job, job_name, input_metadata_file, output_metadata_file, handler, 
    g_master_ctx.n_worker_conns, &g_master_ctx);
    
    g_master_ctx.job = &job;

    start_job(&job);

    wait_for_job_completion(&job);

    print_job_result(&job);
    // cleanup();
}
