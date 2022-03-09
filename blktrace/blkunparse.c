//
// Created by Omkar Desai on 9/8/21.
//
/*
Each blocktrace record contains the following fields

[Device Major Number,Device Minor Number] [CPU Core ID] [Record ID] [Timestamp (in nanoseconds)]
[ProcessID] [Trace Action] [OperationType] [SectorNumber + I/O Size] [ProcessName]
*/
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <errno.h>
#include <signal.h>
#include <locale.h>
#include <libgen.h>
#include <time.h>

#include "blktrace.h"
#include "rbtree.h"
#include "blktrace_api.h"

static char blkunparse_version[] = "2.0";
int data_is_native = -1;
int sequence = 0; //CHANGE FOR systor

struct per_dev_info {
    dev_t dev;
    char *name;

    int backwards;
    unsigned long long events;
    unsigned long long first_reported_time;
    unsigned long long last_reported_time;
    unsigned long long last_read_time;
    struct io_stats io_stats;
    unsigned long skips;
    unsigned long long seq_skips;
    unsigned int max_depth[2];
    unsigned int cur_depth[2];

    struct rb_root rb_track;

    int nfiles;
    int ncpus;

    unsigned long *cpu_map;
    unsigned int cpu_map_max;

    struct per_cpu_info *cpus;
};

struct ms_stream {
    struct ms_stream *next;
    struct trace *first, *last;
    struct per_dev_info *pdi;
    unsigned int cpu;
};

#define MS_HASH(d, c) ((MAJOR(d) & 0xff) ^ (MINOR(d) & 0xff) ^ (cpu & 0xff))

struct ms_stream *ms_head;
struct ms_stream *ms_hash[256];

/*
 * some duplicated effort here, we can unify this hash and the ppi hash later
 */
struct process_pid_map {
    pid_t pid;
    char comm[16];
    struct process_pid_map *hash_next, *list_next;
};

#define PPM_HASH_SHIFT	(8)
#define PPM_HASH_SIZE	(1 << PPM_HASH_SHIFT)
#define PPM_HASH_MASK	(PPM_HASH_SIZE - 1)
static struct process_pid_map *ppm_hash_table[PPM_HASH_SIZE];

struct per_process_info {
    struct process_pid_map *ppm;
    struct io_stats io_stats;
    struct per_process_info *hash_next, *list_next;
    int more_than_one;

    /*
     * individual io stats
     */
    unsigned long long longest_allocation_wait[2];
    unsigned long long longest_dispatch_wait[2];
    unsigned long long longest_completion_wait[2];
};

static struct rb_root rb_sort_root;
static unsigned long rb_sort_entries;

#define PPI_HASH_SHIFT	(8)
#define PPI_HASH_SIZE	(1 << PPI_HASH_SHIFT)
#define PPI_HASH_MASK	(PPI_HASH_SIZE - 1)
static struct per_process_info *ppi_hash_table[PPI_HASH_SIZE];
static struct per_process_info *ppi_list;
static int ppi_list_entries;

//just copied over form blkparse. may need changes


static int ndevices = 0; //static config for now.
static struct per_dev_info *devices; //how to fill this list of devices?

static char *input_dir;

//static FILE *dump_fp;
static FILE *ip_fp;
static char *dump_binary_dir;
static char *ip_fstr;
static char *dev_size; //2000000000(sectors)*512(bytes in each sector) = 1TB

FILE * btrace_fp;
char * line = NULL;
size_t len = 0;

int num_cpus;

__u64 unparse_genesis_time;

#define is_done()	(*(volatile int *)(&done))
static volatile int done;

static int resize_devices(char *name)
{
    int size = (ndevices + 1) * sizeof(struct per_dev_info);

    devices = realloc(devices, size);
    if (!devices) {
        fprintf(stderr, "Out of memory, device %s (%d)\n", name, size);
        return 1;
    }
    memset(&devices[ndevices], 0, sizeof(struct per_dev_info));
    devices[ndevices].name = name;
    ndevices++;
    return 0;
}

static struct per_dev_info *get_dev_info(dev_t dev)
{
    struct per_dev_info *pdi;
    int i;

    for (i = 0; i < ndevices; i++) {
        if (!devices[i].dev)
            devices[i].dev = dev;
        if (devices[i].dev == dev)
            return &devices[i];
    }

    if (resize_devices(NULL))
        return NULL;

    pdi = &devices[ndevices - 1];
    pdi->dev = dev;
    pdi->first_reported_time = 0;
    pdi->last_read_time = 0;

    return pdi;
}

static struct ms_stream *ms_alloc(struct per_dev_info *pdi, int cpu)
{
    struct ms_stream *msp = malloc(sizeof(*msp));

    msp->next = NULL;
    msp->first = msp->last = NULL;
    msp->pdi = pdi;
    msp->cpu = cpu;

    //if (ms_prime(msp))
    //    ms_sort(msp);

    return msp;
}

static void resize_cpu_info(struct per_dev_info *pdi, int cpu)
{
    struct per_cpu_info *cpus = pdi->cpus;
    int ncpus = pdi->ncpus;
    int new_count = cpu + 1;
    int new_space, size;
    char *new_start;

    size = new_count * sizeof(struct per_cpu_info);
    cpus = realloc(cpus, size);
    if (!cpus) {
        char name[20];
        //fprintf(stderr, "Out of memory, CPU info for device %s (%d)\n",
        //        get_dev_name(pdi, name, sizeof(name)), size);
        fprintf(stderr, "Out of memory");
        exit(1);
    }

    new_start = (char *)cpus + (ncpus * sizeof(struct per_cpu_info));
    new_space = (new_count - ncpus) * sizeof(struct per_cpu_info);
    memset(new_start, 0, new_space);

    pdi->ncpus = new_count;
    pdi->cpus = cpus;

    for (new_count = 0; new_count < pdi->ncpus; new_count++) {
        struct per_cpu_info *pci = &pdi->cpus[new_count];

        if (!pci->fd) {
            pci->fd = -1;
            memset(&pci->rb_last, 0, sizeof(pci->rb_last));
            pci->rb_last_entries = 0;
            pci->last_sequence = -1;
        }
    }
}


static struct option l_opts[] = {
        {
                .name = "input",
                .has_arg = required_argument,
                .flag = NULL,
                .val = 'i'
        },
        {
                .name = "version",
                .has_arg = no_argument,
                .flag = NULL,
                .val = 'V'
        },
        {
                .name = NULL,
        }
};

static int name_fixup(char *name)
{
    char *b;

    if (!name)
        return 1;

    b = strstr(name, ".blkparse.");
    if (b)
        *b = '\0';

    return 0;
}

static struct per_cpu_info *get_cpu_info(struct per_dev_info *pdi, int cpu){
    struct per_cpu_info *pci;

    if (cpu >= pdi->ncpus)
        resize_cpu_info(pdi, cpu);

    pci = &pdi->cpus[cpu];
    pci->cpu = cpu;
    return pci;
}

static int setup_out_file(struct per_dev_info *pdi, int cpu){
    int len = 0;
    char *dname, *p;
    struct per_cpu_info *pci = get_cpu_info(pdi, cpu);

    pci->cpu = cpu;
    pci->fdblock = -1;

    p = strdup(pdi->name);
    dname = dirname(p);

    if (strcmp(dname, ".")) {
        input_dir = dname;
        p = strdup(pdi->name);
        strcpy(pdi->name, basename(p));
    }
    free(p);

    if (input_dir)
        len = sprintf(pci->fname, "%s/", input_dir);

    snprintf(pci->fname + len, sizeof(pci->fname)-1-len,
             "%s.blktrace.%d", pdi->name, pci->cpu);

    pci->fd = open(pci->fname, O_RDWR | O_CREAT, 0644);
    if (pci->fd < 0) {
        perror(pci->fname);
        return 0;
    }

    printf("Output file %s added\n", pci->fname);

    //cpu_mark_online(pdi, pci->cpu);

    pdi->nfiles++;
    ms_alloc(pdi, pci->cpu);

    return 1;

}
/*
static int do_btrace_file(void){
    // name_fixup();
    //if (ret)
    //    return ret;
    int i, cpu;
    struct per_dev_info *pdi;
    for (i = 0; i< ndevices; i++){
        pdi = &devices[i];
        for(cpu = 0; setup_out_file(pdi, cpu); cpu++);
    }
    char * log_line;
    size_t len = 0;
    while (getline(&log_line, &len, ip_fp) != -1) {
        printf("%s", log_line);
    }
    return 0;
}
*/
#define S_OPTS  "a:A:b:D:d:f:F:hi:o:Oqstw:vVM"
static char usage_str[] =    "\n\n" \
	"-i <file>              | --input=<file>\n" \
	"-d <dir_path>          | --binary_dump=<dir_path>\n" \
	"-s <# of 512B sectors> | --size=<# of 512B sectors>\n"\
	"[ -V                | --version ]\n\n" \
	"\t-i Input file containing trace data, or '-' for stdin\n" \
	"\t-V Print program version info\n\n";

static void usage(char *prog){

    fprintf(stderr, "Usage: %s %s", prog, usage_str);
}

static void handle_sigint(__attribute__((__unused__)) int sig)
{
    done = 1;
}

void process_bdiq(struct blk_io_trace* bio_, char* tok[]){
    //CHANGE FOR unified
    __u64 sector = atof(tok[6])/512;//%390625000;
    //__u64 sector = (atof(tok[6])/512); //for ms_enterprise traces use this
    //sector = sector%390625000;
    sector = sector%atoi(dev_size);
    //sector = sector%partition_size;
    int bytes = atoi(tok[5]);
    //printf("%d\n", bytes);
    //printf("%d\n", (int)sector);
    bio_->sector = (__u64) sector;
    bio_->bytes = bytes;
}

void process_a(struct blk_io_trace* bio_, char* tok[]){
    int sector = atoi(tok[7])%atoi(dev_size);
    int bytes = atoi(tok[9])*512;
    bio_->sector = (__u64) sector;
    bio_->bytes = bytes;
}


void process_c(struct blk_io_trace* bio_, char* tok[]){
    int sector = atoi(tok[7])%atoi(dev_size);
    int bytes = atoi(tok[9])*512;
    bio_->sector = (__u64) sector;
    bio_->bytes = bytes;
}

void process_fgms(struct blk_io_trace* bio_, char* tok[]){
    int sector = atoi(tok[7])%atoi(dev_size);
    int bytes = atoi(tok[9])*512;
    bio_->sector = (__u64) sector;
    bio_->bytes = bytes;
}

void get_rwbs(struct blk_io_trace* bio_, char* tok[]){
    char *rwbs = tok[4];//CHANGE FOR unified
    //printf("len = %lu\n", strlen(rwbs));
    //printf("rwbs_str = %s\n", rwbs);
    for(int i =0; i<strlen(rwbs); i++){
        //printf("this rwbs char= %c\n", rwbs[i]);
        if(rwbs[i] == 'W')
            bio_->action |= BLK_TC_ACT(BLK_TC_WRITE);
        else if(rwbs[i] == 'R')
            bio_->action |= BLK_TC_ACT(BLK_TC_READ);
        else if(rwbs[i] == 'D')
            bio_->action |= BLK_TC_ACT(BLK_TC_DISCARD);
        else if(rwbs[i] == 'F')
            bio_->action |= BLK_TC_ACT(BLK_TC_FLUSH);
        else if(rwbs[i] == 'S')
            bio_->action |= BLK_TC_ACT(BLK_TC_SYNC);
        else if(rwbs[i] == 'N')
            bio_->action |= BLK_TC_ACT(BLK_TC_NOTIFY);
        else if(rwbs[i] == 'M')
            bio_->action |= BLK_TC_ACT(BLK_TC_META);
    }

}

void get_action_code(struct blk_io_trace* bio_, char* tok[]){
    //char *act = tok[4];
    char *act = "Q";//CHANGE FOR systor
    unsigned long act_len = strlen(act);

    //printf("Action code is %s\n", act);
    for(int i =0; i<strlen(act); i++) {
        if (act[i] == 'Q') {
            //printf("case Q");
            bio_->action = BLK_TA_QUEUE; //0001|1010 latency and resp time
            //bio_->sector = (__u64)atoi(tok[7]);
            //bio_->bytes = atoi(tok[9])*512;
            //printf("%s\n", tok[10]);
            process_bdiq(bio_, tok);
            //break;
        } else if (act[i] == 'I') {
            //printf("case I");
            bio_->action = BLK_TA_INSERT;
            process_bdiq(bio_, tok);
            //break;
        } else if (act[i] == 'M') {
            //printf("case M");
            bio_->action = BLK_TA_BACKMERGE;
            process_fgms(bio_, tok);
            //break;
        } else if (act[i] == 'F') {
            //printf("case F");
            bio_->action = BLK_TA_FRONTMERGE;
            process_fgms(bio_, tok);
            //break;
        } else if (act[i] == 'G') {
            //printf("case G");
            bio_->action = BLK_TA_GETRQ;
            process_fgms(bio_, tok);
            //break;
        } else if (act[i] == 'S') {
            //printf("case S");
            bio_->action = BLK_TA_SLEEPRQ;
            process_fgms(bio_, tok);
            //break;
        } else if (act[i] == 'R') {
            //printf("case R");
            bio_->action = BLK_TA_REQUEUE;
            //process_q(bio_, tok); //new
            //break;
        } else if (act[i] == 'D') {
            //this is probably the one
            //printf("case D");
            bio_->action = BLK_TA_ISSUE;
            process_bdiq(bio_, tok);
            //break;
        } else if (act[i] == 'C') {
            //printf("case C");
            bio_->action = BLK_TA_COMPLETE;
            process_c(bio_, tok);
            //break;
        } else if (act[i] == 'P') {
            //printf("case P");
            bio_->action = BLK_TA_PLUG;
            //process_q(bio_, tok); //new
            //break;
        } else if (act[i] == 'U') {
            //printf("case U");
            bio_->action = BLK_TA_UNPLUG_IO;
            //process_q(bio_, tok); //new
            //break;
        } else if (act[i] == 'T') {
            //printf("case UT");
            bio_->action = BLK_TA_UNPLUG_TIMER;
            //process_q(bio_, tok); //new
            //break;
        } else if (act[i] == 'X') {
            //printf("case X");
            bio_->action = BLK_TA_SPLIT;
            //process_q(bio_, tok); //new
            //break;
        } else if (act[i] == 'B') {
            //printf("case B");
            bio_->action = BLK_TA_BOUNCE;
            process_bdiq(bio_, tok);
            //break;
        } else if (act[i] == 'A') {
            //printf("case A");
            bio_->action = BLK_TA_REMAP;
            //bio_->sector = atoi(tok[7]);
            //bio_->bytes = atoi(tok[9])*512;
            process_a(bio_, tok); //new
            //break;
        } else {
            fprintf(stderr, "Bad fs action code %s\n", act);
            //fprintf(stderr, "Bad fs action %c\n", act);
            //break;
        }
    }
}

void get_device_code(struct blk_io_trace* bio_, char* tok){
    bio_->device = 259 << 20 | 0;
}
struct blk_io_trace get_bit(char * tok[]){
    struct blk_io_trace bio_;
    //printf("%s %s %s %s %s %s %s\n", tok[0], tok[1], tok[2],tok[3],tok[4],tok[5], tok[6]);

    sequence++;// = atoi(tok[2]);

    //char * time_parts;
    char *delim = ".";
    int i=0;
    char *time_arr[2];
    //time_parts = strtok(tok[1], delim);//CHANGE FOR systor
    //while(time_parts != NULL) {
    //    time_arr[i] = time_parts;
    //    i++;
    //    time_parts = strtok(NULL, delim);
    //}
    //__u64 time = unparse_genesis_time + ((atoi(time_arr[0])*1000000000)+atoi(time_arr[1]));
    //printf("%f\n", atof(tok[1])/1000000.0);
    __u64 time = ((atof(tok[1])/1000000.0)*1000000000.0);//+atoi(time_arr[1]);
    //printf("%s.%s\t %f\n", time_arr[0], time_arr[1], atoi(time_arr[0])*1000000000.0);

    //unsigned long time = atoi(tok[3]);
    int cpu = sequence % (num_cpus);//CHANGE FOR unified

    int pid=1234;
    if (strcmp(tok[3], "****")!=0){
        pid = atoi(tok[3]);
    }

    bio_.magic = BLK_IO_TRACE_MAGIC | BLK_IO_TRACE_VERSION;
    bio_.sequence = (__u32) sequence;
    //unsigned long i_time = unparse_genesis_time+time;
    //bio_.time = (__u64) time;
    bio_.time = time;
    bio_.cpu = (__u32) cpu;
    bio_.pid = (__u32) pid;
    __u16 error_status = 0;
    __u16 pdu_len = 0;
    bio_.error = error_status;
    bio_.pdu_len = pdu_len;
    char * a = "0";
    get_device_code(&bio_, a);//CHANGE FOR unified
    get_action_code(&bio_, tok);
    get_rwbs(&bio_, tok);
    return bio_;
}

static int handle(void){
    ssize_t read;
    char * t;
    char *delim = ",";//CHANGE FOR unified_traces
    char *token;
    while((read = getline(&line, &len, ip_fp)) != -1){
        t = line;
        char *tokens[20];
        int i=0;
        struct per_dev_info * device_ptr;
        struct per_cpu_info * cpu_ptr;
        token = strtok(t, delim);
        while(token != NULL) {
            if (strcmp(token, "****")==0){
                token="";
            }
            tokens[i] = token;
            i++;
            token = strtok(NULL, delim);
        }
        //for (int l=0;l<7;l++){
        //    printf("%s,", tokens[l]);
        //}
        //printf("\n");
        //printf("%s ", tokens[0]);
        //printf("%s, %s, %s, %s, %s, %s, %s\n", tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[5], tokens[6]);
        if (strcmp(tokens[0],"Timestamp") != 0) {
            struct blk_io_trace processed_bit = get_bit(tokens);
            device_ptr = &devices[0];
            cpu_ptr = get_cpu_info(device_ptr, (processed_bit.cpu % 8));

            write(cpu_ptr->fd, &processed_bit, sizeof(struct blk_io_trace));
        }

    }
    return 0;
}

static int setup_out_files(void){
    int i, cpu;
    struct per_dev_info *pdi;
    num_cpus = get_nprocs();
    //static int ncpus = sysconf(_SC_NPROCESSORS_ONLN);
    for (i = 0; i < ndevices; i++) {
        pdi = &devices[i];

        for (cpu = 0; cpu < num_cpus; cpu++)
            setup_out_file(pdi, cpu);
    }
    return 1;
}

int main(int argc, char *argv[]){
    int c, ret;

    while ((c = getopt_long(argc, argv, S_OPTS, l_opts, NULL)) != -1) {
        switch (c) {
            case 'i':
                ip_fstr = optarg;
                if(resize_devices(optarg)!=0){
                    return 1;
                }
                break;

            case 'd':
                dump_binary_dir = optarg;
                break;
            case 'b':
                //printf("partition size %s", argv[3]);
                dev_size = optarg;
                break;
            case 'V':
                printf("%s version %s\n", argv[0], blkunparse_version);
                return 0;
            default:
                usage(argv[0]);
                return 1;
        }
    }

    //memset(&rb_sort_root, 0, sizeof(rb_sort_root));

    signal(SIGINT, handle_sigint);
    signal(SIGHUP, handle_sigint);
    signal(SIGTERM, handle_sigint);

    setlocale(LC_NUMERIC, "en_US");

    if(ip_fstr){
        printf("%s\n", ip_fstr);
        ip_fp = fopen(ip_fstr, "r");
        if(!ip_fp){
            perror(ip_fstr);
            ip_fstr = NULL;
            return 1;
        }
    }

    //resize_devices(ip_fstr);

    ret = setup_out_files();
    if (!ret){
        perror("output file creation error\n");
        return ret;
    }
    //unparse_genesis_time = time(NULL);
    unparse_genesis_time = 0.0;
    //unparse_genesis_time = unparse_genesis_time*1000000000;
    ret = handle();

    return ret;
}
