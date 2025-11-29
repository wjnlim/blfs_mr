# MapReduce Using Block-Level File Sharing over iSCSI
A lightweight MapReduce framework that uses shared iSCSI block devices to access intermediate data directly between worker nodes without a distributed file system.

## Overview
This research project presents an experimental MapReduce framework that leverages **shared iSCSI volumes** for direct access to intermediate data.
Unlike conventional MapReduce implementations (e.g., Hadoop, Spark), which rely on shuffle daemons and network-based data transfer services, this framework enables workers to **read and write intermediate data directly on shared block devices**.

This design:
* **Simplifies** the MapReduce architecture
* **Eliminates redundant data transfers** inherent in traditional shuffle mechanisms
* Enables **block-level data sharing** across nodes through iSCSI
* Removes the need for shuffle daemons or data transfer services
* Allows intermediate files to be accessed as if they were local files

A central focus of the research is resolving **cache inconsistency issues** that occur when multiple nodes access the same iSCSI volumes without a distributed file system.

## Documentation
For full details—including cache analysis, system design, experimental findings, and architectural diagrams—please refer to this 
**Technical Report:** [blfs_mr.pdf](docs/blfs_mr.pdf)

## Key Features
* **Asynchronous message-driven master-worker model**\
Built on my [msg_pass](https://github.com/wjnlim/msg_pass.git) library
* **iSCSI-based shared intermediate storage**\
Direct block-level data access across nodes without network-based data copying.
* **Simplified MapReduce workflow**\
No shuffle handler, fetcher, or auxiliary data transfer daemons.
* **Cache-Consistent Shared Access using:**
    * **Pre-allocated** intermediate files
    * Explicit **fsync()** on both file and block device
    * **O_DIRECT** reads on reducers to bypass page cache
* **Notes**
    * This project is mainly for an **experimental** MapReduce framework design.\
    Thus, the implementation may lack fault-tolerance, load balancing, proper error-handling, so please use it with caution.
    * This project uses my [msg_pass](https://github.com/wjnlim/msg_pass.git) library. The CMake file will automatically fetch the project internally
    * The blfs_mr library depends on pthread, so you must link with **-lpthread**.

## Core Concept: Pre-Allocated Files Approach
ISCSI provides no built-in cache coherence and most file systems do
not have built-in conflict resolution for shared access.
Thus, sharing iSCSI volumes among workers without a distributed file system creates cache inconsistency problems, where reducers do not detect new files created by mappers.
While explicit cache-dropping before every intermediate file opening fixes the issue, it is prohibitively expensive.

To address this, the framework adopts:
* **Pre-Allocated Files Strategy**
  * All intermediate files are created on each node’s local iSCSI target before that target disk is mounted by remote nodes.
  * Mappers are assigned pre-created intermediate files and simply overwrite them rather than creating new ones.
  * Mappers flush intermediate data using ```fsync(fd)``` and ```fsync(blkdev_fd)```
  to ensure durability.
  * Reducers open the files using O_DIRECT and read intermediate data bypassing page caches
  * No cache-dropping is required on reducer nodes
  * **Notes**
    * This experimental design was implemented based on an assumption that
      each node has enough pre-allocated file for tasks.
  
This ensures consistent shared access without needing a distributed file system and without imposing expensive cache invalidation operations.
## How the MapReduce framework works using intermediate file sharing
![Execution overview](images/Execution%20overview.png)

0. The ```run_mapred.sh``` script takes a user-provided MapReduce program—implementing the ```Mapper```, ```Reducer``` functions and calling ```MR_run_task()```—as input. It copies the MapReduce executable to all worker nodes and launches the ```master``` process.
1. The master assigns map tasks to workers where input splits are located and
assigns reduce tasks to workers with the smallest current task load.
2. Each worker spawns processes to run the MapReduce program. The program
invokes ```MR_run_task()``` to execute the assigned task. Map tasks run the user-defined ```Mapper``` function.
3. Mappers write intermediate data to pre-allocated files provided by their local worker processes and flush the data using both ```fsync(fd)``` and ```fsync(blkdev fd)```. Then they notify task completions to the master.
4. When reducers are notified by the master about these files, they open the files using the ```O_DIRECT``` flag and read the data directly from remote shared disks.
5.  After reading all intermediate data, reducers run the user-defined ```Reducer``` function and write final outputs to their local disks.

## Quick Start Guide
### Scripts and Configuration Files
```sharedfiles```: Contains a list of **\<filename\>:\<size\>** pairs, one per line, 
specifying the pre-allocated intermediate files.

```alloc_shared_files.sh```: Creates the shared intermediate files listed in ```sharedfiles``` on the device provided as input.

```set_env.sh```: Sets the ```BLFS_MR_HOME``` envrionment variable, which defines the project’s base directory.

```workers```: Contains **\<worker name\>:\<ip\>** pairs. Used by scripts for worker name ↔ IP resolution.

```mnt_targets.sh```: Logs into all worker nodes' iSCSI targets and mounts them under the ```$BLFS_MR_HOME/mnt``` directory.

```init_workers.sh```: Initializes worker nodes by creating directories for input splits(```$BLFS_MR_HOME/data/inputs```), output partitions(```$BLFS_MR_HOME/data/outputs```), and MapReduce executables(```$BLFS_MR_HOME/mapred_bin```).
It also runs the ```mnt_targets.sh``` on each node, and generates the ```workers_n_splits``` metadata file, which stores
**\<worker\>:\<num_of_input_split\>** pairs used for input split distribution.


```gen_wordcount_input.sh```: Generates a random input file for the WordCount MapReduce example.

```distr_input.sh```: Distributes an input file across the specified workers and produces a metadata file containing **\<file path\>:\<worker\>** mappings.

```rm_isplits.sh```: Deletes the metadata and input splits associated with a given input.

```rm_outputs.sh```: Deletes the metadata and partitions associated with a given output.

```print_output.sh```: Prints the final output by merging all output partitions.

```run_worker.sh```: Start a worker process.

```start_workers.sh```: Starts worker processes on all worker nodes.

```stop_workers.sh```: Stops worker processes on all worker nodes.

```run_mapred.sh```: Copies a user-provided MapReduce executable to all workers and launches the ```master``` program to execute the MapReduce workflow.

### WordCount Example
Example setup on a 4-node cluster (1 GB shared volumes, 32 MB input, 2 MB intermediate files).
#### Prerequisites
1. Prepare 4 nodes, for example:
    - master/worker0 (172.30.1.33) // this also runs the master process
    - worker1 (172.30.1.34)
    - worker2 (172.30.1.35)
    - worker3 (172.30.1.36)\
(The project assumes passwordless SSH from the master to all nodes.)
2. Prepare a dedicated raw block device on each node for the shared iSCSI target, 
e.g.:
    - /dev/sdX
3. Obtain each node’s initiator name using: ```cat /etc/iscsi/initiatorname.iscsi```:
   - worker0: iqn.1994-05.com.redhat:AAAA
   - worker1: iqn.1994-05.com.redhat:BBBB
   - worker2: iqn.1994-05.com.redhat:CCCC
   - worker3: iqn.1994-05.com.redhat:DDDD
#### Per-Node Setup (the following steps must be performed on **each node**)

1. Configure the iSCSI target.\
Replace \<worker name\>, /dev/sdX, /dev/sdY, \<ip\>,  and initiator IQNs ('iqn.1994-05.com.redhat:AAAA',...) with your values.
```bash
# prerequisites
# install targetcli
yum install targetcli -y
systemctl start target
systemctl enable target

# open port 3260 for iSCSI
firewall-cmd --permanent --add-port=3260/tcp
firewall-cmd --reload

# install initiator
yum install iscsi-initiator-utils -y
systemctl enable iscsid iscsi
systemctl start iscsid iscsi

# create a block backstore
# naming format: <worker name>_shared
sudo targetcli "backstores/block create name=<worker name>_shared dev=/dev/sdX"

# create an iSCSI target with IQN
# naming format: iqn.2022-05.<ip>:<worker name>
sudo targetcli "iscsi/ create iqn.2022-05.<ip>:<worker name>"

# create a LUN
sudo targetcli "iscsi/iqn.2022-05.<ip>:<worker name>/tpg1/luns \
create /backstores/block/<worker name>_shared"

# Add all initiator names to the ACL, enabling them to
# connect the target
sudo targetcli "iscsi/iqn.2022-05.<ip>:<worker name>/tpg1/acls \
create iqn.1994-05.com.redhat:AAAA"
sudo targetcli "iscsi/iqn.2022-05.<ip>:<worker name>/tpg1/acls \
create iqn.1994-05.com.redhat:BBBB"
sudo targetcli "iscsi/iqn.2022-05.<ip>:<worker name>/tpg1/acls \
create iqn.1994-05.com.redhat:CCCC"
sudo targetcli "iscsi/iqn.2022-05.<ip>:<worker name>/tpg1/acls \
create iqn.1994-05.com.redhat:DDDD"
# save the configuration
sudo targetcli "saveconfig"

# discover and log in to the local target
iscsiadm --mode discovery --type sendtargets --portal <ip>
iscsiadm -m node -T iqn.2022-05.<ip>:<worker name> -l

# verify device (e.g., /dev/sdY)
lsscsi

# format the shared device with the ext4 file system
sudo mkfs.ext4 /dev/sdY
```

2. Build and install the project:
```bash
git clone https://github.com/wjnlim/blfs_mr.git

mkdir blfs_mr/build
cd blfs_mr/build

cmake -DCMAKE_INSTALL_PREFIX=<your install directory> ..
cmake --build . --target install
```
3. Set the project environment variable:
```bash
# Change directory to blfs_mr/
cd ..
./set_env.sh && source ~/.bashrc
```
4. Configure the ```sharedfiles``` file (default: 64 files named shfileXX, each 2 MB) and create the shared files on the target device (ensure the node is logged into the target):
```bash
./alloc_shared_files.sh /dev/sdY
```
<!-- 8. configure the ```workers``` file with **\<worker name\>:\<ip\>** pairs, one per line, on **each node** -->
---
#### Master-Node Setup
1. configure the ```workers``` file with **\<worker name\>:\<ip\>** pairs, one per line.
2. **After the per-node setup is complete,**, Initialize the workers from the master:
```bash
./init_workers.sh
```
#### Run a MapReduce program
1. Write and compile a MapReduce program
(example: [mr_wordcount.c](mr_wordcount.c)):
```bash
gcc mr_wordcount.c -o mr_wordcount -I <your install directory>include/ \
<your install directory>lib/libblfs_mr.a -lpthread
```
2. (Optional) Generate a WordCount input:
```bash
# ex) ./gen_wordcount_input.sh inputs/32M_input 32Mi
./gen_wordcount_input.sh <path to input> 32Mi
```
3. Distribute the input across the workers (this will generate a .meta file for the input):
```bash
# across all workers, 2MB chunks
# ex) ./distr_input.sh inputs/32M_input 2M 
# or across worker0 and worker1 only
# ex) ./distr_input.sh inputs/32M_input 2M -w "worker0 worker1"
./distr_input.sh <path to input> <chunk size>
```
4. Start worker processes on all worker nodes from the **master**:
```bash
./start_workers.sh
```
5. Run the MapReduce job (this will generate a .meta file for the output):
```bash
# ex) ./run_mapred.sh mr_wordcount inputs/32M_input.meta outputs/32M_output.meta
./run_mapred.sh <path to the wordcount executable> \
<path to the input metadata> <path to the output metadata>
```
6. Print the output:
```bash
# ex) ./print_output.sh outputs/32M_output.meta -s # '-s' for sorted output
./print_output.sh <path to the output metadata>
```