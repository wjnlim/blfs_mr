# MapReduce Using Block-Level File Sharing over iSCSI
A lightweight MapReduce framework that uses shared iSCSI block devices to access intermediate data directly between worker nodes.

## Overview
This research project presents an experimental MapReduce framework that leverages **shared iSCSI volumes** for block-level intermediate file sharing.
Unlike conventional MapReduce implementations (e.g., Hadoop, Spark), which rely on shuffle daemons and network-based file copying, this framework enables workers to **read and write intermediate data directly on shared block devices**.

This design:
* **Simplifies** the MapReduce architecture
* **Eliminates redundant data transfers** inherent in traditional shuffle mechanisms
* Enables **block-level data sharing** across nodes through iSCSI
* Removes the need for shuffle daemons or file copy services
* Allows intermediate files to be accessed as if they were local files

A central focus of the research is resolving **cache inconsistency issues** that occur when multiple nodes share the same iSCSI volumes.

## Documentation
For full details—including cache analysis, system design, experimental findings, and architectural diagrams—please refer to this 
**Technical Report:** [blfs_mr.pdf](docs/blfs_mr.pdf)

## Key Features
* **Asynchronous master-worker model**\
Built on my [msg_pass](https://github.com/wjnlim/msg_pass.git) library
* **iSCSI-based shared intermediate storage**\
Direct block-level data access across nodes without network-based file copying.
* **Simplified MapReduce workflow**\
No shuffle handler, fetcher, or auxiliary data transfer daemons.
* **Cache-Consistent Shared Access using:**
    * Pre-allocated intermediate files
    * Explicit fsync() on both file and block device
    * O_DIRECT reads on reducers to bypass page cache
* **Notes**
    * This project is mainly for an **experimental** MapReduce framework design.\
    Thus, the implementation may lack fault-tolerance, load balancing, proper error-handling, so please use it with caution.
    * This project uses my [msg_pass](https://github.com/wjnlim/msg_pass.git) library. The CMake file will automatically fetch the project internally

## Core Concept: Pre-Allocated Files Approach
ISCSI provides no built-in cache coherence and most file systems do
not have built-in conflict resolution for shared access.
Thus, sharing iSCSI volumes directly among workers creates cache inconsistency problems, where reducers do not detect new files created by mappers.
While explicit cache-dropping before every intermediate file opening fixes the issue, it is prohibitively expensive.

To address this, the framework adopts:
* **Pre-Allocated Files Strategy**
  * All intermediate files are created on each node’s local iSCSI target before that disk is mounted by remote nodes.
  * Mappers are assigned pre-created intermediate files and simply overwrite them rather than creating new ones.
  * Mappers flush intermediate data using ```fsync(fd)``` and ```fsync(blkdev_fd)```
  to ensure durability.
  * Reducers open the files using O_DIRECT and read intermediate data bypassing page caches
  * No cache-dropping is required on reducer nodes
  * **Notes**
    * This experimental design was implemented based on an assumption that
      each node has enough pre-allocated file for tasks.
    * The blfs_mr library depends on pthread, so you must link with **-lpthread**.
  
This ensures consistent shared access without needing a distributed file system and without imposing expensive cache invalidation operations.
## How the MapReduce framework works using intermediate file sharing
![Execution overview](images/Execution%20overview.png)

0. The ```run_mapred.sh``` script takes a user-provided MapReduce program—implementing the ```Mapper```, ```Reducer``` functions and calling ```MR_run_task()```—as input. It copies the MapReduce executable to all worker nodes and launches the ```master``` process.
1. The master assigns map tasks to workers where input splits are located and
assigns reduce tasks to workers with the smallest current task load.
2. Each worker spawns processes to run the MapReduce program. The program
invokes ```MR_run_task()``` to execute the assigned task. Map tasks run the user-defined ```Mapper``` function.
3. Mappers write intermediate data to pre-allocated files provided by the local worker process and flush the data using both ```fsync(fd)``` and ```fsync(blkdev fd)```. Then they notify task completions to the master.
4. When reducers are notified by the master about these files, they open the files using the ```O_DIRECT``` flag and read the data directly from remote shared disks.
5. Reducers run the user-defined ```Reducer``` function and write final outputs to their local disks.

## Quick Start Guide
### Scripts and Configuration Files
```sharedfiles```: Cotains a list of **\<filename\>:\<size\>** pairs, one per line, 
specifying the pre-allocated intermediate files.

```alloc_shared_files.sh```: Creates the shared intermediate files listed in ```sharedfiles``` on the device file provided as input.

```set_env.sh```: Sets the ```BLFS_MR_HOME``` envrionment variable, which defines the project’s base directory.

```workers```: Contains **\<worker name\>:\<ip\>** pairs. Used by scripts for worker name ↔ IP resolution.

```mnt_targets.sh```: Logs into all worker nodes' iSCSI targets and mounts them.

```init_workers.sh```: Initializes worker nodes by creating directories for input splits and output partitions and executing the ```mnt_targets.sh``` on each nodes. 
Generates the ```workers_n_splits``` metadata file, which stores
**\<worker\>:\<num_of_input_split\>** pairs used for input-split distribution.


```gen_wordcount_input.sh```: Generates a random input file for the WordCount MapReduce example.

```distr_input.sh```: Distributes an input file across the specified workers and produces a metadata file containing **\<file path\>:\<worker\>** mappings.

```rm_isplits.sh```: Deletes the metadata and file splits associated with a given input.

```rm_outputs.sh```: Deletes the metadata and partitions associated with a given output.

```print_output.sh```: Prints the final output by merging all output partitions.

```run_mapred.sh```: Copies a user-provided MapReduce executable to all workers and launches the ```master``` program to execute the MapReduce workflow.

### 4-node cluster Example
1. Prepare 4nodes. For example,
    - master/worker0 (172.30.1.33) // this node also works as master node
    - worker1 (172.30.1.34)
    - worker2 (172.30.1.35)
    - worker3 (172.30.1.36)\
(This project assumes that the master can ssh to all nodes)
2. Prepare a dedicated raw volume on each node for share iSCSI target.
    - /dev/sdX
3. Get all initiator name from the worker nodes 
using ```cat /etc/iscsi/initiatorname.iscsi```.
    - worker0: iqn.1994-05.com.redhat:AAAA
    - worker1: iqn.1994-05.com.redhat:BBBB
    - worker2: iqn.1994-05.com.redhat:CCCC
    - worker3: iqn.1994-05.com.redhat:DDDD
1. Configure iSCSI target on **each node**.\
Put your backstore_name and device name instead of '\<backstore_name\>' and '/dev/sdX', '/dev/sdY'.\
Put the node's ip and name instead of '\<ip\>' and '\<worker name\>'.\
Put your initiator names instread of the 'iqn.1994-05.com.redhat:AAAA',...
```bash
# prerequisites
# install targetcli
yum install targetcli -y
systemctl start target
systemctl enable target
# open the 3260 port for the iSCSI protocol
firewall-cmd --permanent --add-port=3260/tcp
firewall-cmd --reload
# install initiator
yum install iscsi-initiator-utils -y
systemctl enable iscsid iscsi
systemctl start iscsid iscsi

# create a block backstore
# this project use a name format: <worker name>_shared.
# please follow the format.
sudo targetcli "backstores/block create name=<worker name>_shared dev=/dev/sdX"
# create a iSCSI target with IQN 
# this project uses format: iqn.2022-05.<worker's ip>:<worker name>
# please follow the format.
sudo targetcli "iscsi/ create iqn.2022-05.<ip>:<worker name>"
# create a LUN
sudo targetcli "iscsi/iqn.2022-05.<ip>:<worker name>/tpg1/luns create \ /backstores/block/<worker name>_shared"

# Add all the initiator names to the ACL allowing them to
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

# perform a discovery to local target
iscsiadm --mode discovery --type sendtargets --portal <ip>
# log in to the target
iscsiadm -m node -T iqn.2022-05.<ip>:<worker name> -l
# Check the target's device name (e.g., /dev/sdY)
lsscsi
# Format the device with the ext4 file system
sudo mkfs.ext4 /dev/sdY
```
5. Build and install the project source code on **each node**.
```bash
# 1. Clone the repository
git clone https://github.com/wjnlim/blfs_mr.git

# 2. Create a build directory
mkdir blfs_mr/build
cd blfs_mr/build

# 3. Configure with CMake
cmake -DCMAKE_INSTALL_PREFIX=<your install directory> ..

# 4. Build and install the library
cmake --build . --target install
```
6. Set the project's environment variable on **each node**.
```bash
# Change directory to blfs_mr/
cd ..
./set_env.sh && source ~/.bashrc
```
7. Configure the ```sharedfiles``` file (default configuration specifying 64 files named with 'shfileXX', sized with 2MB) and
create shared files on the target device on **each node** (make sure you are logged into the target)
```bash
./alloc_shared_files.sh /dev/sdY
```
8. configure the ```workers``` file with **\<worker name\>:\<ip\>** pairs, one per line, on **each node**

9.  On **master node**, Initialize the workers.
```bash
./init_workers.sh
```
10.  Write a MapReduce program and compile.
(This example use the [mr_wordcount.c](mr_wordcount.c))
```bash
gcc mr_wordcount.c -o mr_wordcount -I <your install directory>include/ \
<your install directory>lib/libblfs_mr.a -lpthread
```