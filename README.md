# cluster-sync

ckuster-sync is a tool to replicate a single "master" Bright cluster to one or more "replica" clusters. This solution does not replicate all the objects defined in CMDaemon, Instead,  you control what gets replicated through a synchronization definition file. It can be extended to replicate many additional cluster object types (including multiples of each type) to as many remote cluster as you desire. 

# Download and install the distribution

'''
yum install git
git clone https://github.com/plabrop/cluster-sync.git
'''

# Make sure you're running latest version of Bright

We recommend that you update both the master and the replica head nodes using 'yum update'. But you can choose to update only the required Bright packages as shown below.

```
[root@master ~]# yum update cmdaemon
```

# Establish one-way trust

The cluster-sync.py script will use rsync to replicate the desired software images and cluster objects, as defined in the cluster-sync-definition.xml file. To facilitate replication over an insecure link, replication will be affected using ssh. To enable this, add the root account's ssh public key to the root account's authorized_keys file on the replica cluster. This will establish a one-way trust; the replica cluster will trust the root account of the master cluster, but the master cluster will not trust the replica cluster.

```
[root@master ~]# ssh-copy-id -i ~/.ssh/id_dsa.pub root@replica-headnode
root@replica-headnode's password:
```
CMDaemon on the replica cluster also needs to trust the CMDaemon on the master cluster. Using a secure copy program, copy the replica cluster's x509 keys for the root account into a secure local directory. The permissions of both the directory and the keys must match that show below (directory 0700, keys 0600).

```
[root@master ~]# scp root@replica-headnode:/root/.cm/cmsh/\{admin.key,admin.pem\} \
bright-cluster-replication/replica/keys

# ls -lR bright-cluster-replication/replica/

replica:
total 4
drwx------ 2 root root 4096 Aug 14 14:34 keys

replica/keys:
total 8
-rw------- 1 root root 1869 Aug  7 15:14 admin.key
-rw------- 1 root root 1427 Aug  7 15:14 admin.pem
```

If you choose to store the keys in a file path other than that shown above you will need to edit the cluster-sync-definition.xml file such that it reflects the actual file path. Here's the section you'll need to update:

```
<brightcert>
  <pemfile>/root/bright-cluster-replication/replica/keys/admin.pem</pemfile>
  <keyfile>/root/bright-cluster-replication/replica/keys/admin.key</keyfile>
</brightcert> 
```

# Configure replication

You can select which objects get replicated  by editing the synchronization definition file (cluster-sync.xml). See the example in this repository.

# Replicate Clusters

```
  Usage: cluster-sync.py -f <file> -x <exclude> [-v -d -n]

  Options
    -f | --file <file>         Synchronization definition file
    -v | --verbose             Be verbose in output 
    -x | --exclude <file>      List of files that should be excluded from Rsync operations
    -d | --dry                 Perform  a dry run
    -n | --preserve-fsmounts   Preserve FSMounts on target head node.
    -r | --preserve-roles      Preserve roles on target head node's categories.
  


[root@master ~]# cd cluster-sync &&  chmod +x cluster-sync.py
[root@master cluster-sync]# ./cluster-sync.py -v -f cluster-sync.xml

```

# Excluding files in the software image from being transfered

 Certain files on the target cluster's software image might have to be different because they contain server-specific settings. In this case, tou can exclude such files from being sychronized by provding an exclude list file.  The format is the same as for a typical rsync excludelist.

The source path is e.g. /cm/images/test-image and the destination is target_headnode:/cm/images/test-image

So the exclude list file could be :
```
etc/sssd/sssd.conf (relative path)
```
or 
```
- etc/sssd/sssd.conf
```

Or use absolute paths e.g.
```
- /usr
```
(/cm/images/test-image/usr will not be synced on the target)

In the first case both /cm/images/test-image/etc/sssd/sssd.conf and e.g. /cm/images/test-image/root/backup/etc/sssd/sssd.conf (all paths ending with that pattern) will be excluded.

The format is the same as for the exclude lists that you define for node categories. The only difference is that you cannot uses the no-new files directive.


```
no-new-files: - /tftpboot/*
```
