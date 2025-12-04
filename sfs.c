#define FUSE_USE_VERSION 26

#include <errno.h>
#include <fuse.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <time.h>

#include "sfs.h"
#include "diskio.h"


static const char default_img[] = "test.img";

/* Options passed from commandline argumentss */
struct options {
    const char *img;
    int background;
    int verbose;
    int show_help;
    int show_fuse_help;
} options;


#define log(fmt, ...) \
    do { \
        if (options.verbose) \
            printf(" # " fmt, ##__VA_ARGS__); \
    } while (0)


/* libfuse2 leaks, so let's shush LeakSanitizer if we are using Asan. */
const char* __asan_default_options() { return "detect_leaks=0"; }



static blockidx_t get_next(blockidx_t current) {
    blockidx_t next;

    off_t offset = SFS_BLOCKTBL_OFF + (current * sizeof(blockidx_t));
    disk_read(&next, sizeof(blockidx_t), offset);
    return next;
}


static void set_next(blockidx_t current, blockidx_t nextVal) {
    off_t offset = SFS_BLOCKTBL_OFF + (current * sizeof(blockidx_t));
    disk_write(&nextVal, sizeof(blockidx_t), offset);
}


static blockidx_t free_blk() {
    blockidx_t entry;

    for (int i = 0; i < SFS_BLOCKTBL_NENTRIES; i++) {

        off_t offset = SFS_BLOCKTBL_OFF + (i * sizeof(blockidx_t));

        disk_read(&entry, sizeof(blockidx_t), offset);

        if (entry == SFS_BLOCKIDX_EMPTY) {
            return (blockidx_t)i;
        }
    }
    return SFS_BLOCKIDX_END;
}

/*
 * This is a helper function that is optional, but highly recomended you
 * implement and use. Given a path, it looks it up on disk. It will return 0 on
 * success, and a non-zero value on error (e.g., the file did not exist).
 * The resulting directory entry is placed in the memory pointed to by
 * ret_entry. Additionally it can return the offset of that direntry on disk in
 * ret_entry_off, which you can use to update the entry and write it back to
 * disk (e.g., rmdir, unlink, truncate, write).
 *
 * You can start with implementing this function to work just for paths in the
 * root entry, and later modify it to also work for paths with subdirectories.
 * This way, all of your other functions can use this helper and will
 * automatically support subdirectories. To make this function support
 * subdirectories, we recommend you refactor this function to be recursive, and
 * take the current directory as argument as well. For example:
 *
 *  static int get_entry_rec(const char *path, const struct sfs_entry *parent,
 *                           size_t parent_nentries, blockidx_t parent_blockidx,
 *                           struct sfs_entry *ret_entry,
 *                           unsigned *ret_entry_off)
 *
 * Here parent is the directory it is currently searching (at first the rootdir,
 * later the subdir). The parent_nentries tells the function how many entries
 * there are in the directory (SFS_ROOTDIR_NENTRIES or SFS_DIR_NENTRIES).
 * Finally, the parent_blockidx contains the blockidx of the given directory on
 * the disk, which will help in calculating ret_entry_off.
 */
static int get_entry(const char *path, struct sfs_entry *ret_entry,
                     unsigned *ret_entry_off)
{
    char *copy = strdup(path);
    char *token = strtok(copy, "/");
    int found;
    unsigned int dirOff = SFS_ROOTDIR_OFF;
    int dirEntries = SFS_ROOTDIR_NENTRIES;
    struct sfs_entry entry;

    while (token != NULL) {
        found = 0;

        for (int i = 0; i < dirEntries; i++) { 
            unsigned int entryDiskOff = 0;

            if (dirOff == SFS_ROOTDIR_OFF) {

                entryDiskOff = SFS_ROOTDIR_OFF + (i * sizeof(struct sfs_entry));
                disk_read(&entry, sizeof(struct sfs_entry), entryDiskOff);
            } else {
                blockidx_t blk = (blockidx_t)dirOff;
                
                if (i >= 8) {
                    blk = get_next(blk);
                }
                
                int indexInblk = i % 8;
                entryDiskOff = SFS_DATA_OFF + (blk * SFS_BLOCK_SIZE) + (indexInblk * sizeof(struct sfs_entry));
                
                disk_read(&entry, sizeof(struct sfs_entry), entryDiskOff);
            }

            if (strlen(entry.filename) > 0 && strcmp(entry.filename, token) == 0) {
                found = 1;

                if (ret_entry){
                    *ret_entry = entry;
                }

                if (ret_entry_off){
                    *ret_entry_off = entryDiskOff;
                }
                
                token = strtok(NULL, "/");
                
                if (token != NULL) {
                    dirOff = entry.first_block;
                    dirEntries = SFS_DIR_NENTRIES;
                }
                break;
            }
        }

        if (!found) {
            return -ENOENT;
        }
    }

    return 0;
}

/*
 * Retrieve information about a file or directory.
 * You should populate fields of `st` with appropriate information if the
 * file exists and is accessible, or return an error otherwise.
 *
 * For directories, you should at least set st_mode (with S_IFDIR) and st_nlink.
 * For files, you should at least set st_mode (with S_IFREG), st_nlink and
 * st_size.
 *
 * Return 0 on success, < 0 on error.
 */
static int sfs_getattr(const char *path,
                       struct stat *st)
{
    log("getattr %s\n", path);

    memset(st, 0, sizeof(struct stat));
   
    if (strcmp(path, "/") == 0) {
        st->st_mode = S_IFDIR | 0755;
        st->st_nlink = 2;
        return 0;
    }

    struct sfs_entry entry;

    int res = 0;
    res = get_entry(path, &entry, NULL);
    
    if (res != 0){
        return res;
    }

    if (entry.size & SFS_DIRECTORY) {
        st->st_mode = S_IFDIR | 0755;
        st->st_nlink = 2;
        st->st_size = 0;
    } else {
        st->st_mode = S_IFREG | 0644;
        st->st_nlink = 1;
        st->st_size = entry.size & SFS_SIZEMASK;
    }

    return 0;
}

/*
 * Return directory contents for `path`. This function should simply fill the
 * filenames - any additional information (e.g., whether something is a file or
 * directory) is later retrieved through getattr calls.
 * Use the function `filler` to add an entry to the directory. Use it like:
 *  filler(buf, <dirname>, NULL, 0);
 * Return 0 on success, < 0 on error.
 */
static int sfs_readdir(const char *path,
                       void *buf,
                       fuse_fill_dir_t filler,
                       off_t offset,
                       struct fuse_file_info *fi)
{
    (void)offset, (void)fi;
    log("readdir %s\n", path);

    (void)filler;
    (void)buf;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    struct sfs_entry dirEntry;
    unsigned int startOffDisk = 0;
    int nEntries = 0;

    if (strcmp(path, "/") == 0) {
        startOffDisk = SFS_ROOTDIR_OFF;
        nEntries = SFS_ROOTDIR_NENTRIES;
        
        for (int i = 0; i < nEntries; i++) {
             struct sfs_entry entry;
             disk_read(&entry, sizeof(struct sfs_entry), startOffDisk + i * sizeof(struct sfs_entry));
             
             if (strlen(entry.filename) != 0) {
                 filler(buf, entry.filename, NULL, 0);
             }
        }

    } else {

        int res = get_entry(path, &dirEntry, NULL);

        if (res != 0) {
            return res;
        }

        blockidx_t blk = dirEntry.first_block;
        
        while (blk != SFS_BLOCKIDX_END && blk != SFS_BLOCKIDX_EMPTY) {
            
            for (int i=0; i < 8; i++) {

                struct sfs_entry entry;

                off_t addr = SFS_DATA_OFF + (blk * SFS_BLOCK_SIZE) + (i * sizeof(struct sfs_entry));
                
                disk_read(&entry, sizeof(struct sfs_entry), addr);

                if (strlen(entry.filename) != 0) {
                    filler(buf, entry.filename, NULL, 0);
                }
            }
            blk = get_next(blk);
        }
    }

    return 0;
}

/*
 * Read contents of `path` into `buf` for  up to `size` bytes.
 * Note that `size` may be bigger than the file actually is.
 * Reading should start at offset `offset`; the OS will generally read your file
 * in chunks of 4K byte.
 * Returns the number of bytes read (writting into `buf`), or < 0 on error.
 */
static int sfs_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
    (void)fi;
    log("read %s size=%zu offset=%ld\n", path, size, offset);

    struct sfs_entry entry;

    if (get_entry(path, &entry, NULL) != 0){
        return -ENOENT;
    }

    blockidx_t blk = entry.first_block;
    size_t bytesRead = 0;

    while (offset >= SFS_BLOCK_SIZE) {
        blk = get_next(blk);
        offset -= SFS_BLOCK_SIZE;
    }

    while (bytesRead < size && blk != SFS_BLOCKIDX_END && blk != SFS_BLOCKIDX_EMPTY) {
        
        size_t needRead = SFS_BLOCK_SIZE - offset;

        off_t addr = SFS_DATA_OFF + (blk * SFS_BLOCK_SIZE) + offset;
        disk_read(buf + bytesRead, needRead, addr);

        bytesRead += needRead;
        offset = 0;
        blk = get_next(blk);
    }

    return bytesRead;
}

/*
 * Create directory at `path`.
 * The `mode` argument describes the permissions, which you may ignore for this
 * assignment.
 * Returns 0 on success, < 0 on error.
 */
static int sfs_mkdir(const char *path, mode_t mode)
{
    log("mkdir %s\n", path);
    (void)mode;
    
    char *copy = strdup(path);
    
    char *endSlh = strrchr(copy, '/');    
    *endSlh = '\0';

    char *pPath = copy;
    
    if (strlen(pPath) == 0){
        pPath = "/";
    }

    struct sfs_entry pEntry;
    unsigned int pDiskOff = 0;

    int isRoot = (strcmp(pPath, "/") == 0);

    if (!isRoot) {
        if (get_entry(pPath, &pEntry, &pDiskOff) != 0) {
            return -ENOENT;
        }
    }

    blockidx_t pBlk = isRoot ? 0 : pEntry.first_block;
    
    unsigned int emptySlotAddr = 0;
    int foundSlot = 0;
    int entriesChecked = 0;

    int maxEntry = isRoot ? SFS_ROOTDIR_NENTRIES : SFS_DIR_NENTRIES;

    while(entriesChecked < maxEntry) {
        
        for(int i=0; i<8; i++) {
            
            off_t addr;

            if (isRoot) {
                addr = SFS_ROOTDIR_OFF + (entriesChecked * sizeof(struct sfs_entry));
            } else {
                addr = SFS_DATA_OFF + (pBlk * SFS_BLOCK_SIZE) + (i * sizeof(struct sfs_entry));
            }

            struct sfs_entry temp;
            disk_read(&temp, sizeof(struct sfs_entry), addr);
            
            if (strlen(temp.filename) == 0 && foundSlot == 0) {
                emptySlotAddr = addr;
                foundSlot = 1;
            }

            entriesChecked++;
        }

        pBlk = get_next(pBlk);

        if (pBlk == SFS_BLOCKIDX_END){
            break;
        }
    }

    blockidx_t b1 = free_blk();
    
    set_next(b1, SFS_BLOCKIDX_END); 
    
    blockidx_t b2 = free_blk();

    set_next(b1, b2);
    set_next(b2, SFS_BLOCKIDX_END);

    struct sfs_entry newEntry;

    memset(&newEntry, 0, sizeof(struct sfs_entry));
    
    char *newName = endSlh + 1;
    strncpy(newEntry.filename, newName, SFS_FILENAME_MAX - 1);

    newEntry.first_block = b1;

    newEntry.size = SFS_DIRECTORY;

    disk_write(&newEntry, sizeof(struct sfs_entry), emptySlotAddr);
    
    struct sfs_entry emptyEntries[8];

    memset(emptyEntries, 0, sizeof(emptyEntries));

    for(int i=0; i<8; i++) {
        emptyEntries[i].first_block = SFS_BLOCKIDX_EMPTY;
    }

    disk_write(emptyEntries, SFS_BLOCK_SIZE, SFS_DATA_OFF + (b1 * SFS_BLOCK_SIZE));
    disk_write(emptyEntries, SFS_BLOCK_SIZE, SFS_DATA_OFF + (b2 * SFS_BLOCK_SIZE));

    return 0;
}

/*
 * Remove directory at `path`.
 * Directories may only be removed if they are empty, otherwise this function
 * should return -ENOTEMPTY.
 * Returns 0 on success, < 0 on error.
 */
static int sfs_rmdir(const char *path)
{
    log("rmdir %s\n", path);

    struct sfs_entry entry;
    unsigned int entryAddr;

    if (get_entry(path, &entry, &entryAddr) != 0){
        return -ENOENT;
    }

    blockidx_t blk = entry.first_block;

    while(blk != SFS_BLOCKIDX_END && blk != SFS_BLOCKIDX_EMPTY) {
        
        for(int i=0; i<8; i++) {

            struct sfs_entry temp;

            disk_read(&temp, sizeof(struct sfs_entry), SFS_DATA_OFF + (blk * SFS_BLOCK_SIZE) + (i*sizeof(struct sfs_entry)));
            
            if (strlen(temp.filename) > 0){
                return -ENOTEMPTY;
            }
        }
        blk = get_next(blk);
    }

    blk = entry.first_block;

    while(blk != SFS_BLOCKIDX_END && blk != SFS_BLOCKIDX_EMPTY) {

        blockidx_t next = get_next(blk);
        set_next(blk, SFS_BLOCKIDX_EMPTY);

        blk = next;
    }

    struct sfs_entry emptyEntry;

    memset(&emptyEntry, 0, sizeof(struct sfs_entry));

    emptyEntry.first_block = SFS_BLOCKIDX_EMPTY;
    
    disk_write(&emptyEntry, sizeof(struct sfs_entry), entryAddr);

    return 0;
}

/*
 * Remove file at `path`.
 * Can not be used to remove directories.
 * Returns 0 on success, < 0 on error.
 */
static int sfs_unlink(const char *path)
{
    log("unlink %s\n", path);

    struct sfs_entry entry;
    unsigned int entryAddr;

    if (get_entry(path, &entry, &entryAddr) != 0){
        return -ENOENT;
    }

    blockidx_t blk = entry.first_block;

    while(blk != SFS_BLOCKIDX_END && blk != SFS_BLOCKIDX_EMPTY) {

        blockidx_t next = get_next(blk);
        set_next(blk, SFS_BLOCKIDX_EMPTY);

        blk = next;
    }

    struct sfs_entry empty;

    memset(&empty, 0, sizeof(struct sfs_entry));

    empty.first_block = SFS_BLOCKIDX_EMPTY;

    disk_write(&empty, sizeof(struct sfs_entry), entryAddr);

    return 0;
}

/*
 * Create an empty file at `path`.
 * The `mode` argument describes the permissions, which you may ignore for this
 * assignment.
 * Returns 0 on success, < 0 on error.
 */
static int sfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    (void)fi; 
    (void)mode;
    
    log("create %s\n", path);

    char *copy = strdup(path);

    char *endSlh = strrchr(copy, '/');
    *endSlh = '\0';
    
    char *pPath = copy;
    
    if (strlen(pPath) == 0){
        pPath = "/";
    }

    struct sfs_entry pEntry;

    int isRoot = (strcmp(pPath, "/") == 0);

    if (!isRoot) {
        if (get_entry(pPath, &pEntry, NULL) != 0) { 
            return -ENOENT; 
        }
    }

    blockidx_t pBlk = isRoot ? 0 : pEntry.first_block;

    unsigned int emptySlot = 0;
    int found = 0;
    int checked = 0;
    int maxEntry = isRoot ? SFS_ROOTDIR_NENTRIES : SFS_DIR_NENTRIES;

    while(checked < maxEntry) {

        for(int i=0; i<8; i++) {

             off_t addr = isRoot ? (SFS_ROOTDIR_OFF + checked * sizeof(struct sfs_entry)) : (SFS_DATA_OFF + pBlk * SFS_BLOCK_SIZE + i * sizeof(struct sfs_entry));
             struct sfs_entry temp;
             
             disk_read(&temp, sizeof(temp), addr);

             if (strlen(temp.filename) == 0 && !found) {
                 emptySlot = addr;
                 found = 1;
             }

             checked++;
        }

        pBlk = get_next(pBlk);

        if (pBlk == SFS_BLOCKIDX_END){
            break;
        }
    }

    struct sfs_entry newFile;

    memset(&newFile, 0, sizeof(newFile));

    char *newName = endSlh + 1;

    if (strlen(newName) > SFS_FILENAME_MAX - 1) { 
        return -ENAMETOOLONG; 
    }

    strncpy(newFile.filename, newName, SFS_FILENAME_MAX - 1);

    newFile.first_block = SFS_BLOCKIDX_END;
    newFile.size = 0;

    disk_write(&newFile, sizeof(newFile), emptySlot);
    return 0;
}


/*
 * Shrink or grow the file at `path` to `size` bytes.
 * Excess bytes are thrown away, whereas any bytes added in the process should
 * be nil (\0).
 * Returns 0 on success, < 0 on error.
 */
static int sfs_truncate(const char *path, off_t size)
{
    log("truncate %s size=%ld\n", path, size);

    return -ENOSYS;
}


/*
 * Write contents of `buf` (of `size` bytes) to the file at `path`.
 * The file is grown if nessecary, and any bytes already present are overwritten
 * (whereas any other data is left intact). The `offset` argument specifies how
 * many bytes should be skipped in the file, after which `size` bytes from
 * buffer are written.
 * This means that the new file size will be max(old_size, offset + size).
 * Returns the number of bytes written, or < 0 on error.
 */
static int sfs_write(const char *path,
                     const char *buf,
                     size_t size,
                     off_t offset,
                     struct fuse_file_info *fi)
{
    (void)fi;
    log("write %s data='%.*s' size=%zu offset=%ld\n", path, (int)size, buf,
        size, offset);

    return -ENOSYS;
}


/*
 * Move/rename the file at `path` to `newpath`.
 * Returns 0 on succes, < 0 on error.
 */
static int sfs_rename(const char *path,
                      const char *newpath)
{
    /* Implementing this function is optional, and not worth any points. */
    log("rename %s %s\n", path, newpath);

    return -ENOSYS;
}


static const struct fuse_operations sfs_oper = {
    .getattr    = sfs_getattr,
    .readdir    = sfs_readdir,
    .read       = sfs_read,
    .mkdir      = sfs_mkdir,
    .rmdir      = sfs_rmdir,
    .unlink     = sfs_unlink,
    .create     = sfs_create,
    .truncate   = sfs_truncate,
    .write      = sfs_write,
    .rename     = sfs_rename,
};


#define OPTION(t, p)                            \
    { t, offsetof(struct options, p), 1 }
#define LOPTION(s, l, p)                        \
    OPTION(s, p),                               \
    OPTION(l, p)
static const struct fuse_opt option_spec[] = {
    LOPTION("-i %s",    "--img=%s",     img),
    LOPTION("-b",       "--background", background),
    LOPTION("-v",       "--verbose",    verbose),
    LOPTION("-h",       "--help",       show_help),
    OPTION(             "--fuse-help",  show_fuse_help),
    FUSE_OPT_END
};

static void show_help(const char *progname)
{
    printf("usage: %s mountpoint [options]\n\n", progname);
    printf("By default this FUSE runs in the foreground, and will unmount on\n"
           "exit. If something goes wrong and FUSE does not exit cleanly, use\n"
           "the following command to unmount your mountpoint:\n"
           "  $ fusermount -u <mountpoint>\n\n");
    printf("common options (use --fuse-help for all options):\n"
           "    -i, --img=FILE      filename of SFS image to mount\n"
           "                        (default: \"%s\")\n"
           "    -b, --background    run fuse in background\n"
           "    -v, --verbose       print debug information\n"
           "    -h, --help          show this summarized help\n"
           "        --fuse-help     show full FUSE help\n"
           "\n", default_img);
}

int main(int argc, char **argv)
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    options.img = strdup(default_img);

    fuse_opt_parse(&args, &options, option_spec, NULL);

    if (options.show_help) {
        show_help(argv[0]);
        return 0;
    }

    if (options.show_fuse_help) {
        assert(fuse_opt_add_arg(&args, "--help") == 0);
        args.argv[0][0] = '\0';
    }

    if (!options.background)
        assert(fuse_opt_add_arg(&args, "-f") == 0);

    disk_open_image(options.img);

    return fuse_main(args.argc, args.argv, &sfs_oper, NULL);
}
