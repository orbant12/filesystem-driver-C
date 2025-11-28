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

/* Options passed from commandline arguments */
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



static blockidx_t get_next_block(blockidx_t current) {
    blockidx_t next;

    off_t offset = SFS_BLOCKTBL_OFF + (current * sizeof(blockidx_t));
    disk_read(&next, sizeof(blockidx_t), offset);
    return next;
}

static void set_next_block(blockidx_t current, blockidx_t nextVal) {
    off_t offset = SFS_BLOCKTBL_OFF + (current * sizeof(blockidx_t));
    disk_write(&nextVal, sizeof(blockidx_t), offset);
}

static blockidx_t find_free_block() {
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

    if (!copy) {
        return -ENOMEM;
    }

    unsigned int current_dir_offset = SFS_ROOTDIR_OFF;
    int current_dir_entries = SFS_ROOTDIR_NENTRIES;
    struct sfs_entry entry;

    int found = 0;

    char *token = strtok(copy, "/");

    if (token == NULL && strcmp(path, "/") == 0) {
        free(copy);
        return 0; 
    }

    while (token != NULL) {
        found = 0;

        for (int i = 0; i < current_dir_entries; i++) { 
            unsigned int entry_disk_offset = 0;

            if (current_dir_offset == SFS_ROOTDIR_OFF) {

                entry_disk_offset = SFS_ROOTDIR_OFF + (i * sizeof(struct sfs_entry));
                disk_read(&entry, sizeof(struct sfs_entry), entry_disk_offset);
            } else {
                blockidx_t blk = (blockidx_t)current_dir_offset;
                
                if (i >= 8) {
                    blk = get_next_block(blk);
                }
                
                int index_in_block = i % 8;
                entry_disk_offset = SFS_DATA_OFF + (blk * SFS_BLOCK_SIZE) + (index_in_block * sizeof(struct sfs_entry));
                
                disk_read(&entry, sizeof(struct sfs_entry), entry_disk_offset);
            }

            if (strlen(entry.filename) > 0 && strcmp(entry.filename, token) == 0) {
                found = 1;
                if (ret_entry) *ret_entry = entry;
                if (ret_entry_off) *ret_entry_off = entry_disk_offset;
                
                token = strtok(NULL, "/");
                
                if (token != NULL) {
                    if (!(entry.size & SFS_DIRECTORY)) {
                        free(copy);
                        return -ENOTDIR;
                    }
                    current_dir_offset = entry.first_block;
                    current_dir_entries = SFS_DIR_NENTRIES;
                }
                break;
            }
        }

        if (!found) {
            free(copy);
            return -ENOENT;
        }
    }

    free(copy);
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
    int res = 0;
    log("getattr %s\n", path);
    memset(st, 0, sizeof(struct stat));
    st->st_uid = getuid();
    st->st_gid = getgid();
    st->st_atime = time(NULL);
    st->st_mtime = time(NULL);

    if (strcmp(path, "/") == 0) {
        st->st_mode = S_IFDIR | 0755;
        st->st_nlink = 2;
        return 0;
    }

    struct sfs_entry entry;
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
    (void)offset;
    (void)fi;

    log("readdir %s\n", path);

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    struct sfs_entry dir_entry;
    unsigned int start_offset_on_disk = 0;
    int num_entries = 0;

    if (strcmp(path, "/") == 0) {
        start_offset_on_disk = SFS_ROOTDIR_OFF;
        num_entries = SFS_ROOTDIR_NENTRIES;
        
        for (int i = 0; i < num_entries; i++) {
             struct sfs_entry entry;
             disk_read(&entry, sizeof(struct sfs_entry), start_offset_on_disk + i * sizeof(struct sfs_entry));
             
             if (strlen(entry.filename) != 0) {
                 filler(buf, entry.filename, NULL, 0);
             }
        }

    } else {
        int res = get_entry(path, &dir_entry, NULL);

        if (res != 0){
            return res;
        }

        if (!(dir_entry.size & SFS_DIRECTORY)){
            return -ENOTDIR;
        }

        blockidx_t current_blk = dir_entry.first_block;
        
        while (current_blk != SFS_BLOCKIDX_END && current_blk != SFS_BLOCKIDX_EMPTY) {
            
            for (int i=0; i < 8; i++) {
                struct sfs_entry entry;
                off_t addr = SFS_DATA_OFF + (current_blk * SFS_BLOCK_SIZE) + (i * sizeof(struct sfs_entry));
                disk_read(&entry, sizeof(struct sfs_entry), addr);

                if (strlen(entry.filename) != 0) {
                    filler(buf, entry.filename, NULL, 0);
                }
            }
            current_blk = get_next_block(current_blk);
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

    if (entry.size & SFS_DIRECTORY){
        return -EISDIR;
    }

    size_t file_size = entry.size & SFS_SIZEMASK;

    if (offset >= file_size){
        return 0;
    }

    if (offset + size > file_size){
        size = file_size - offset;
    }

    blockidx_t current_blk = entry.first_block;
    size_t bytes_read = 0;

    while (offset >= SFS_BLOCK_SIZE) {
        if (current_blk == SFS_BLOCKIDX_END){
            break;
        }
        current_blk = get_next_block(current_blk);
        offset -= SFS_BLOCK_SIZE;
    }

    while (bytes_read < size && current_blk != SFS_BLOCKIDX_END && current_blk != SFS_BLOCKIDX_EMPTY) {
        size_t to_read = SFS_BLOCK_SIZE - offset;
        if (to_read > size - bytes_read){
            to_read = size - bytes_read;
        }

        off_t disk_addr = SFS_DATA_OFF + (current_blk * SFS_BLOCK_SIZE) + offset;
        disk_read(buf + bytes_read, to_read, disk_addr);

        bytes_read += to_read;
        offset = 0;
        current_blk = get_next_block(current_blk);
    }

    return bytes_read;
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
    char *last_slash = strrchr(copy, '/');
    if (!last_slash){ 
        free(copy); 
        return -EINVAL; 
    }
    
    *last_slash = '\0';
    char *new_name = last_slash + 1;
    char *parent_path = copy;
    
    if (strlen(parent_path) == 0){
        parent_path = "/";
    }

    if (strlen(new_name) > SFS_FILENAME_MAX - 1) {
        free(copy);
        return -ENAMETOOLONG;
    }

    struct sfs_entry parent_entry;
    unsigned int parent_disk_off = 0;
    int is_root = (strcmp(parent_path, "/") == 0);

    if (!is_root) {
        if (get_entry(parent_path, &parent_entry, &parent_disk_off) != 0) {
            free(copy);
            return -ENOENT;
        }
        if (!(parent_entry.size & SFS_DIRECTORY)) {
            free(copy);
            return -ENOTDIR;
        }
    }

    int max_entries = is_root ? SFS_ROOTDIR_NENTRIES : SFS_DIR_NENTRIES;
    blockidx_t p_blk = is_root ? 0 : parent_entry.first_block;
    
    unsigned int empty_slot_addr = 0;
    int found_slot = 0;

    int entries_checked = 0;

    while(entries_checked < max_entries) {
        
        for(int i=0; i<8; i++) {

            if (is_root && entries_checked >= SFS_ROOTDIR_NENTRIES){
                break;
            }
            
            off_t addr;

            if (is_root) {
                addr = SFS_ROOTDIR_OFF + (entries_checked * sizeof(struct sfs_entry));
            } else {
                addr = SFS_DATA_OFF + (p_blk * SFS_BLOCK_SIZE) + (i * sizeof(struct sfs_entry));
            }

            struct sfs_entry temp;
            disk_read(&temp, sizeof(struct sfs_entry), addr);
            
            if (strlen(temp.filename) > 0 && strcmp(temp.filename, new_name) == 0) {
                free(copy);
                return -EEXIST;
            }

            if (strlen(temp.filename) == 0 && found_slot == 0) {
                empty_slot_addr = addr;
                found_slot = 1;
            }

            entries_checked++;
        }

        if (is_root){
            break;
        }

        p_blk = get_next_block(p_blk);

        if (p_blk == SFS_BLOCKIDX_END){
            break;
        }
    }

    if (!found_slot) {
        free(copy);
        return -ENOSPC;
    }

    blockidx_t b1 = find_free_block();

    if (b1 == SFS_BLOCKIDX_END){ 
        free(copy); 
        return -ENOSPC; 
    }
    
    set_next_block(b1, SFS_BLOCKIDX_END); 
    
    blockidx_t b2 = find_free_block();
    if (b2 == SFS_BLOCKIDX_END) { 
        set_next_block(b1, SFS_BLOCKIDX_EMPTY);
        free(copy); 
        return -ENOSPC; 
    }

    set_next_block(b1, b2);
    set_next_block(b2, SFS_BLOCKIDX_END);

    struct sfs_entry new_entry;
    memset(&new_entry, 0, sizeof(struct sfs_entry));
    strncpy(new_entry.filename, new_name, SFS_FILENAME_MAX - 1);
    new_entry.first_block = b1;
    new_entry.size = SFS_DIRECTORY;

    disk_write(&new_entry, sizeof(struct sfs_entry), empty_slot_addr);
    
    struct sfs_entry empty_entries[8];
    memset(empty_entries, 0, sizeof(empty_entries));
    for(int k=0; k<8; k++) empty_entries[k].first_block = SFS_BLOCKIDX_EMPTY;

    disk_write(empty_entries, SFS_BLOCK_SIZE, SFS_DATA_OFF + (b1 * SFS_BLOCK_SIZE));
    disk_write(empty_entries, SFS_BLOCK_SIZE, SFS_DATA_OFF + (b2 * SFS_BLOCK_SIZE));

    free(copy);
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
    unsigned int entry_addr;

    if (get_entry(path, &entry, &entry_addr) != 0){
        return -ENOENT;
    }

    if (!(entry.size & SFS_DIRECTORY)){
        return -ENOTDIR;
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
        blk = get_next_block(blk);
    }

    blk = entry.first_block;

    while(blk != SFS_BLOCKIDX_END && blk != SFS_BLOCKIDX_EMPTY) {
        blockidx_t next = get_next_block(blk);
        set_next_block(blk, SFS_BLOCKIDX_EMPTY);
        blk = next;
    }

    struct sfs_entry empty_entry;
    memset(&empty_entry, 0, sizeof(struct sfs_entry));
    empty_entry.first_block = SFS_BLOCKIDX_EMPTY;
    disk_write(&empty_entry, sizeof(struct sfs_entry), entry_addr);

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
    unsigned int entry_addr;

    if (get_entry(path, &entry, &entry_addr) != 0){
        return -ENOENT;
    }

    if (entry.size & SFS_DIRECTORY){
        return -EISDIR;
    }

    blockidx_t blk = entry.first_block;

    while(blk != SFS_BLOCKIDX_END && blk != SFS_BLOCKIDX_EMPTY) {
        blockidx_t next = get_next_block(blk);
        set_next_block(blk, SFS_BLOCKIDX_EMPTY);
        blk = next;
    }

    struct sfs_entry empty;
    memset(&empty, 0, sizeof(struct sfs_entry));
    empty.first_block = SFS_BLOCKIDX_EMPTY;
    disk_write(&empty, sizeof(struct sfs_entry), entry_addr);

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
    char *last_slsh = strrchr(copy, '/');

    *last_slsh = '\0';
    char *new_name = last_slsh + 1;
    char *parent = copy;
    
    if (strlen(parent) == 0){
        parent = "/";
    }

    if (strlen(new_name) > SFS_FILENAME_MAX - 1) { 
        free(copy); 
        return -ENAMETOOLONG; 
    }

    struct sfs_entry parent_entry;

    int is_root = (strcmp(parent, "/") == 0);

    if (!is_root) {
        if (get_entry(parent, &parent_entry, NULL) != 0) { 
            free(copy); 
            return -ENOENT; 
        }
    }

    blockidx_t p_blk = is_root ? 0 : parent_entry.first_block;

    int max_entries = is_root ? SFS_ROOTDIR_NENTRIES : SFS_DIR_NENTRIES;

    unsigned int empty_slot = 0;

    int found = 0;

    int checked = 0;

    while(checked < max_entries) {
        for(int i=0; i<8; i++) {
             if (is_root && checked >= SFS_ROOTDIR_NENTRIES){
                break;
             }
             off_t addr = is_root ? (SFS_ROOTDIR_OFF + checked * sizeof(struct sfs_entry)) : (SFS_DATA_OFF + p_blk * SFS_BLOCK_SIZE + i * sizeof(struct sfs_entry));
             struct sfs_entry t;
             disk_read(&t, sizeof(t), addr);

             if (strlen(t.filename) > 0 && strcmp(t.filename, new_name) == 0) {
                 free(copy); 
                 return -EEXIST;
             }
             if (strlen(t.filename) == 0 && !found) {
                 empty_slot = addr;
                 found = 1;
             }
             checked++;
        }

        if (is_root){ 
            break;
        }

        p_blk = get_next_block(p_blk);

        if (p_blk == SFS_BLOCKIDX_END){
            break;
        }
    }

    if (!found) { 
        free(copy); 
        return -ENOSPC; 
    }

    struct sfs_entry new_file;
    memset(&new_file, 0, sizeof(new_file));
    strncpy(new_file.filename, new_name, SFS_FILENAME_MAX - 1);
    new_file.first_block = SFS_BLOCKIDX_END;
    new_file.size = 0;

    disk_write(&new_file, sizeof(new_file), empty_slot);
    free(copy);
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

//not modify
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

//not modify
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

//not modify
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


//not modify
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