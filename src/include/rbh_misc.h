/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2004-2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * \file  rbh_misc.h
 * \brief Common tools for parsing, converting, checking FS status...
 */
#ifndef _ROBINHOOD_MISC_H
#define _ROBINHOOD_MISC_H

#include "xplatform_print.h"
#include "list_mgr.h"
#include <sys/stat.h>
#ifndef __FreeBSD__
#include <sys/vfs.h>
#endif
#include <dirent.h>
#include <stdint.h>
#include <stdbool.h>
#include <glib.h>
#include <semaphore.h>
#include <unistd.h>
#include "rbh_logs.h"

/* displaying FID */
#ifndef _HAVE_FID
#undef DFID
#undef DFID_NOBRACE
#undef PFID
#undef SFID
#undef RFID

#define DFID "%"PRIX64"/%"PRI_STI
#define DFID_NOBRACE DFID
#define PFID(_pid) (_pid)->fs_key, (_pid)->inode
#define SFID "0X%"PRIX64"/%"PRI_STI
#define RFID(_pid) &((_pid)->fs_key), &((_pid)->inode)
#define FID_SCAN_CNT 2
#define RBH_FID_LEN 64

#else
#define FID_SCAN_CNT 3
#endif

/**
 * Common info
 */
#define DEFAULT_CFG_VAR "RBH_CFG_DEFAULT"

/**
 *  Miscellaneous parsing macros
 */
#define EMPTY_STRING(s)       ((s)[0] == '\0')
#define FINAL_SLASH(s)        ((strlen(s) > 1) && (s[strlen(s)-1] == '/'))
#define SLASH_IN(s)           (strchr(s, '/') != NULL)
#define REMOVE_FINAL_SLASH(s) ((s)[strlen(s)-1] = '\0')
#define IS_ABSOLUTE_PATH(s)   (((s)[0]) && ((s)[0] == '/'))

#define ANY_LEVEL_MATCH(_s_)  (strstr(_s_, "**") != NULL)

#define WILDCARDS_IN(s) (strchr(s, '*') || strchr(s, '?') || strchr(s, '[') \
                         || strchr(s, ']') || strchr(s, '{') || strchr(s, '}'))
#define STAR_SLASH_BEGIN(s) (((s)[0] == '*') && ((s)[1] == '/'))

#define GSTRING_SAFE(_g) (((_g) == NULL || ((_g)->str == NULL)) ? "" : \
                            (_g)->str)
#define GSTRING_EMPTY(_g) (EMPTY_STRING(GSTRING_SAFE(_g)))

#define bool2str(_b_)   ((_b_) ? "yes" : "no")

#define rh_strncpy(_s1, _s2, _sz) do { \
        if (_sz > 0) {                 \
            strncpy(_s1, _s2, _sz-1);  \
            (_s1)[_sz-1] = '\0';       \
        }                              \
    } while (0)

/**
 *  Other useful definitions
 */
#define MIN2(_a_, _b_) ((_a_) < (_b_) ? (_a_) : (_b_))
#define MIN3(_a_, _b_, _c_) (MIN2(MIN2((_a_) , (_b_)) , (_c_)))

#define MAX2(_a_, _b_) ((_a_) > (_b_) ? (_a_) : (_b_))
#define MAX3(_a_, _b_, _c_) (MAX2(MAX2((_a_) , (_b_)) , (_c_)))

#ifndef P
#define P(_mutex_) pthread_mutex_lock(&(_mutex_))
#endif
#ifndef V
#define V(_mutex_) pthread_mutex_unlock(&(_mutex_))
#endif

#ifdef __GNUC__
#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)

/**
 * GCC hint for unreachable code
 * See: https://gcc.gnu.org/onlinedocs/gcc/Other-Builtins.html
 */
#define UNREACHED       __builtin_unreachable

#else
#define likely(x)       (x)
#define unlikely(x)     (x)
#endif

#ifndef MEMBER_SIZE
#define MEMBER_SIZE(_type, _member) sizeof(((_type *)0)->_member)
#endif

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(_array) (sizeof(_array) / sizeof((_array)[0]))
#endif

/**
 * Send a mail
 */
int SendMail(const char *recipient, const char *subject, const char *message);

/**
 * Search for Robinhood config file
 */
int SearchConfig(const char *cfg_in, char *cfg_out, bool *changed,
                 char *unmatched, size_t max_len);

/**
 * This function is blocking as long as the lock file is present.
 * Optionaly updates an action timestamp, at each test.
 */
void TestLockFile(time_t *p_last_action);

/**
 * Convert a POSIX attribute structure (returned by lstat)
 * to a robinhood attribute set.
 * @param size_info indicates if size info is set in the stat structure.
 */
void stat2rbh_attrs(const struct stat *p_inode, attr_set_t *p_attr_set,
                    bool size_info);

/**
 * Convert a robinhood attribute set to a posix struct stat.
 */
void rbh_attrs2stat(const attr_set_t *p_attr_set, struct stat *p_inode);

/* convert file mode to DB type string */
const char *mode2type(mode_t mode);

/**
 * Retrieve the name associated to a user (or the text representation of its
 * uid if the user doesn't exist)
 */
char *uid2str(uid_t uid, char *username);

/**
 * Retrieve the name associated to a user (or the text representation of its
 * uid if the user doesn't exist).
 */
char *gid2str(gid_t gid, char *groupname);

/**
 * Check mount point and FS type.
 * Also return the associated device number.
 * (for STAY_IN_FS security option).
 */
int check_fs_info(const char *path, const char *expected_type,
                  dev_t *p_fs_dev, char *fsname_out,
                  bool check_mounted, bool save_fs);

/**
 * Initialize filesystem access and retrieve current devid/fs_key
 * - global_config must be set
 * - initialize mount_point, fsname and dev_id
 */
int InitFS(void);

/**
 * This is to be called after a dev_id change was detected
 * return 0 if fskey is unchanged and update mount_point, fsname and dev_id
 * else, return -1
 */
int ResetFS(void);

/**
 *  Check that FS path is the same as the last time.
 */
int CheckLastFS(void);

/* retrieve FS info */
const char *get_mount_point(unsigned int *plen);
#ifdef _HAVE_FID
const char *get_fid_dir(void);
const char *get_dot_lustre_dir(void);
const entry_id_t *get_dot_lustre_fid(void);
const entry_id_t *get_fid_fid(void);
#endif

const char *get_fsname(void);
dev_t get_fsdev(void);
uint64_t get_fskey(void);
const entry_id_t *get_root_id(void);

/**
 * extract relative path from full path.
 */
int relative_path(const char *fullpath, const char *root, char *rel_path);

/**
 * create parent directory, and return its id (even if it already exists).
 */
int create_parent_of(const char *child_path, entry_id_t *p_parent_id);

/**
 * create an object with the given attributes.
 */
int create_from_attrs(const attr_set_t *attrs_in,
                      attr_set_t *attrs_out,
                      entry_id_t *new_id, bool overwrite, bool setstripe);

enum path_check_return {
    PCR_NO_CHANGE = 0, /**< no attribute updated */
    PCR_UPDATED,    /**< at least an attribute is updated. */
    PCR_ORPHAN,
};
/**
 * Update parent id, name and/or full path, according to attr_mask.
 * @return one of the values defined in enum path_check_return.
 */
enum path_check_return path_check_update(const entry_id_t *p_id,
                                         const char *fid_path,
                                         attr_set_t *p_attrs,
                                         attr_mask_t attr_mask);

#ifdef _LUSTRE

/** initialize access to lustre */
int Lustre_Init(void);

/** Retrieve stripe info for a file */
int File_GetStripeByPath(const char *entry_path, stripe_info_t *p_stripe_info,
                         stripe_items_t *p_stripe_items);

int File_GetStripeByDirFd(int dirfd, const char *fname,
                          stripe_info_t *p_stripe_info,
                          stripe_items_t *p_stripe_items);
/**
 * check if a file has data on the given OST.
 */
bool DataOnOST(size_t fsize, unsigned int ost_index,
               const stripe_info_t *sinfo, const stripe_items_t *sitems);

/**
 * compute the number of blocks of a file on a given OST.
 */
blkcnt_t BlocksOnOST(blkcnt_t blocks, unsigned int ost_index,
                     const stripe_info_t *sinfo,
                     const stripe_items_t *sitems);

#ifdef HAVE_LLAPI_GETPOOL_INFO
/** Create a file with the given stripe information */
int CreateStriped(const char *path, const stripe_info_t *old_stripe,
                  int overwrite);
int CreateWithoutStripe(const char *path, mode_t mode, int overwrite);
#endif

#ifdef _HAVE_FID
int BuildFidPath(const entry_id_t *p_id /* IN */ , char *path /* OUT */);
int Lustre_GetFullPath(const entry_id_t *p_id, char *fullpath,
                       unsigned int len);
int Lustre_GetFidFromPath(const char *fullpath, entry_id_t *p_id);
int Lustre_GetFidByFd(int fd, entry_id_t *p_id);
int Lustre_GetNameParent(const char *path, int linkno,
                         lustre_fid *pfid, char *name, int namelen);

#define FID_IS_ZERO(_pf) (((_pf)->f_seq == 0) && ((_pf)->f_oid == 0))

#endif

#ifdef HAVE_CHANGELOGS
/* if the FS has changelogs, define function for converting changelog time */
static inline time_t cltime2sec(uint64_t cltime)
{
    /* extract secs from time field */
    return cltime >> 30;
}

static inline unsigned int cltime2nsec(uint64_t cltime)
{
    /* extract nanosecs: */
    return cltime & ((1 << 30) - 1);
}
#endif

/** Retrieve OST usage info ('ost df') */
int Get_OST_usage(const char *fs_path, unsigned int ost_index,
                  struct statfs *ost_statfs);

#ifdef HAVE_LLAPI_GETPOOL_INFO
/** Retrieve pool usage info */
int Get_pool_usage(const char *poolname, struct statfs *pool_statfs);
#endif

/**
 * Retrieve file information from MDS.
 * @return 0 on success, -errno on error.
 */
int lustre_mds_stat(const char *fullpath, int parentfd, struct stat *inode);
#ifdef _HAVE_FID
int lustre_mds_stat_by_fid(const entry_id_t *p_id, struct stat *inode);
#endif

#ifndef _MDT_SPECIFIC_LOVEA
/**
 * build LOVEA buffer from stripe information
 * @return size of significant information in buffer.
 */
ssize_t BuildLovEA(const entry_id_t *p_id, const attr_set_t *p_attrs,
                   void *buff, size_t buf_sz);
#endif

#endif /* lustre */

/**
 * Shoot a thread.
 */
int TerminateThread(pthread_t thread_id);

/**
 * Clean termination of the daemon + display message in log
 */
void Exit(int error_code);

/**
 * Format functions
 */
char *FormatFileSize(char *buff, size_t str_sz, uint64_t file_size);
char *FormatDuration(char *buff, size_t str_sz, time_t duration);
char *FormatDurationFloat(char *buff, size_t str_sz, time_t duration);

#ifdef _LUSTRE
/**
 * Append a list of stripes to a GString.
 * @param[in,out] str  Allocated GString.
 */
void append_stripe_list(GString *str, const stripe_items_t *p_stripe_items,
                        bool brief);
#endif

/*
 * Parsing functions
 */

/**
 * Convert a string to a boolean
 * @return -1 on error.
 */
int str2bool(const char *str);

/**
 * Convert a string to an integer
 * @return -1 on error.
 */
static inline int str2int(const char *str)
{
    char suffix[256];
    int nb_read, value;

    if (str == NULL)
        return -1;

    nb_read = sscanf(str, "%d%s", &value, suffix);

    if (nb_read <= 0)
        return -1;  /* invalid format */

    if ((nb_read == 1) || (suffix[0] == '\0'))
        return value;   /* no suffix => 0K */
    else
        return -1;
}

/**
 * Convert a string to a long integer
 * @return -1 on error.
 */
long long str2bigint(const char *str);

/**
 * Convert a string to a duration in seconds
 * @return -1 on error.
 */
int str2duration(const char *str);

/**
 * Convert a string to a size (in bytes)
 * @return -1 on error.
 */
uint64_t str2size(const char *str);

/** parse date/time yyyymmdd[HH[MM[SS]]] */
time_t str2date(const char *str);

/** convert mode to rwxrwxrwx string */
const char *mode_string(mode_t mode, char *buf);

/**
 *  Print attributes to a GString.
 *  @param[in,out] str           Allocated GString (contents is overwritten).
 *  @param         overide_mask  If != 0, override attrmask with this one.
 *  @param         brief         Brief notation for diff output.
 */
void print_attrs(GString *str, const attr_set_t *p_attr_set,
                 attr_mask_t overide_mask, bool brief);

/**
 *  Apply attribute changes
 *  \param change_mask mask of attributes to be changed
 */
int ApplyAttrs(const entry_id_t *p_id,
               const attr_set_t *p_attr_new, const attr_set_t *p_attr_old,
               attr_mask_t change_mask, bool dry_run);

/** Compute greatest common divisor (GCD) of 2 numbers */
unsigned int gcd(unsigned int x, unsigned int y);

/** Ensure that the thread is suspended for a given amount
 * of time, event if the process gets interrupts.
 */
void rh_sleep(unsigned int seconds);

/* signal safe semaphore ops with error logging */
/* man (3) sem_wait/sem_post: on error, the value of the semaphore is left
 * unchanged */
static inline void sem_wait_safe(sem_t *sem)
{
    while (sem_wait(sem)) {
        if (errno != EINTR && errno != EAGAIN)
            DisplayLog(LVL_CRIT, "sem", "ERROR: sem_wait operation failed: %s", \
                       strerror(errno));
    }
}

static inline void sem_post_safe(sem_t *sem)
{
    while (sem_post(sem)) {
            if (errno != EINTR && errno != EAGAIN)
                DisplayLog(LVL_CRIT, "sem",
                           "ERROR: sem_post operation failed: %s",
                           strerror(errno));
    }
}

/**
 * Interuptible sleep.
 * returns when _v != 0.
 */
#define rh_intr_sleep(_s, _v) do { \
        unsigned int _i; for (_i = 0; (_i < _s) && !(_v); _i++) rh_sleep(1); \
    } while (0)

#define rh_usleep(_usec) usleep(_usec)

/** replace a pattern in a string with another sub-string
 * \param str_in_out must be large enough to receive
 *  the resulting string, and cannot exceed 1024.
 */
int str_subst(char *str_in_out, const char *to_be_replaced,
              const char *replacement);

static inline void subst_char(char *str, char c1, char c2)
{
    char *curr;
    for (curr = str; *curr != '\0'; curr++)
        if (*curr == c1)
            *curr = c2;
}

/** escape every special character in a regex
 *
 * \param dest      the string to copy the escaped regex to
 * \param dest_size the size of dest (including the terminating char)
 * \param src       the null terminated string representing the regex to
 *                  escape
 * \param charset   a string that contains every char to escape
 *
 * \return          0 on success, -error_code on error
 */
int str_escape_charset(char *dest, size_t dest_size, const char *src,
                       char *charset);

/**
 * Callback function to parse command output.
 * The function can freely modify line contents
 * without impacting program working.
 *
 * \param[in,out] cb_arg    argument passed to command_call
 * \param[in]     line      the line to be parsed
 * \param[in]     size      size of the line buffer
 * \param[in]     stream    fileno of the stream the line comes from
 */
typedef int (*parse_cb_t) (void *cb_arg, char *line, size_t size, int stream);

/**
 * Callback function for execute_shell_command() that redirects stderr to
 * to robinhood log.
 * @param arg[in] arg   Desired log level, cast to (void *).
 */
int cb_stderr_to_log(void *arg, char *line, size_t size, int stream);

/**
 * Execute a shell command and call cb_func for each output line
 * (ignore output if cb_func is null).
 */
int execute_shell_command(char **cmd, parse_cb_t cb_func, void *cb_arg);

/**
 * Quote an argument for shell commande line.
 * The caller must free the returned string. */
char *quote_shell_arg(const char *arg);

/**
 * Get the mask for placeholders in the given string.
 * @param[in] str string to be parsed.
 * @param[in] str_descr string context description to be displayed in
 *                      error messages (e.g. "cfg_block::foo_param line 42").
 * @param[our] err this boolean is set to true if an syntax in encountered.
 */
attr_mask_t params_mask(const char *str, const char *str_descr, bool *err);

struct sm_instance;
/**
 * Replace special parameters {cfg}, {fspath}, ... in the given string.
 * Result string is allocated by the function and must be released using
 * g_free().
 * @param[in] str_in    Input string with {} placeholders.
 * @param[in] str_descr String description (for logging).
 * @param[in] p_id      Pointer to entry id (if the command is executed on an
 *                      entry).
 * @param[in] p_attrs   Pointer to entry attrs (if the command is executed on an
 *                      entry).
 * @param[in] params    List of action parameters.
 * @param[in] subst_array   char** of param1, value1,
 *                      param2, value2, ..., NULL, NULL.
 * @param[in] smi       When applying a policy, pointer to the current status
 *                      manager instance.
 * @param[in] quote     If true, escape and quote the replaced values as shell
 *                      arguments.
 * @param[in] strict_braces If true, only allow braces for variable names like
 *                      {var}.
 */
char *subst_params(const char *str_in,
                   const char *str_descr,
                   const entry_id_t *p_id,
                   const attr_set_t *p_attrs,
                   const action_params_t *params,
                   const char **subst_array,
                   const struct sm_instance *smi,
                   bool quote, bool strict_braces);

/**
 * Replace special parameters {cfg}, {fspath}, ... in the given string.
 * Result is formated as argc/argv for shell by the function in cmd_out,
 * cmd_out.av must be released using g_strfreev().
 * returns 0 on success, -errno on error.
 * @param[in] cmd_in    Input command, av contains strings with {} placeholders.
 * @param[in] str_descr String description (for logging).
 * @param[in] p_id      Pointer to entry id (if the command is executed on an
 *                      entry).
 * @param[in] p_attrs   Pointer to entry attrs (if the command is executed on an
 *                      entry).
 * @param[in] params    List of action parameters.
 * @param[in] subst_array char** of param1, value1, param2, value2, ...,
 *                      NULL, NULL.
 * @param[in] smi       When applying a policy, pointer to the current status
 *                      manager instance.
 * @param[in] strict_braces If true, only allow braces for variable names like
 *                      {var}.
 * @param[out] cmd_out  parsed argc/argv after subst
 */
int subst_shell_params(char **cmd_in,
                       const char *str_descr,
                       const entry_id_t *p_id,
                       const attr_set_t *p_attrs,
                       const action_params_t *params,
                       const char **subst_array,
                       const struct sm_instance *smi,
                       bool strict_braces, char ***cmd_out);

/**
 * concatenate a string array into a string
 * The returned string must be freed with free().
 */
char *concat_cmd(char **argv);

/** compare commands */
int compare_cmd(char **c1, char **c2);

/** convert to upper case */
void upperstr(char *str);
/** convert to lower case */
void lowerstr(char *str);

/** recursively create a directoy and return its id */
int mkdir_recurse(const char *full_path, mode_t mode, entry_id_t *dir_id);

/**
 * Get id for the given path.
 * @param[in] st stat struct if available.
 */
int path2id(const char *path, entry_id_t *id, const struct stat *st);

int set_uid_val(const char *username, db_type_u *val);
int set_gid_val(const char *groupname, db_type_u *val);
const char *id_as_str(db_type_u *val);

#endif
