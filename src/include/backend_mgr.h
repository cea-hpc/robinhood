#ifndef _BACKEND_MGR_H
#define _BACKEND_MGR_H

typedef struct backend_t
{
	/* are archive asynchronous? */
	unsigned int async_archive:1;

	/* does the backend supports remove operation */
	unsigned int rm_support:1;

	/* does the whole system supports release/copyback operations */
	unsigned int release_support:1;
} backend_t;

typedef struct backend_config_t
{
    char root[RBH_PATH_MAX];
    char mnt_type[RBH_NAME_MAX];
    char action_cmd[RBH_PATH_MAX];
#ifdef HAVE_SHOOK
    char shook_cfg[RBH_PATH_MAX];
#endif
    unsigned int copy_timeout; /* 0=disabled */
    bool xattr_support;
    bool check_mounted;
    bool archive_symlinks;
    bool sync_archive_data;
    bool compress;
    bool sendfile;
} backend_config_t;

int            SetDefault_Backend_Config( void *module_config, char *msg_out );
int            Read_Backend_Config(config_file_t config,
                                   void *module_config, char *msg_out,
                                   bool for_reload);
int            Reload_Backend_Config( void *module_config );
int            Write_Backend_ConfigTemplate( FILE * output );
int            Write_Backend_ConfigDefault( FILE * output );

extern backend_t backend;

int Backend_Start( backend_config_t * config, int flags );
int Backend_Stop( void );

#endif
