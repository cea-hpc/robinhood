/** perform a standard unlink() action */
int common_unlink(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                  const char *hints, post_action_e *after,
                  db_cb_func_t db_cb_fn, void *db_cb_arg);

/** just log it! */
int common_log(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
               const char *hints, post_action_e *after,
               db_cb_func_t db_cb_fn, void *db_cb_arg);

/** standard copy of file contents and its attributes */
int common_copy(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                const char *hints, post_action_e *after,
                db_cb_func_t db_cb_fn, void *db_cb_arg);

/** copy file contents using sendfile() */
int common_sendfile(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                    const char *hints, post_action_e *after,
                    db_cb_func_t db_cb_fn, void *db_cb_arg);

/** copy and compress file contents */
int common_gzip(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                const char *hints, post_action_e *after,
                db_cb_func_t db_cb_fn, void *db_cb_arg);
