AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=libpolicies.la

libpolicies_la_SOURCES=policy_matching.c policy_loader.c policy_triggers.c \
                       policy_run_cfg.c status_manager.c run_policies.h policy_run.c \
                       policy_actions.c 

libpolicies_la_LIBADD=../modules/libmodule_common.la

if LUSTRE_HSM
libpolicies_la_LIBADD+=../modules/libmodule_lhsm.la
endif

if HSM_LITE
libpolicies_la_LIBADD+=../modules/libmodule_backup.la
endif

if SHOOK
libpolicies_la_SOURCES+=shook.c
endif
