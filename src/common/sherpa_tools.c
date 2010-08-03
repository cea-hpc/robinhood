/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#define SHERPA_TAG "SHERPA"

#include "list_mgr.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "xplatform_print.h"
#include <DiversCache.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>

#define MAX_CACHE_DIR 32
static int tab_cache_size = 0;
static struct EntreeCache tab_cache[MAX_CACHE_DIR];

int InitSherpa(char * sherpa_config, char * log_file, char * report_file )
{
    char msg[1024];
    int rc;

    if ( putenv("CODE=rbh-sherpa") )
        DisplayLog( LVL_MAJOR, SHERPA_TAG, "Error setting CODE environment variable" );
        
    InitialiseLogFichiers( "sherpa", log_file, report_file );

    /* set default configuration */
    ConfigParDefaut();
    
    /* read config file */
    rc = LitConfigurationSherpa( sherpa_config, msg );
    if (rc)
    {
        DisplayLog( LVL_CRIT, SHERPA_TAG, "Error reading SHERPA configuration: %s", msg );
        return rc;
    }

    memset( tab_cache, 0, sizeof(tab_cache) );

    /* Load list of cache directories */
    tab_cache_size = sizeof (tab_cache) / sizeof (struct EntreeCache);

    tab_cache_size = MAX_CACHE_DIR; 
    rc = LitFichierMontage (NULL, NULL, "PROT", tab_cache, &tab_cache_size, NULL, NULL );

    if ( rc != SUCCES )
    {
        DisplayLog( LVL_CRIT, SHERPA_TAG, "Error %d reading mount table of cache directories", rc);
        return rc;
    }

    return 0;
}

/* get the reference and cache definition for an entry */
int Sherpa_GetEntryDescription( char * cache_path, /* in */
                                char * reference_path,  /* out */
                                char * relative_path, /* out */
                                struct EntreeCache * cache_info ) /* out */
{
    int index;
    int index_found = -1;
    
    int len;
    int matching_len = 0;

    /* get the longest matching entry */
    for (index = 0; index < tab_cache_size; index++)
    {
        len = strlen( tab_cache[index].base_cache );

        /* Root is a cache */
        if ( (len > matching_len ) && !strcmp( tab_cache[index].base_cache, "/" ) )
        {
            char * start_rel = cache_path+len;

            index_found = index;
            matching_len = len;
            while( *start_rel == '/' ) start_rel++;
            strcpy( relative_path, start_rel );
            sprintf( reference_path, "%s/%s", tab_cache[index].base_reference, relative_path );

            DisplayLog( LVL_DEBUG, SHERPA_TAG,
                        "Root mountpoint matches %s, ref=%s, rel_path=%s, ref_path=%s",
                        cache_path, tab_cache[index].base_reference, relative_path,
                        reference_path );
        }
        /* in other cases, the entry must be <cache>/<smthg> or <cache>\0 */
        else if ( ( len > matching_len )
                  && !strncmp( cache_path, tab_cache[index].base_cache, len )
                  && ( ( cache_path[len] == '/' ) || ( cache_path[len] == '\0' ) ) )
        {
            char * start_rel = cache_path+len;
            matching_len = len;
            index_found = index;
            while( *start_rel == '/' ) start_rel++;

            strcpy( relative_path, start_rel );
            sprintf( reference_path, "%s/%s", tab_cache[index].base_reference, relative_path );
            
            DisplayLog( LVL_FULL, SHERPA_TAG, "%s is in cache %s, ref=%s, rel_path=%s, ref_path=%s",
                        cache_path, tab_cache[index].base_cache, tab_cache[index].base_reference, relative_path,
                        reference_path );
        }
    }

    /* no matching cache found */
    if ( index_found == -1 )
    {
        DisplayLog( LVL_MAJOR, SHERPA_TAG, "Warning: no matching cache found for '%s'", cache_path );
        return -ENOENT;
    }

    *cache_info = tab_cache[index_found];

    if ( cache_info->type_montage_reference == REF_NON_DISPO )
    {
        DisplayLog( LVL_VERB, SHERPA_TAG, "%s is in cache %s, reference path is %s, "
                    "that was not mounted when the daemon started",
                    cache_path, cache_info->base_cache, cache_info->base_reference );
        return -EHOSTDOWN;
    }
    else
    {
        DisplayLog( LVL_DEBUG, SHERPA_TAG, "%s is in cache %s, ref=%s, relative_path=%s, ref_path=%s",
                    cache_path, cache_info->base_cache, cache_info->base_reference, relative_path,
                    reference_path );
        return 0;
    }
}

static const char * cause2str( enum CausePurge why )
{
    switch( why )
    {
        case DEMANDE:
            return "user request";
        case DATE_ACCES:
            return "access time";
        case TYPE:
            return "bad type";
        case MODIF:
            return "modified";
        case SUPPRESSION:
            return "removed";
        case COPIE_INCOMPLETE:
            return "incomplete copy";
        case INCONNUE:
            return "unknown";
        default:
            return "?";
    }
}


/* get entry status and perform synchronization operations */
enum what_to_do SherpaManageEntry(const entry_id_t * p_id, attr_set_t * p_attrs)
{
    struct EntreeCache cache_info;
    StatutEntreeCache status_cache;
    StatutEntreeRef  status_ref;
    struct stat stat_ref, stat_cache, st_glob;
    TypeEntree type_ref, type_cache;
    char ref_path[1024];
    char cglob_path[1024];
    char relative_path[1024];
    char link_content[1024];

    int remove = FALSE;
    int upper_tranfer_unlock = FALSE;
    int update_md = FALSE;
    enum CausePurge why = INCONNUE;
    int rc;

#ifdef _HAVE_FID
    /* if the full path is not set, get it */ 
    if ( !ATTR_MASK_TEST( p_attrs, fullpath ) || EMPTY_STRING(ATTR( p_attrs, fullpath )) )
    {
        rc = Lustre_GetFullPath( p_id, ATTR(p_attrs, fullpath), 1024 );
        if ( (-rc == ENOENT) || (-rc == ESTALE) )
        {
           DisplayLog( LVL_DEBUG, SHERPA_TAG, "Fid "DFID" does not exist anymore (%s): removing entry from database.",
                       PFID(p_id), strerror(-rc) );
           return do_skip;
        }
        else if ( rc != 0 )
        {
           DisplayLog( LVL_MAJOR, SHERPA_TAG, "Can not get path for fid "DFID": (%d) %s",
                       PFID(p_id), rc, strerror(-rc) );
           return do_skip;
        }
        ATTR_MASK_SET( p_attrs, fullpath );
        ATTR_MASK_SET( p_attrs, path_update );
        ATTR( p_attrs, path_update ) = time(NULL);
    }
#endif
    if ( !ATTR_MASK_TEST( p_attrs, fullpath ) || EMPTY_STRING(ATTR( p_attrs, fullpath )) )
    {
        DisplayLog( LVL_CRIT, SHERPA_TAG,
                    "Entry path is needed to check its SHERPA status" );
        return do_skip;
    }

    /* get info about this entry  */

    /* 1) determine matching entry in reference */
    rc = Sherpa_GetEntryDescription( ATTR( p_attrs, fullpath ), ref_path, relative_path,
                                     &cache_info );

    /* the reference is not mounted */
    if ( rc == -EHOSTDOWN )
    {
        ATTR_MASK_SET( p_attrs, status );
        ATTR(p_attrs, status) = STATUS_UNKNOWN;
        return do_update;
    }
    else if (rc) /* other error */
        return do_skip;

    if ( config.attitudes.type_cache == CACHE_LOCAL )
        sprintf(cglob_path, "%s/%s", cache_info.base_cache_global, relative_path );

recheck:
    remove = FALSE;
    upper_tranfer_unlock = FALSE;
    update_md = FALSE;
    why = INCONNUE;

    /* 2) get entry status */

    rc = DetermineStatutEntree( config.attitudes.type_cache,
                                ATTR(p_attrs, fullpath), ref_path,
                                &status_cache, &status_ref,
                                &stat_cache, &stat_ref,
                                &type_cache, &type_ref,
                                link_content,
                                FALSE );
    if (rc)
    {
        DisplayLog( LVL_MAJOR, SHERPA_TAG, "Could not determine status for entry %s", ATTR(p_attrs, fullpath) );
        return do_skip;
    }

#ifndef _HAVE_FID
    /* check that entry_id is matching */
    if ( (stat_cache.st_ino != p_id->inode )
         || (stat_cache.st_dev != p_id->device ) )
        /* remove previous entry from DB */
        return do_rm;
#endif

    DisplayLog( LVL_DEBUG, SHERPA_TAG, "cache status: %s; reference status: %s",
                StatutCache2Str( status_cache ), StatutRef2Str( status_ref ) );

    /* update entry attributes */
    PosixStat2EntryAttr( &stat_cache, p_attrs, TRUE );
    ATTR_MASK_SET(p_attrs, md_update);
    ATTR(p_attrs, md_update) = time(NULL);

    if (!EMPTY_STRING(ref_path))
    {
        ATTR_MASK_SET(p_attrs, refpath);
        strcpy( ATTR(p_attrs, refpath), ref_path );
    }

    /* 3) perform action depending on entry status and attributes */

    switch( status_cache )
    {
        case STATUT_CACHE_MAUVAIS_TYPE:
            remove = TRUE;
            why = TYPE;
            break;

        case STATUT_CACHE_REF_ABSENTE:
            /* we keep the entry in the following situations:
             * - archive in use
             * - entry modified very recently
             */
            ATTR_MASK_SET( p_attrs, status );
            ATTR(p_attrs, status) = STATUS_NO_REF;

            if ( archive_utilisee(ATTR(p_attrs, fullpath),&stat_cache) )
            {
                DisplayLog(LVL_EVENT, SHERPA_TAG, "Archive entry %s has been recently accessed, keeping it in cache.", ATTR(p_attrs, fullpath) );
            }
            else if ( EntreeModifieeTresRecemment(&stat_cache) )
            {
                DisplayLog(LVL_EVENT, SHERPA_TAG, "Entry %s has been recently accessed, keeping it in cache.", ATTR(p_attrs, fullpath) );
            }
            else
            {
                DisplayLog(LVL_VERB, SHERPA_TAG, "Entry %s has no reference: it must be removed.", ATTR(p_attrs, fullpath) );
                remove = TRUE;
                why = SUPPRESSION;
            }
            break;

        case STATUT_CACHE_MD_OBSOLETE:
            update_md = TRUE;
            /* /!\ [NO BREAK] then, manage it has an up_to_date entry: */

        case STATUT_CACHE_A_JOUR:

            if ( type_cache == fichier )
            {
                int glob_cache_missing = FALSE;

                if ( config.attitudes.type_cache == CACHE_LOCAL )
                {
                    /* get global cache status */
                    if ( lstat( cglob_path, &st_glob ) != 0 )
                    {
                        if ( (errno == ENOENT) || (errno == ESTALE) )
                        {
                            /* no data in global cache:
                             * compare with reference to know its status
                             */
                            glob_cache_missing = TRUE;
                        }
                        else
                        {
                            /* failure */
                            DisplayLog( LVL_MAJOR, SHERPA_TAG, "Error performing lstat() on %s: %s", cglob_path, strerror(errno) );
                            return do_skip;
                        }
                    }
                    else
                    {
                       /* is it beeing archived ? */
                       if ( FlushEnCoursCacheGlob( cglob_path, &st_glob) )
                       {
                            /* reference is beeing written */
                            ATTR_MASK_SET( p_attrs, status );
                            ATTR(p_attrs, status) = STATUS_ARCHIVE_RUNNING;
                       }
                       else if ( TransfertEnCoursCache( cglob_path, &st_glob ) )
                       {
                            /* if the file is beeing loaded to global cache,
                             * we can just compare it to the reference,
                             * to know its status.
                             */
                            glob_cache_missing = TRUE;
                       }
                       else if ( st_glob.st_mtime > stat_cache.st_mtime )
                       {
                            /* local cache is obsolete? */
                            if ( LatenceNFSPossible( &stat_cache ) || LatenceNFSPossible( &st_glob ) )
                            {
                                DisplayLog( LVL_EVENT, SHERPA_TAG, "%s seams out-of-date, but we skip it because it was modified recently", ATTR(p_attrs, fullpath) );
                                ATTR_MASK_SET( p_attrs, status );
                                ATTR(p_attrs, status) = STATUS_OUT_OF_DATE;
                            }
                            else
                            {
                                remove = TRUE;
                                why = MODIF;
                            }
                       }
                       else if ( st_glob.st_mtime < stat_cache.st_mtime )
                       {
                            /* entry is really younger that others */
                            ATTR_MASK_SET( p_attrs, status );
                            ATTR(p_attrs, status) = STATUS_MODIFIED;
                       }
                       else /* same mtime = mtime: check file size */
                       {
                           if ( st_glob.st_size != stat_cache.st_size )
                           {
                               DisplayLog( LVL_MAJOR, SHERPA_TAG, "WARNING! size in local cache (%"PRINT_ST_SIZE")"
                                          " is different from global cache (%"PRINT_ST_SIZE") with the same mtime! (file %s)",
                                          stat_cache.st_size, st_glob.st_size, ATTR(p_attrs, fullpath) );

                                if ( st_glob.st_size > stat_cache.st_size )
                                {
                                    if ( !LatenceNFSPossible( &stat_cache ) && !LatenceNFSPossible( &st_glob ) )
                                    {
                                       DisplayLog( LVL_MAJOR, SHERPA_TAG, "File in reference is bigger: invalidate cache entry");
                                       remove = TRUE;
                                       why = MODIF;
                                    }
                                    else
                                    {
                                        /*  we are not sure about this entry... */
                                        ATTR_MASK_SET( p_attrs, status );
                                        ATTR(p_attrs, status) = STATUS_UNKNOWN;
                                    }
                                }
                                else if ( st_glob.st_size < stat_cache.st_size )
                                {
                                    if ( !LatenceNFSPossible( &st_glob ) && !LatenceNFSPossible( &stat_cache ) )
                                    {
                                        DisplayLog( LVL_MAJOR, SHERPA_TAG, "File in local cache is bigger: invalidating reference");
                                        st_glob.st_mtime = 1;
                                        MetAJourDates( cglob_path, st_glob );

                                        ATTR_MASK_SET( p_attrs, status );
                                        ATTR(p_attrs, status) = STATUS_MODIFIED;
                                    }
                                    else
                                    {
                                        /* we are not sure about this entry status... */
                                        ATTR_MASK_SET( p_attrs, status );
                                        ATTR(p_attrs, status) = STATUS_UNKNOWN;
                                    }
                                }
                           }
                           else /* file is really synchronized */
                           {
                                /* eventually backport atime to global cache */
                                MiseaJourAtimeReguliere( cglob_path, st_glob, stat_cache );
                                ATTR_MASK_SET( p_attrs, status );
                                ATTR(p_attrs, status) = STATUS_UP_TO_DATE;
                           }
                       }
                    }

                } /* end if local cache */

                /* if the cache is global or standard */
                if ( (config.attitudes.type_cache != CACHE_LOCAL) || glob_cache_missing )
                {
                    if ( (status_ref == STATUT_REF_OBSOLETE)  || (status_ref == STATUT_REF_FLUSH_TIMEOUT ) )
                    {
                        ATTR_MASK_SET( p_attrs, status );
                        ATTR(p_attrs, status) = STATUS_MODIFIED;
                    }
                    else if ( status_ref == STATUT_REF_FLUSH_ACTIF )
                    {
                        /* reference is beeing written */
                        ATTR_MASK_SET( p_attrs, status );
                        ATTR(p_attrs, status) = STATUS_ARCHIVE_RUNNING;
                    }
                    /* sanity check on size */
                    else if ( stat_ref.st_size != stat_cache.st_size )
                    {
                        DisplayLog( LVL_MAJOR, SHERPA_TAG, "WARNING! size in cache (%"PRINT_ST_SIZE")"
                                  " is different from reference (%"PRINT_ST_SIZE") with the same mtime! (file %s)",
                                  stat_cache.st_size, stat_ref.st_size, ATTR(p_attrs, fullpath) );

                        if ( stat_cache.st_size > stat_ref.st_size )
                        {
                            /* If file in cache is bigger, we consider its the good one.
                             * This may happen when file is written the same second as its creation,
                             * (or if mtime in filesystem has not been updated).
                             * In this case, we invalidate reference and consider the reference as obsolete.
                             */

                            /* we change the date only if the last modification is not recent
                             * (size difference may be due to NFS latency).
                             */
                            if ( !LatenceNFSPossible( &stat_ref ) && !LatenceNFSPossible( &stat_cache ) )
                            {
                                DisplayLog( LVL_MAJOR, SHERPA_TAG, "File in cache is bigger: invalidating reference");
                                stat_ref.st_mtime = 1;
                                MetAJourDates( ref_path, stat_ref );

                                ATTR_MASK_SET( p_attrs, status );
                                ATTR(p_attrs, status) = STATUS_MODIFIED;
                            }
                            else
                            {
                                /* we are not sure about this entry... */
                                ATTR_MASK_SET( p_attrs, status );
                                ATTR(p_attrs, status) = STATUS_UNKNOWN;
                            }
                        }
                        else if ( stat_cache.st_size < stat_ref.st_size )
                        {
                            /* cache is smaller. Maybe a transfer from another machine has just finished.
                             * In this cache, we consider the cache is out of date.
                             */
                            if ( !LatenceNFSPossible( &stat_cache ) && !LatenceNFSPossible( &stat_ref ) )
                            {
                               DisplayLog( LVL_MAJOR, SHERPA_TAG, "File in reference is bigger: invalidate cache entry");
                               remove = TRUE;
                               why = MODIF;
                            }
                            else
                            {
                                /*  we are not sure about this entry... */
                                ATTR_MASK_SET( p_attrs, status );
                                ATTR(p_attrs, status) = STATUS_UNKNOWN;
                            }
                        }
                    } /* end if != sizes */
                    else /* entry is really up to date */
                    {
                        /* eventually backport atime to reference */
                        MiseaJourAtimeReguliere( ref_path, stat_ref, stat_cache );
                        ATTR_MASK_SET( p_attrs, status );
                        ATTR(p_attrs, status) = STATUS_UP_TO_DATE;
                    }
                }
            }
            else
            {
                ATTR_MASK_SET( p_attrs, status );
                ATTR(p_attrs, status) = STATUS_UP_TO_DATE;
            }
            break;

        case STATUT_CACHE_ABSENTE:
            /* entry disapeared: skipping it */
            return do_skip;

        case STATUT_CACHE_OBSOLETE:
            /* entry out of date */

            /* be careful about latency (for files only) */
            if ( (type_cache != fichier) || (!LatenceNFSPossible( &stat_ref ) && !LatenceNFSPossible( &stat_cache )) )
            {
                remove = TRUE;
                why = MODIF;
            }
            else
            {
                DisplayLog( LVL_EVENT, SHERPA_TAG, "%s seams out-of-date, but we skip it because it was modified recently", ATTR(p_attrs, fullpath));
                ATTR_MASK_SET( p_attrs, status );
                ATTR(p_attrs, status) = STATUS_OUT_OF_DATE;
            }
            break;
            
        case STATUT_CACHE_CHARG_ACTIF:
            ATTR_MASK_SET( p_attrs, status );
            ATTR(p_attrs, status) = STATUS_RESTORE_RUNNING;
            break;

        case STATUT_CACHE_FLUSH_ACTIF:
            ATTR_MASK_SET( p_attrs, status );
            ATTR(p_attrs, status) = STATUS_ARCHIVE_RUNNING;
            break;

        case STATUT_CACHE_CHARG_TIMEOUT:
            DisplayLog( LVL_EVENT, SHERPA_TAG, "Copy (from reference) operation timeout for %s: removing it", ATTR(p_attrs, fullpath));
            remove = TRUE;
            why = COPIE_INCOMPLETE;
            break;

        case STATUT_CACHE_FLUSH_TIMEOUT:
            DisplayLog( LVL_EVENT, SHERPA_TAG, "Copy (from lower cache level) operation timeout for %s: unlocking tier file", ATTR(p_attrs, fullpath));
            upper_tranfer_unlock = TRUE;
            break;

        case STATUT_CACHE_INDETERMINE:
            DisplayLog( LVL_MAJOR, SHERPA_TAG, "Undetermined status for entry %s", ATTR(p_attrs, fullpath) );
            ATTR_MASK_SET( p_attrs, status );
            ATTR(p_attrs, status) = STATUS_UNKNOWN;
            break;

        default:
            DisplayLog( LVL_CRIT, SHERPA_TAG, "Unknown status value %d for entry %s", status_cache, ATTR(p_attrs, fullpath) );
            return do_skip;
    }

    /* check if entry is whitelisted */
    check_policies( p_id, p_attrs );
    
    if ( upper_tranfer_unlock )
    {
        if ( config.attitudes.type_cache == CACHE_GLOBAL )
        {
            char lock_file[MAXPATHLEN];
            CheminTransfert( ATTR(p_attrs, fullpath), &stat_cache, lock_file, cible_cache_global );
            rc = TermineFlushCacheGlob( FALSE, ATTR(p_attrs, fullpath), lock_file, &stat_cache );
            if ( rc != SUCCES )
            {
                DisplayLog( LVL_MAJOR, SHERPA_TAG, "Error trying to unlock file %s (lock=%s): %s",
                            ATTR(p_attrs, fullpath), lock_file, sherpa_errnum2str(rc) );
                return do_skip;
            }
            /* now recheck status */
            goto recheck;
        }
        else
        {
            DisplayLog( LVL_CRIT, SHERPA_TAG, "Unexpected status STATUT_CACHE_FLUSH_TIMEOUT for a non-global cache" );
            return do_skip;
        }
    }
    else if ( update_md )
    {
        /* only update metadata */
        rc = MetAJourPropGrpDrts(ATTR(p_attrs, fullpath), NULL, stat_ref, stat_cache);

        if ( rc != SUCCES )
        {
            DisplayLog( LVL_MAJOR, SHERPA_TAG, "Error updating metadata for cache entry %s: %s",
                        ATTR(p_attrs, fullpath), sherpa_errnum2str(rc) );
            return do_skip;
        }
    }
    else if ( remove )
    {
        char           straccess[256];

        switch (type_cache)
        {
            case repertoire:
                /* recursively remove entries */
                EffaceArborescence( NULL, ATTR(p_attrs, fullpath), stat_cache.st_dev,
                                    stat_cache, NULL, why );
                break;

            case fichier:
            case lien:
                Unlink( ATTR(p_attrs, fullpath) );

                /* report messages */

                FormatDurationFloat( straccess, 256, time( NULL ) - ATTR( p_attrs, last_access ));
                
                DisplayReport( "Removed %s | reason: %s | last access %s ago | size=%" PRINT_ST_SIZE
                       ", last_access=%" PRINT_TIME_T ", last_mod=%" PRINT_TIME_T,
                       ATTR(p_attrs, fullpath), cause2str(why), straccess, ATTR( p_attrs, size ),
                       ATTR( p_attrs , last_access ), ATTR( p_attrs, last_mod ) );
                break;
            default:
                DisplayLog(LVL_CRIT, SHERPA_TAG, "Unknown type for entry %s: skipping", ATTR(p_attrs, fullpath));
                return do_skip;
        }
        return do_rm;
    }
    
    /* the entry is OK */
    return do_update;
}

