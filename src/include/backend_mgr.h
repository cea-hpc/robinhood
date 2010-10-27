#ifndef _BACKEND_MGR_H
#define _BACKEND_MGR_H

struct backend_t
{
	/* are archive asynchronous? */
	unsigned int async_archive:1;

	/* does the backend supports remove operation */
	unsigned int rm_support:1;

	/* does the "new" status exist and is different from "modified"? */
	unsigned int has_status_new:1;
} backend;

#endif
