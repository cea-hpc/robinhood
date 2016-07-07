/* Copyright 2015 Cray Inc. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/* Create several files with random names. Names can include any byte
 * except NUL and /, which are not legal in filenames. */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>

#include <glib.h>

static int urandom;
static GString *filename;

/* Create a filename. Not the fastest algorithm, but it's good enough
 * for our purpose. */
static char *make_name(const char *dirname, size_t len)
{
	int rc;

	g_string_printf(filename, "%s/", dirname);
	while (len)
	{
		char byte;

		rc = read(urandom, &byte, 1);
		if (rc == -1)
		{
			fprintf(stderr, "urandom read failed: %d\n", errno);
			return NULL;
		}

		if (byte == '\0' || byte == '/')
			continue;

		g_string_append_c(filename, byte);

		len--;
	}

	return filename->str;
}

int main(int argc, char *argv[])
{
	int i;
	const char *name;
	int rc;
	int num;
	size_t length;
	char *dirname;

	if (argc != 4)
	{
		fprintf(stderr, "%s needs 3 arguments:\n", argv[0]);
		fprintf(stderr, "  - number of files\n");
		fprintf(stderr, "  - length of file names (1 to 255)\n");
		fprintf(stderr, "  - directory where to create the files\n");
		return EXIT_FAILURE;
	}

	num = atoi(argv[1]);
	if (num <= 0)
	{
		fprintf(stderr, "invalid number of files to create: %d\n", num);
		return EXIT_FAILURE;
	}

	length = atoi(argv[2]);
	if (length <= 0 || length >= 256)
	{
		fprintf(stderr,
				"invalid length of file names to create: %zu\n", length);
		return EXIT_FAILURE;
	}

	dirname = argv[3];

	urandom = open("/dev/urandom", O_RDONLY);
	if (urandom == -1)
	{
		fprintf(stderr, "can't open urandom: %d\n", errno);
		return EXIT_FAILURE;
	}

	filename = g_string_sized_new(1000);

	for (i = 0; i < num; i++)
	{
		char buf[100];
		int fd;

		name = make_name(dirname, length);
		if (name == NULL)
			return EXIT_FAILURE;

		fd = creat(name, S_IRUSR);
		if (fd == -1) {
			fprintf(stderr, "creat failed: %d\n", errno);
			return EXIT_FAILURE;
		}

		sprintf(buf, "file with weird name #%d", i);
		rc = write(fd, buf, strlen(buf));
		if (rc == -1) {
			fprintf(stderr, "write failed: %d\n", errno);
			return EXIT_FAILURE;
		}

		close(fd);
	}

	close(urandom);
	g_string_free(filename, TRUE);

	return EXIT_SUCCESS;
}
