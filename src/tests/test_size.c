
#include "config.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

int main(int argc, char ** argv)
{
	size_t sz;

	printf("SIZEOF(size_t)=%lu\n", sizeof(sz) );
}
