#include <stdio.h>
#include <assert.h>

unsigned int max_count_to_hash_size(unsigned int max_count);
void *log_config;

int main(int argc, char **argv)
{
	int i;

	for (i = 1; i < 1024 * 1024 * 1024; i <<= 1) {
		unsigned int s = max_count_to_hash_size(i);
		fprintf(stderr, "count2size(%d) = %u\n", i, s);
		assert(s > 0);
	}
	return 0;
}
