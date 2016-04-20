#define FIND_TAG "find"

extern attr_mask_t disp_mask;

const char *type2char(const char *type);
const char type2onechar(const char *type);

GArray *prepare_printf_format(const char *format);
void printf_entry(GArray *chunks, const wagon_t *id, const attr_set_t *attrs);
void free_printf_formats(GArray *chunks);
