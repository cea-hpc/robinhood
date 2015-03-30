
block_inc1 {
    # variable in include
    include_var = 42;
}

%include "inc2.inc"

block_inc2 {
    # env var in include
    var_env = $TEST_VAL;
}
