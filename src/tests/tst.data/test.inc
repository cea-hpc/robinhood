
block_inc1 {
    # variable in include
    include_var = 42;
}

# include with an environment variable (value: "inc2.inc")
%include "$FILE_INC2"

block_inc2 {
    # env var in include
    var_env = $TEST_VAL;
}
