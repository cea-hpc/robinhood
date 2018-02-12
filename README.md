I - Setting up source tree
==========================

This describes how to work on v4 branch.

Retrieve git repository:
* from github:
> git clone https://github.com/cea-hpc/robinhood.git
> git checkout v4
* from gerrithub:
> git clone -o gerrithub ssh://<login>@review.gerrithub.io/cea-hpc/robinhood
> git checkout v4

Generate autotool stuff and install git hooks:
> cd robinhood.git
> ./autogen.sh

Build:
> ./configure
> make
