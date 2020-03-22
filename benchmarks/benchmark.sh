#!/usr/bin/env zsh

# this script should be run in the same directory it resides in.
# the program should be compiled using the command:
#   go build -o doodlechallenge cmd/*.go
# at the root.
set -euf -o pipefail

OUT=benchmark.out

echo "" > $OUT

for i in {1..10}; do
    echo -e "\nRun $i\n======\n" |& tee -a $OUT
    (time ../doodlechallenge) |& tee -a $OUT
    sleep 30
done
