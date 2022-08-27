#! /bin/bash

[[ $# -ne 2 ]] && echo "Usage: ./test.sh name times" && exit 1

test_name=$1
test_times=$2
all_passed=1

for (( i = 0; i < $test_times; i++ )); do
  go test -run $test_name > /tmp/out
  if [[ $? -ne 0 ]]; then
    echo "${i} check not passed"
    all_passed=0
    break
  else
    echo "${i} check passed"
    rm /tmp/out
  fi
done

if [[ all_passed -eq 1 ]]; then
  echo "all passed"
else
  echo "some testcase not passed"
fi
