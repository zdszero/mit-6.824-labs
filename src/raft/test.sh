#! /bin/bash

[[ $# -ne 1 ]] && echo "Usage: ./test.sh name" && exit 1

test_times=20
all_passed=0

for (( i = 0; i < $test_times; i++ )); do
  go test -run $1 &> /dev/null
  if [[ $? -ne 0 ]]; then
    echo "${i} check not passed"
    all_passed=0
  else
    echo "${i} check passed"
  fi
done

if [[ all_passed -eq 0 ]]; then
  echo "all passed"
fi
