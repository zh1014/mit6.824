#!/bin/bash
cd kvraft/ || exit
for i in {1..50}
do
   go test -race || exit
done