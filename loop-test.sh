#!/bin/bash
cd kvraft/ || exit
for i in {1..10}
do
   go test -race || exit
done