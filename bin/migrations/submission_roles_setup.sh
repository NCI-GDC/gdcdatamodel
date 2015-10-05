#!/bin/bash
# This is supposed to be run in gdc-repl to setup admin roles

. gdc-openstack
projects=( "phs000178" "phs000235" "phs000218" )
groups=( "JOSHUASMILLER" "YAJINGT" "SHANE_WILSON" )
roles=( "admin" "create" "update" "delete" )

for project in "${projects[@]}"
do
  for group in "${groups[@]}"
  do
    for role in "${roles[@]}"
    do
      echo "grant role $role for $group to $project"
      openstack role add --project $project --group $group $role
    done
  done
done
