#!/bin/bash
# @author: Peter B. (pb@das-werkstatt.com)
# @date: 2020-10-19

# CollectiveAccess Providence installation:
# This script sets write permissions for the user that Apache runs as, which is
# usually "www-data" on Debian-based distributions.
# It does so by assigning _group_ permissions, keeping the original owner
# intact.

WWW_GROUP="www-data"

DIR_CA_PROV="$1"
if [ ! -e "$DIR_CA_PROV/setup.php" ]; then
    echo "ERROR: Invalid CA providence folder '$DIR_CA_PROV'."
    exit 1
fi

# Base 'app' folder of CA providence:
DIR_APP="$DIR_CA_PROV/app"

# Create the following folders:
DIRS_CREATE="$DIR_CA_PROV/media/collectiveaccess
    $DIR_CA_PROV/media/collectiveaccess/workspace
    $DIR_APP/tmp/collectiveaccessCache"

# Allow write access to these folders:
DIRS_RW="$DIRS_CREATE
    $DIR_CA_PROV
    $DIR_APP/tmp 
    $DIR_APP/tmp/collectiveaccessCache
    $DIR_APP/log 
    $DIR_CA_PROV/media
    $DIR_CA_PROV/vendor/ezyang/htmlpurifier/library/HTMLPurifier/DefinitionCache/Serializer 
    $DIR_CA_PROV/vendor"

for DIR in $DIRS_CREATE; do
    if [ -d "$DIR" ]; then
        echo "Skipping '$DIR': Already exist. Good!"
        continue
    fi
    mkdir -v "$DIR"
done

echo ""
echo "Setting write permissions for group '$WWW_GROUP'..."
for DIR in $DIRS_RW; do
    echo "Dir: $DIR"

    chgrp $WWW_GROUP "$DIR"
    chmod 775 "$DIR"
done


