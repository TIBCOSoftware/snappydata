#!/usr/bin/env bash

# This utility moves markdown files to the docs folder for 
# generating documentation and then call the mkdocs utility 
# with the passed arguments.  

# In place replace GettingStarted.md with index.md in all the md files in docs
FDIR="./docs"
for f in `find ${FDIR} -name "*.md"`
do
  #echo REPLACING IN ${f}
  sed -i 's/GettingStarted.md/index.md/' ${f}
done

## Remove the lines till toc and then copy that as index.md
index_start_line=`grep -n '## Introduction' ./docs/GettingStarted.md | cut -d':' -f1`
echo LINE START $index_start_line

if [ ! -z ${index_start_line} ]; then
  tail -n +$index_start_line ./docs/GettingStarted.md > ./docs/index.md
else
  echo "Did not find the Introduction line in GettingStarted.md"
  exit 1
fi

# call the mkdocs utility
MKDOCS_EXISTS=`which mkdocs`

if [ -z ${MKDOCS_EXISTS} ]; then
  echo "Install mkdocs...exiting"
  exit 1
fi

# echo $@
# mkdocs $@
mkdocs build --clean

# Copy the generated scala docs inside the site folder. 
mkdir site/apidocs
cp -R build-artifacts/scala-2.11/docs/* site/apidocs/
#mkdocs gh-deploy

# remove extra files added to docs
#rm ./docs/index.md
mkdocs serve --dev-addr=0.0.0.0:8002
