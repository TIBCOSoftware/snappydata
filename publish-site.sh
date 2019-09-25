#!/usr/bin/env bash

# This utility moves markdown files to the docs folder for 
# generating documentation and then call the mkdocs utility 
# with the passed arguments.  

API_DOCS_DIR="build-artifacts/scala-2.11/docs"

if [ ! -d "${API_DOCS_DIR}" ]; then
  echo "API Docs folder ${API_DOCS_DIR} does not exist. Can't Publish."
  echo "Please check if you have run the gradle task docs."
  exit 1
fi

# Basic sanity of API DOCS folder. Check if HTML files are greater than
# 3500 in number. Currently it is actually more than 3700
NUM_HTML_FILES=`find ${API_DOCS_DIR} -name "*.html" | wc -l`

if [ "${NUM_HTML_FILES}" -lt 3500 ]; then
  echo "Expected at least 3500 html files but got only ${NUM_HTML_FILES} html files"
  echo "${API_DOCS_DIR} does not seem to contain all the htmls of the apidocs"
  exit 1 
fi

# In place replace GettingStarted.md with index.md in all the md files in docs
FDIR="./docs"
for f in `find ${FDIR} -name "*.md"`
do
  #echo REPLACING IN ${f}
  sed -i 's/GettingStarted.md/index.md/' ${f}
done

## Remove the lines till toc and then copy that as index.md
index_start_line=`grep -n '# Introduction' ./docs/GettingStarted.md | cut -d':' -f1`
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
  echo "Please install MkDocs (www.mkdocs.org/#installation) before publishing the docs. Exiting."
  exit 1
fi

# echo $@
# mkdocs $@
mkdocs build --clean

# Copy the generated scala docs inside the site folder. 
mkdir -p site/apidocs
cp -R build-artifacts/scala-2.11/docs/* site/apidocs/
mkdir -p site/sql_functions
cp -R spark/sql/site/* site/sql_functions/

#mkdocs gh-deploy

# remove extra files added to docs
#rm ./docs/index.md
mkdocs serve $@
