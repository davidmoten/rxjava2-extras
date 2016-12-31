#!/bin/bash
set -e
mvn site
cd ../davidmoten.github.io
git pull
mkdir -p rxjava2-extras
cp -r ../rxjava2-extras/target/site/* rxjava2-extras/
git add .
git commit -am "update site reports"
git push
