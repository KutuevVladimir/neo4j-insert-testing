#!/bin/bash

NEO4J_SHA256=f2a2eda7a4313216a99a9400001fef6de1a3aa399602dd1a57f552a5f73cf349
NEO4J_URI=http://dist.neo4j.org/neo4j-community-3.4.7-unix.tar.gz
NEO4J_TARBALL=neo4j-community-3.4.7-unix.tar.gz
NEO4J_EDITION=community

APOC_URI=https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/3.4.0.4/apoc-3.4.0.4-all.jar
APOC_LIB=apoc-3.4.0.4-all.jar

echo "Installing Neo4j"
sudo curl --fail --silent --show-error --location --remote-name ${NEO4J_URI}
echo "${NEO4J_SHA256}  ${NEO4J_TARBALL}" | sha256sum -cw
sudo tar --extract --file ${NEO4J_TARBALL} --directory /var/lib
sudo mv /var/lib/neo4j-* /var/lib/neo4j
sudo rm ${NEO4J_TARBALL}
sudo mv /var/lib/neo4j/data /data
#sudo chown -R usr /data
sudo chmod -R 777 /data
#sudo chown -R usr /var/lib/neo4j
sudo chmod -R 777 /var/lib/neo4j
sudo ln -s /data /var/lib/neo4j/data
echo "Building custom-insert-procedure"
./gradlew :custom-insert-procedure:clean :custom-insert-procedure:jar
mv $(dirname $0)/custom-insert-procedure/build/libs/*.jar /var/lib/neo4j/plugins
echo "Installing APOC"
sudo curl --fail --silent --show-error --location --remote-name ${NEO4J_URI}
mv ${APOC_LIB} /var/lib/neo4j/plugins
echo "apoc.import.file.enabled=true" >> /var/lib/neo4j/conf/neo4j.conf
echo "dbms.security.procedures.unrestricted=apoc.*,vkutuev.*" >> /var/lib/neo4j/conf/neo4j.conf