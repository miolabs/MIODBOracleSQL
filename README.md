# MIODBOracleSQL

MIODB Wrapper to connect with Oracle Databases through Oracle Call Interface (OCI) 

#INSTALLATION

macOS:
$ brew tap InstantClientTap/instantclient
$ brew install instantclient-basic
$ brew install instantclient-sdk

Optional: 
$ brew install instantclient-sqlplus


#TESTING

Oracle Docker DB Image can be found at: https://github.com/oracle/docker-images/blob/main/OracleDatabase/SingleInstance/README.md

#CREDITS

https://marschall.github.io/2020/04/12/oracle-docker-container.html


Setting up an Oracle Docker Container for Local Testing

2020 April 12
Running Oracle in a Docker container locally can be a convenient way to run tests when don’t have an Oracle instance at hand. This avoids the need to install Oracle and is better suited for automation than creating a virtual machine.

To create Docker container you first need to create a Docker image. Follow the instructions of Oracle Database on Docker to build a Docker image of your choice, eg. ./buildDockerImage.sh -v 19.3.0 -s.

We do not want to run our tests with an administrator account so we need to set up a user with proper permissions for our tests. One particularity of these Docker images is that they always use container databases, this makes the setup a bit more involved.

A convenient way to set this all up is to have alphabetically ordered scripts in a folder named ´sql´ and have the Oracle Docker image automatically executing them by mounting the folder to /docker-entrypoint-initdb.d/setup.

First we create the user with the file sql/01_users.sql

ALTER SESSION SET CONTAINER = XEPDB1;

CREATE USER test_user IDENTIFIED BY "some-password";
Then we give him the permissions with the file sql/02_permissions.sql

ALTER SESSION SET CONTAINER = XEPDB1;

GRANT CONNECT TO test_user CONTAINER=CURRENT;
GRANT CREATE SESSION TO test_user CONTAINER=CURRENT;
GRANT RESOURCE TO test_user CONTAINER=CURRENT;

ALTER USER test_user QUOTA 100M ON USERS;
And finally we create the objects with the file sql/03_objects.sql

ALTER SESSION SET CONTAINER = XEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = test_user;

CREATE TABLE test_table (
  id     NUMBER(5) NOT NULL PRIMARY KEY
);

We can then create a Docker container with the following shell script.

DIRECTORY=`dirname $0`
DIRECTORY=$(realpath $DIRECTORY)

docker run --name test-project \
 -p 1521:1521 -p 5500:5500 \
 --shm-size=1g \
 -v ${DIRECTORY}/sql:/docker-entrypoint-initdb.d/setup \
 -d oracle/database:19.3.0-se2
Replace test-project with your chosen container name and 19.3.0-se2 with your chosen Oracle version.
