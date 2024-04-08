#!/bin/bash

docker run -it --rm -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=Pa55w0rd! --name databoss_mssql -p 0:1433 mcr.microsoft.com/mssql/server:2019-latest