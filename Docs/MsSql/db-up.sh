dotnet ./../../Build/DataBoss/Debug/net7.0/DataBoss.dll -ServerInstance $(docker port databoss_mssql 1433/tcp) $1
