#!/bin/bash

BARGS="/v:m /nologo /p:Configuration=Release"
PARGS="$BARGS /t:Pack /p:PackageOutputPath=\"../../Build\""

nupack() {
    dotnet msbuild "Source/$1/$1.csproj" $PARGS
}

dotnet msbuild DataBoss.sln /t:Restore /t:Clean /t:Build $BARGS
nupack DataBoss.Uuid
nupack DataBoss.Linq
nupack DataBoss.PowerArgs
nupack DataBoss.Data 
nupack DataBoss.Data.MsSql 
nupack DataBoss.Data.SqlClient 
nupack DataBoss.Data.Npgsql
nupack DataBoss.DataPackage
nupack DataBoss.Migrations
nupack DataBoss
nupack DataBoss.Testing.SqlServer