@echo off
pushd DataBoss.Specs
dotnet conesole %~dp0\Build\DataBoss.Specs\Debug\net45\DataBoss.Specs.dll %*
popd