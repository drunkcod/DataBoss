@echo off
pushd DataBoss.Specs
dotnet conesole %~dp0\Build\DataBoss.Specs\Debug\net452\DataBoss.Specs.dll %*
popd